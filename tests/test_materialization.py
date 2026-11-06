import unittest
from zope.interface import Interface

from pyramid import testing
from pyramid.config.views import MultiView
from pyramid.csrf import (
    CookieCSRFStoragePolicy,
    SessionCSRFStoragePolicy,
    check_csrf_token,
    get_csrf_token,
)
from pyramid.events import BeforeRender, NewResponse
from pyramid.exceptions import PredicateMismatch
from pyramid.httpexceptions import HTTPFound
from pyramid.interfaces import (
    IRequest,
    IResponse,
    IRouteRequest,
    IView,
    IViewClassifier,
)
from pyramid.materialize import (
    CSRF,
    CSRF_COOKIE_APPLIED,
    CSRF_COOKIE_DEFERRED,
    CSRF_TOKEN,
    EXECUTE,
    LOOKUP,
    MULTIVIEW_CANDIDATES,
    MULTIVIEW_EXECUTE_CONTINUE,
    MULTIVIEW_EXECUTE_HIT,
    MULTIVIEW_PROBE,
    MULTIVIEW_PROBE_MISS,
    OWRAPPED_INNER,
    OWRAPPED_LOOKUP,
    PROBE,
    RENDER_BEFORE,
    RENDERED_ADAPTER,
    RENDERED_IDENTITY,
    RENDERED_NO_RENDERER,
    RENDERED_OVERRIDE,
    RENDERED_RENDERER,
    RENDERED_VIEW_POPPED,
    RESPONSE_BODY,
    RESPONSE_BODY_GUARDED,
    RESPONSE_FACTORY_NEW,
    SESSION_COOKIE_APPLIED,
    SESSION_COOKIE_APPLY,
    SESSION_DEFERRED,
    VIEW_LOOKUP,
    MaterializationLedger,
    get_materialization_ledger,
    materialization_ledger,
)
from pyramid.renderers import RendererHelper
from pyramid.request import Request
from pyramid.response import Response
from pyramid.session import SignedCookieSessionFactory


def make_request(config, path='/', **kw):
    request = Request.blank(path, **kw)
    request.registry = config.registry
    return request


def derive(config, view, **kw):
    name = kw.pop('name', '')
    config.add_view(view, name=name, **kw)
    return config.registry.adapters.lookup(
        (IViewClassifier, kw.get('request_iface', IRequest), Interface),
        IView,
        name=name,
        default=None,
    )


def set_cookie_headers(response):
    # webob keeps one entry per Set-Cookie header
    return [v for k, v in response.headerlist if k == 'Set-Cookie']


class TestLedgerCore(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp()

    def tearDown(self):
        testing.tearDown()

    def test_scopes_depth_and_event_order(self):
        request = make_request(self.config)
        self.assertIsNone(get_materialization_ledger(request))
        with materialization_ledger(request, name='outer') as ledger:
            self.assertIsInstance(ledger, MaterializationLedger)
            self.assertIs(get_materialization_ledger(request), ledger)
            self.assertEqual(ledger.depth, 0)
            ledger.record('a')
            with materialization_ledger(request, name='inner'):
                self.assertEqual(ledger.depth, 1)
                ledger.record('b')
            self.assertEqual(ledger.depth, 0)
            ledger.record('c')
        kinds = [e.kind for e in ledger.events]
        depths = [e.depth for e in ledger.events]
        # scope enter / exit events bracket the inner events causally
        self.assertEqual(
            kinds,
            [
                'ledger.scope_enter',
                'a',
                'ledger.scope_enter',
                'b',
                'ledger.scope_exit',
                'c',
                'ledger.scope_exit',
            ],
        )
        self.assertEqual(depths, [0, 0, 1, 1, 1, 0, 0])
        # after exit the ledger stays attached, append-only, no rollback
        self.assertIs(get_materialization_ledger(request), ledger)
        self.assertEqual(len(ledger.events), 7)

    def test_phases_classify_moment(self):
        request = make_request(self.config)
        with materialization_ledger(request) as ledger:
            self.assertEqual(ledger.current_phase, 'explicit')
            with ledger.phase('check'):
                self.assertEqual(ledger.current_phase, 'check')
                with ledger.phase('render'):
                    self.assertEqual(ledger.current_phase, 'render')
                self.assertEqual(ledger.current_phase, 'check')
            self.assertEqual(ledger.current_phase, 'explicit')

    def test_filter_find_queries(self):
        request = make_request(self.config)
        with materialization_ledger(request) as ledger:
            with materialization_ledger(request):
                ledger.record('x', plane='p', n=1)
            ledger.record('x', plane='p', n=2)
        self.assertEqual(
            [e.payload['n'] for e in ledger.filter(kind='x')], [1, 2]
        )
        self.assertEqual(
            [
                e.payload['n']
                for e in ledger.filter(kind='x', depth=1)
            ],
            [1],
        )
        self.assertEqual(ledger.find('x').payload['n'], 2)
        self.assertIsNone(ledger.find('nope'))


class TestRendererHelperMaterialization(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp()

    def tearDown(self):
        testing.tearDown()

    def _makeHelper(self):
        return RendererHelper(registry=self.config.registry)

    def test_singleton_body_write(self):
        request = make_request(self.config)
        helper = self._makeHelper()
        with materialization_ledger(request) as ledger:
            response = helper._make_response('hello', request)
        self.assertIs(response, request.response)
        self.assertEqual(response.text, 'hello')
        body = ledger.find(RESPONSE_BODY)
        self.assertIsNotNone(body)
        self.assertTrue(body.payload['singleton'])
        self.assertEqual(body.payload['field'], 'text')
        self.assertIsNone(ledger.find(RESPONSE_FACTORY_NEW))
        self.assertIsNone(ledger.find(RESPONSE_BODY_GUARDED))

    def test_factory_new_response(self):
        # request that has no reified response attribute: the pin code
        # creates a new response via the response factory
        helper = self._makeHelper()
        request = type(
            'BareRequest',
            (),
            {'registry': self.config.registry},
        )()
        with materialization_ledger(request) as ledger:
            response = helper._make_response(b'bytes', request)
        self.assertEqual(response.body, b'bytes')
        born = ledger.find(RESPONSE_FACTORY_NEW)
        self.assertIsNotNone(born)
        self.assertIs(born.payload['response'], response)
        body = ledger.find(RESPONSE_BODY)
        self.assertFalse(body.payload['singleton'])
        self.assertEqual(body.payload['field'], 'body')

    def test_inner_scope_cannot_write_through_outer_singleton(self):
        request = make_request(self.config)
        helper = self._makeHelper()
        with materialization_ledger(request, name='outer') as ledger:
            outer = helper._make_response('outer-body', request)
            self.assertIs(outer, request.response)
            # a header/cookie committed by the outer scope
            request.response.set_cookie('outer_cookie', '1')
            with materialization_ledger(request, name='inner'):
                inner = helper._make_response('inner-body', request)
                # inner materialization is diverted; it has its own body
                self.assertIsNot(inner, request.response)
                self.assertEqual(inner.text, 'inner-body')
                inner_events = ledger.filter(depth=1)
                self.assertTrue(
                    any(e.kind == RESPONSE_BODY_GUARDED for e in inner_events)
                )
            # leaving the inner scope does not restore anything, but
            # neither did the inner write reach the outer singleton
            self.assertEqual(request.response.text, 'outer-body')
            cookies = set_cookie_headers(request.response)
            self.assertTrue(
                any(c.startswith('outer_cookie=1') for c in cookies)
            )
        # leaving the whole ledger still does not undo body or cookie
        self.assertEqual(request.response.text, 'outer-body')
        guarded = ledger.find(RESPONSE_BODY_GUARDED)
        self.assertIs(guarded.payload['singleton'], request.response)
        self.assertIs(guarded.payload['response'], inner)
        self.assertEqual(guarded.payload['fields'], ('text',))

    def test_same_scope_rewrite_is_pin_behavior(self):
        request = make_request(self.config)
        helper = self._makeHelper()
        with materialization_ledger(request) as ledger:
            first = helper._make_response('one', request)
            second = helper._make_response('two', request)
        # no scope boundary -> overwrite on the same singleton, as pin
        self.assertIs(first, second)
        self.assertIs(second, request.response)
        self.assertEqual(request.response.text, 'two')
        self.assertIsNone(ledger.find(RESPONSE_BODY_GUARDED))

    def test_cookie_only_commit_keeps_body_on_singleton(self):
        # view sets a cookie then returns a value to render: cookie and
        # body must land on the same singleton, even across a scope
        request = make_request(self.config)
        helper = self._makeHelper()
        with materialization_ledger(request, name='outer'):
            request.response.set_cookie('flash', 'yes')
            with materialization_ledger(request, name='inner') as ledger:
                response = helper._make_response('rendered', request)
        self.assertIs(response, request.response)
        self.assertEqual(response.text, 'rendered')
        self.assertIsNone(ledger.find(RESPONSE_BODY_GUARDED))
        cookies = set_cookie_headers(response)
        self.assertTrue(any(c.startswith('flash=yes') for c in cookies))

    def test_no_ledger_is_pin_behavior(self):
        request = make_request(self.config)
        helper = self._makeHelper()
        response = helper._make_response('plain', request)
        self.assertIs(response, request.response)
        self.assertEqual(response.text, 'plain')
        self.assertFalse(
            hasattr(request, '_materialization_ledger')
        )


class _LyingPredicatedView:
    # probe says "I match", but execution raises PredicateMismatch, so
    # the probe cut and the execution cut must diverge
    def __predicated__(self, context, request):
        return True

    def __call__(self, context, request):
        raise PredicateMismatch('lying')


def _plain_hit(context, request):
    return 'PLAIN-HIT'


class TestMultiViewFork(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp()

    def tearDown(self):
        testing.tearDown()

    def _makeMultiView(self):
        mv = MultiView('test')
        mv.add(_LyingPredicatedView(), 0, phash='a', accept=None)
        mv.add(_plain_hit, 1, phash='b', accept=None)
        return mv

    def test_probe_and_execution_hits_diverge(self):
        request = make_request(self.config)
        mv = self._makeMultiView()
        with materialization_ledger(request) as ledger:
            probed = mv.match(None, request)
            executed = mv(None, request)
        self.assertIsInstance(probed, _LyingPredicatedView)
        self.assertEqual(executed, 'PLAIN-HIT')
        probe = ledger.filter(kind=MULTIVIEW_PROBE, plane=PROBE)
        self.assertEqual(len(probe), 1)
        self.assertTrue(probe[0].payload['predicated'])
        self.assertTrue(probe[0].payload['matched'])
        cont = ledger.find(MULTIVIEW_EXECUTE_CONTINUE)
        self.assertIsNotNone(cont)
        hit = ledger.find(MULTIVIEW_EXECUTE_HIT)
        self.assertIs(hit.payload['view'], _plain_hit)
        # the probe completes entirely before any execution attempt
        probe_seq = [e.seq for e in ledger.filter(plane=PROBE)]
        exec_seq = [
            e.seq
            for e in ledger.filter(plane=EXECUTE)
            if e.kind == 'multiview.execute.attempt'
        ]
        self.assertLess(max(probe_seq), min(exec_seq))

    def test_member_without_predicated_hits_probe_directly(self):
        request = make_request(self.config)
        mv = MultiView('test')
        mv.add(_plain_hit, 0, phash=None, accept=None)
        with materialization_ledger(request) as ledger:
            self.assertIs(mv.match(None, request), _plain_hit)
            self.assertEqual(mv(None, request), 'PLAIN-HIT')
        probe = ledger.find(MULTIVIEW_PROBE)
        self.assertFalse(probe.payload['predicated'])
        self.assertTrue(probe.payload['matched'])

    def test_probe_and_execution_miss(self):
        request = make_request(self.config)

        class Never:
            def __predicated__(self, context, request):
                return False

            def __call__(self, context, request):
                raise PredicateMismatch('never')

        never = Never()
        mv = MultiView('test')
        mv.add(never, 0, phash='x', accept=None)
        with materialization_ledger(request) as ledger:
            self.assertRaises(PredicateMismatch, mv.match, None, request)
            self.assertRaises(PredicateMismatch, mv, None, request)
        self.assertIsNotNone(ledger.find(MULTIVIEW_PROBE_MISS))
        self.assertIsNotNone(
            ledger.find('multiview.execute.miss')
        )
        # probe refused the candidate; execution tried it, continued
        # past the mismatch and never produced a hit
        probe = ledger.find(MULTIVIEW_PROBE)
        self.assertFalse(probe.payload['matched'])
        self.assertIsNotNone(ledger.find(MULTIVIEW_EXECUTE_CONTINUE))
        self.assertIsNone(ledger.find(MULTIVIEW_EXECUTE_HIT))

    def test_permitted_uses_probe_plane(self):
        request = make_request(self.config)
        mv = self._makeMultiView()
        with materialization_ledger(request) as ledger:
            mv.__permitted__(None, request)
        self.assertTrue(
            any(e.plane == PROBE for e in ledger.events)
        )
        self.assertFalse(
            any(e.plane == EXECUTE for e in ledger.events)
        )

    def test_accept_merged_order_recorded_per_plane(self):
        def json_hit(context, request):
            return 'JSON-HIT'

        request = make_request(
            self.config, headers={'Accept': 'application/json'}
        )
        mv = MultiView('test')
        mv.add(json_hit, 0, phash='j', accept='application/json')
        mv.add(_plain_hit, 1, phash='p', accept=None)
        with materialization_ledger(request) as ledger:
            self.assertEqual(mv.match(None, request), json_hit)
            self.assertEqual(mv(None, request), 'JSON-HIT')
        candidates = ledger.filter(
            kind=MULTIVIEW_CANDIDATES
        )
        planes = {c.plane: c for c in candidates}
        self.assertEqual(set(planes), {PROBE, EXECUTE})
        for event in candidates:
            # the accept merge fronts the media view in the exact
            # same candidate order serving both control flows
            self.assertEqual(event.payload['accepts'], ['application/json'])
            self.assertEqual(event.payload['orders'], [0, 1])
            self.assertEqual(event.payload['count'], 2)

    def test_no_ledger_pin_behavior(self):
        request = make_request(self.config)
        mv = self._makeMultiView()
        self.assertIsInstance(
            mv.match(None, request), _LyingPredicatedView
        )
        self.assertEqual(mv(None, request), 'PLAIN-HIT')
        self.assertIsNone(get_materialization_ledger(request))


class TestRenderedViewForks(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp()

    def tearDown(self):
        testing.tearDown()

    def _call(self, view, **kw):
        request = make_request(self.config)
        callable_view = derive(self.config, view, **kw)
        return request, callable_view(None, request)

    def test_response_identity_fast_path(self):
        def view(request):
            return Response('ident')

        request = make_request(self.config)
        call = derive(self.config, view)
        with materialization_ledger(request) as ledger:
            response = call(None, request)
        self.assertEqual(response.text, 'ident')
        event = ledger.find(RENDERED_IDENTITY)
        self.assertIsNotNone(event)
        self.assertIs(event.payload['result'], response)

    def test_response_subclass_adapted_as_self(self):
        def view(request):
            return HTTPFound(location='http://example.com/')

        request = make_request(self.config)
        call = derive(self.config, view)
        with materialization_ledger(request) as ledger:
            response = call(None, request)
        self.assertEqual(response.status_int, 302)
        event = ledger.find(RENDERED_ADAPTER)
        self.assertIsNotNone(event)
        self.assertIs(event.payload['response'], response)
        self.assertFalse(event.payload['adapted'])
        self.assertIsInstance(response, HTTPFound)

    def test_iresponse_adapter_fork(self):
        class Widget:
            pass

        def widget_to_response(widget):
            return Response('widget-adapted')

        self.config.registry.registerAdapter(
            widget_to_response, (Widget,), IResponse
        )

        def view(request):
            return Widget()

        request = make_request(self.config)
        call = derive(self.config, view)
        with materialization_ledger(request) as ledger:
            response = call(None, request)
        self.assertEqual(response.text, 'widget-adapted')
        event = ledger.find(RENDERED_ADAPTER)
        self.assertTrue(event.payload['adapted'])
        self.assertIsNot(event.payload['response'], event.payload['result'])

    def test_renderer_with_override_consumed_once(self):
        def view(request):
            request.override_renderer = 'json'
            return {'answer': 42}

        request = make_request(self.config)
        call = derive(self.config, view, renderer='json')
        with materialization_ledger(request) as ledger:
            response = call(None, request)
        self.assertIn('"answer"', response.text)
        override = ledger.find(RENDERED_OVERRIDE)
        self.assertIsNotNone(override)
        self.assertEqual(override.payload['name'], 'json')
        renderer_event = ledger.find('rendered.renderer')
        self.assertIsNotNone(renderer_event)
        # one-shot consumption: the request attribute is gone afterwards
        self.assertNotIn('override_renderer', request.__dict__)

    def test_renderer_pops_view_instance(self):
        class View:
            def __init__(self, request):
                self.request = request

            def __call__(self):
                return {'view': 'class'}

        request = make_request(self.config)
        call = derive(self.config, View, renderer='json')
        with materialization_ledger(request) as ledger:
            response = call(None, request)
        self.assertIn('"view"', response.text)
        popped = ledger.find(RENDERED_VIEW_POPPED)
        self.assertIsNotNone(popped)
        self.assertIsInstance(popped.payload['view'], View)
        self.assertNotIn('__view__', request.__dict__)

    def test_no_renderer_error_fork(self):
        def view(request):
            return {'nope': True}

        request = make_request(self.config)
        call = derive(self.config, view)
        with materialization_ledger(request) as ledger:
            self.assertRaises(ValueError, call, None, request)
        event = ledger.find(RENDERED_NO_RENDERER)
        self.assertIsNotNone(event)
        self.assertEqual(event.payload['result'], {'nope': True})

    def test_no_ledger_pin_overrides_still_popped(self):
        def view(request):
            request.override_renderer = 'json'
            return {'answer': 1}

        request = make_request(self.config)
        call = derive(self.config, view, renderer='json')
        response = call(None, request)
        self.assertIn('"answer"', response.text)
        self.assertNotIn('override_renderer', request.__dict__)


class TestCookieCSRFMoments(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp()
        self.config.set_csrf_storage_policy(CookieCSRFStoragePolicy())

    def tearDown(self):
        testing.tearDown()

    def _install_render_subscriber(self):
        def before_render(event):
            request = event['request']
            # a second call is a cache hit, no new token
            get_csrf_token(request)
            get_csrf_token(request)

        self.config.add_subscriber(before_render, BeforeRender)

    def test_csrf_view_then_render_moment_same_request(self):
        self._install_render_subscriber()

        def view(request):
            return {'ok': True}

        request = make_request(self.config)
        call = derive(self.config, view, renderer='json', require_csrf=True)
        with materialization_ledger(request) as ledger:
            response = call(None, request)
            # csrf_view check moment happens strictly before rendering
            check = ledger.find('csrf.check')
            render_before = ledger.find(RENDER_BEFORE)
            self.assertLess(check.seq, render_before.seq)
            self.assertTrue(check.payload['safe'])
            self.assertFalse(check.payload['applied'])
            tokens = ledger.filter(kind=CSRF_TOKEN, plane=CSRF)
            self.assertTrue(tokens)
            self.assertTrue(
                all(t.payload['policy'] == 'cookie' for t in tokens)
            )
            # mint happens at render time (BeforeRender subscriber)
            self.assertEqual(tokens[0].payload['moment'], 'render')
            self.assertTrue(tokens[0].payload['new'])
            self.assertFalse(tokens[-1].payload['new'])
            deferred = ledger.filter(kind=CSRF_COOKIE_DEFERRED)
            self.assertEqual(len(deferred), 1)
            self.assertEqual(deferred[0].payload['source'], 'cookie_csrf')
            # the deferred cookie is still effective after the view returns
            request._process_response_callbacks(response)
        cookies = set_cookie_headers(response)
        self.assertTrue(any(c.startswith('csrf_token=') for c in cookies))
        applied = ledger.find(CSRF_COOKIE_APPLIED)
        self.assertIsNotNone(applied)
        self.assertIn('"ok"', response.text)

    def test_check_moment_mints_token_and_defers_cookie(self):
        request = make_request(self.config, method='POST')
        with materialization_ledger(request, name='outer') as ledger:
            with materialization_ledger(request, name='during-check'):
                valid = check_csrf_token(request, raises=False)
            self.assertFalse(valid)
            # token was minted inside the check moment
            token = ledger.find(CSRF_TOKEN)
            self.assertEqual(token.payload['moment'], 'check')
            self.assertTrue(token.payload['new'])
            # request-side cookie is mutated immediately
            self.assertIn('csrf_token', request.cookies)
            self.assertEqual(
                len(ledger.filter(kind=CSRF_COOKIE_DEFERRED)), 1
            )
        # the deferred registration survives leaving the inner scope;
        # the Set-Cookie is applied to the response afterwards
        response = Response()
        request._process_response_callbacks(response)
        cookies = set_cookie_headers(response)
        self.assertTrue(any(c.startswith('csrf_token=') for c in cookies))

    def test_check_moment_with_supplied_token_mints_nothing(self):
        request = make_request(self.config, method='POST')
        request.cookies['csrf_token'] = 'known-token'
        request.headers['X-CSRF-Token'] = 'known-token'
        with materialization_ledger(request) as ledger:
            self.assertTrue(check_csrf_token(request, raises=False))
        token = ledger.find(CSRF_TOKEN)
        self.assertEqual(token.payload['moment'], 'check')
        self.assertFalse(token.payload['new'])
        self.assertFalse(ledger.filter(kind=CSRF_COOKIE_DEFERRED))

    def test_derived_csrf_view_unsafe_post_check_moment(self):
        # the check moment runs through the real csrf_view deriver,
        # before the view is called and before rendering
        def view(request):
            return {'ok': True}

        request = make_request(self.config, method='POST')
        request.cookies['csrf_token'] = 'known-token'
        request.headers['X-CSRF-Token'] = 'known-token'
        call = derive(self.config, view, renderer='json', require_csrf=True)
        with materialization_ledger(request) as ledger:
            response = call(None, request)
        check = ledger.find('csrf.check')
        self.assertIsNotNone(check)
        self.assertFalse(check.payload['safe'])
        self.assertTrue(check.payload['applied'])
        self.assertEqual(check.payload['method'], 'POST')
        token = ledger.find(CSRF_TOKEN)
        self.assertEqual(token.payload['moment'], 'check')
        self.assertFalse(token.payload['new'])
        # check moment precedes any rendering on the same request
        rendered = ledger.find(RENDERED_RENDERER)
        self.assertIsNotNone(rendered)
        self.assertLess(check.seq, rendered.seq)
        self.assertFalse(ledger.filter(kind=CSRF_COOKIE_DEFERRED))
        self.assertIn('"ok"', response.text)


class TestSessionCSRFMoments(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp()
        self.config.set_session_factory(
            SignedCookieSessionFactory('sekret')
        )
        self.config.set_csrf_storage_policy(SessionCSRFStoragePolicy())

    def tearDown(self):
        testing.tearDown()

    def test_render_moment_registers_session_cookie_once(self):
        def before_render(event):
            request = event['request']
            get_csrf_token(request)
            get_csrf_token(request)  # cached in the session

        self.config.add_subscriber(before_render, BeforeRender)

        def view(request):
            return {'ok': True}

        request = make_request(self.config)
        call = derive(self.config, view, renderer='json')
        with materialization_ledger(request) as ledger:
            response = call(None, request)
            tokens = ledger.filter(kind=CSRF_TOKEN)
            self.assertEqual(len(tokens), 2)
            self.assertTrue(
                all(t.payload['policy'] == 'session' for t in tokens)
            )
            self.assertTrue(
                all(t.payload['moment'] == 'render' for t in tokens)
            )
            self.assertTrue(tokens[0].payload['new'])
            self.assertFalse(tokens[1].payload['new'])
            # manage_changed registers a single deferred Set-Cookie
            self.assertEqual(
                len(ledger.filter(kind=SESSION_DEFERRED)), 1
            )
            request._process_response_callbacks(response)
        apply_event = ledger.find(SESSION_COOKIE_APPLY)
        self.assertTrue(apply_event.payload['will_set'])
        self.assertFalse(apply_event.payload['gated'])
        self.assertIsNotNone(ledger.find(SESSION_COOKIE_APPLIED))
        cookies = set_cookie_headers(response)
        self.assertTrue(any(c.startswith('session=') for c in cookies))

    def _gate_request(self, set_on_exception):
        factory = SignedCookieSessionFactory(
            'sekret', set_on_exception=set_on_exception
        )
        request = make_request(self.config)
        request.__dict__['session'] = factory(request)
        return request

    def test_set_on_exception_false_gates_the_cookie(self):
        request = self._gate_request(False)
        with materialization_ledger(request) as ledger:
            policy = SessionCSRFStoragePolicy()
            policy.get_csrf_token(request)  # mints -> session.changed()
            request.exception = ValueError('boom')
            response = Response()
            request._process_response_callbacks(response)
        apply_event = ledger.find(SESSION_COOKIE_APPLY)
        self.assertFalse(apply_event.payload['will_set'])
        self.assertTrue(apply_event.payload['gated'])
        self.assertFalse(apply_event.payload['set_on_exception'])
        self.assertIsNone(ledger.find(SESSION_COOKIE_APPLIED))
        self.assertEqual(set_cookie_headers(response), [])

    def test_set_on_exception_true_still_sets_the_cookie(self):
        request = self._gate_request(True)
        with materialization_ledger(request) as ledger:
            SessionCSRFStoragePolicy().get_csrf_token(request)
            request.exception = ValueError('boom')
            response = Response()
            request._process_response_callbacks(response)
        apply_event = ledger.find(SESSION_COOKIE_APPLY)
        self.assertTrue(apply_event.payload['will_set'])
        self.assertFalse(apply_event.payload['gated'])
        self.assertTrue(apply_event.payload['set_on_exception'])
        self.assertIsNotNone(ledger.find(SESSION_COOKIE_APPLIED))
        cookies = set_cookie_headers(response)
        self.assertTrue(any(c.startswith('session=') for c in cookies))


class TestOWrappedPlane(unittest.TestCase):
    def setUp(self):
        self.config = testing.setUp(
            settings={'pyramid.materialization_ledger': 'true'}
        )
        self.captured = []

        def capture(event):
            self.captured.append(get_materialization_ledger(event.request))

        self.config.add_subscriber(capture, NewResponse)

    def tearDown(self):
        from pyramid.config import global_registries

        global_registries.empty()
        testing.tearDown()

    def test_route_vs_providedby_planes_same_request(self):
        def inner(context, request):
            request.response.set_cookie('inner_cookie', 'v')
            return {'inner': True}

        def wrapper(context, request):
            return {
                'wrap': True,
                'inner_body': request.wrapped_body.decode('utf-8'),
            }

        self.config.add_route('r', '/')
        self.config.add_view(
            inner,
            route_name='r',
            renderer='json',
            wrapper='w',
        )
        # registered on the traversal/providedBy plane, not the route
        self.config.add_view(wrapper, name='w', renderer='json')
        from webtest import TestApp

        app = TestApp(self.config.make_wsgi_app())
        result = app.get('/')
        self.assertEqual(result.json['wrap'], True)
        self.assertIn('"inner"', result.json['inner_body'])
        set_cookie = result.headers.getall('Set-Cookie')
        self.assertTrue(
            any(c.startswith('inner_cookie=v') for c in set_cookie)
        )

        ledger = self.captured[0]
        self.assertIsNotNone(ledger)
        route_iface = self.config.registry.queryUtility(
            IRouteRequest, name='r'
        )
        route_lookup = [
            e
            for e in ledger.filter(kind=VIEW_LOOKUP, plane=LOOKUP)
            if e.payload['source'] == 'route'
        ]
        wrapper_lookup = [
            e
            for e in ledger.filter(kind=VIEW_LOOKUP, plane=LOOKUP)
            if e.payload['source'] == 'provided_by'
        ]
        self.assertTrue(route_lookup)
        self.assertTrue(wrapper_lookup)
        self.assertIs(route_lookup[0].payload['request_iface'], route_iface)
        # the wrapper lookup deliberately does not use the route iface
        self.assertIsNot(
            wrapper_lookup[0].payload['request_iface'], route_iface
        )
        self.assertEqual(wrapper_lookup[0].payload['name'], 'w')
        # causal order: inner materializes -> wrapper lookup dispatched
        inner = ledger.find(OWRAPPED_INNER)
        owrapped_lookup = ledger.find(OWRAPPED_LOOKUP)
        self.assertLess(inner.seq, owrapped_lookup.seq)
        self.assertIs(
            owrapped_lookup.payload['route_iface'], route_iface
        )
        self.assertLess(owrapped_lookup.seq, wrapper_lookup[0].seq)
        # both bodies materialize on the same response singleton, so
        # the inner Set-Cookie survives the wrapper rewrite
        body_events = ledger.filter(kind=RESPONSE_BODY)
        self.assertEqual(
            len({id(e.payload['response']) for e in body_events}), 1
        )
        self.assertTrue(
            all(e.payload['singleton'] for e in body_events)
        )
        self.assertIsNone(ledger.find(RESPONSE_BODY_GUARDED))


class TestLedgerTweenAndPin(unittest.TestCase):
    def tearDown(self):
        from pyramid.config import global_registries

        global_registries.empty()
        testing.tearDown()

    def test_tween_records_end_to_end_and_preserves_body(self):
        config = testing.setUp(
            settings={'pyramid.materialization_ledger': 'true'}
        )
        captured = []
        config.add_subscriber(
            lambda event: captured.append(
                get_materialization_ledger(event.request)
            ),
            NewResponse,
        )
        config.add_route('ok', '/ok')
        config.add_view(
            lambda r: Response('ok-body'), route_name='ok'
        )
        from webtest import TestApp

        app = TestApp(config.make_wsgi_app())
        result = app.get('/ok')
        self.assertEqual(result.text, 'ok-body')
        ledger = captured[0]
        self.assertIsNotNone(ledger)
        self.assertEqual(
            [e.kind for e in ledger.events][0], 'ledger.scope_enter'
        )
        self.assertEqual(
            [e.kind for e in ledger.events][-1], 'ledger.scope_exit'
        )
        self.assertTrue(
            any(
                e.kind == VIEW_LOOKUP
                and e.payload['source'] == 'route'
                for e in ledger.events
            )
        )
        self.assertTrue(
            all(e.depth == 0 for e in ledger.events)
        )

    def test_imperative_set_materialization_ledger(self):
        config = testing.setUp()
        config.set_materialization_ledger()
        captured = []
        config.add_subscriber(
            lambda event: captured.append(
                get_materialization_ledger(event.request)
            ),
            NewResponse,
        )
        config.add_route('ok', '/ok')
        config.add_view(lambda r: Response('ok'), route_name='ok')
        from webtest import TestApp

        app = TestApp(config.make_wsgi_app())
        self.assertEqual(app.get('/ok').text, 'ok')
        self.assertIsNotNone(captured[0])

    def test_disabled_request_has_no_ledger(self):
        config = testing.setUp()
        captured = []
        config.add_subscriber(
            lambda event: captured.append(
                get_materialization_ledger(event.request)
            ),
            NewResponse,
        )
        config.add_route('ok', '/ok')
        config.add_view(
            lambda r: Response('plain'), route_name='ok'
        )
        from webtest import TestApp

        app = TestApp(config.make_wsgi_app())
        self.assertEqual(app.get('/ok').text, 'plain')
        self.assertEqual(captured, [None])
