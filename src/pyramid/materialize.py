"""Per-request materialization ledger for view derivation and response
body singleton materialization.

The ledger is an opt-in, append-only bookkeeping and commitment boundary
bound to a single request.  It records, on that *same* request, the
distinct completion surfaces that are otherwise easy to confuse:

- :class:`pyramid.config.views.MultiView` predicate *probe* hits
  (``match`` / ``__permitted__``) versus *execution* hits (``__call__``
  continuing past ``PredicateMismatch``);

- the four materialization forks of the ``rendered_view`` deriver
  (``result.__class__ is Response`` identity, ``IResponse`` adaptation,
  renderer materialization with one-shot ``override_renderer`` /
  ``__view__`` consumption, and the no-renderer error);

- :meth:`pyramid.renderers.RendererHelper._make_response` writing a body
  field onto the reified ``request.response`` singleton versus creating a
  new response via the response factory;

- the ``csrf_view`` check moment (before the downstream view is called)
  versus render-time ``get_csrf_token`` (from a template or a
  ``BeforeRender`` subscriber), for both the cookie and the session CSRF
  storage policies (including ``set_on_exception`` gating);

- the ``owrapped_view`` wrapper lookup plane (``providedBy(request)``,
  via ``render_view_to_response(..., secure=True)``) versus the route
  bound ``request.request_iface`` plane used by the router.

The ledger is *declarable* by the host (``materialization_ledger``
context manager or the ``pyramid.materialization_ledger`` setting) and
introduces no new required constructor parameters anywhere.  When no
ledger is active on a request every carrier behaves exactly as it does
without this feature.

Commitment semantics when nested scopes run on the same request:

- a body field (``text`` / ``body`` / ``app_iter``) already committed on
  the ``request.response`` singleton by an outer scope cannot be written
  through by an inner scope; the inner materialization is diverted to a
  freshly factory-created response and a
  :data:`RESPONSE_BODY_GUARDED` event is recorded;

- deferred ``Set-Cookie`` registrations (cookie CSRF policy and session
  policy response callbacks) are append-only records; the ledger exposes
  no operation that removes them;

- leaving a scope (or the whole ledger) never undoes a recorded event, a
  materialized body/header or an effective ``Set-Cookie``.  This is a
  commitment ledger, not a revocable intent stack.
"""

from contextlib import contextmanager
from zope.interface import implementer

from pyramid.interfaces import IMaterializationLedger
from pyramid.response import _get_response_factory

REQUEST_ATTR = '_materialization_ledger'

# event planes
PROBE = 'probe'
EXECUTE = 'execute'
LOOKUP = 'lookup'
RENDER = 'render'
CSRF = 'csrf'
SESSION = 'session'
OWRAPPED = 'owrapped'

# csrf/session moments
PHASE_CHECK = 'check'
PHASE_RENDER = 'render'
PHASE_EXPLICIT = 'explicit'

# MultiView
MULTIVIEW_CANDIDATES = 'multiview.candidates'
MULTIVIEW_PROBE = 'multiview.probe'
MULTIVIEW_PROBE_MISS = 'multiview.probe.miss'
MULTIVIEW_EXECUTE_ATTEMPT = 'multiview.execute.attempt'
MULTIVIEW_EXECUTE_HIT = 'multiview.execute.hit'
MULTIVIEW_EXECUTE_CONTINUE = 'multiview.execute.continue'
MULTIVIEW_EXECUTE_MISS = 'multiview.execute.miss'

# rendered_view deriver
RENDERED_IDENTITY = 'rendered.response_identity'
RENDERED_ADAPTER = 'rendered.response_adapter'
RENDERED_NO_RENDERER = 'rendered.no_renderer_error'
RENDERED_OVERRIDE = 'rendered.renderer_override'
RENDERED_VIEW_POPPED = 'rendered.view_popped'
RENDERED_RENDERER = 'rendered.renderer'

# RendererHelper
RENDER_BEFORE = 'render.before'
RESPONSE_FACTORY_NEW = 'response.factory_new'
RESPONSE_BODY = 'response.body_committed'
RESPONSE_BODY_GUARDED = 'response.body_write_guarded'

# csrf
CSRF_CHECK = 'csrf.check'
CSRF_TOKEN = 'csrf.token'
CSRF_NEW_TOKEN = 'csrf.new_token'
CSRF_REQUEST_COOKIE = 'csrf.request_cookie_set'
CSRF_COOKIE_DEFERRED = 'csrf.cookie.deferred'
CSRF_COOKIE_APPLIED = 'csrf.cookie.applied'

# session
SESSION_DEFERRED = 'session.deferred'
SESSION_COOKIE_APPLY = 'session.cookie.apply'
SESSION_COOKIE_APPLIED = 'session.cookie.applied'

# view lookup planes
VIEW_LOOKUP = 'view.lookup'
VIEW_DISPATCH = 'view.dispatch'
VIEW_ATTEMPT = 'view.attempt'
VIEW_HIT = 'view.hit'
VIEW_PREDICATE_MISMATCH = 'view.predicate_mismatch'

# owrapped deriver
OWRAPPED_INNER = 'owrapped.inner_materialized'
OWRAPPED_LOOKUP = 'owrapped.wrapper_lookup'

# ledger scope lifecycle
SCOPE_ENTER = 'ledger.scope_enter'
SCOPE_EXIT = 'ledger.scope_exit'


class LedgerEvent:
    """A single append-only ledger record.

    ``seq`` is the global order within the request, ``depth`` is the
    nested scope depth (0 for the root scope), ``plane`` and ``kind``
    classify the completion surface, and ``payload`` is an immutable
    mapping of fork-specific observations.
    """

    __slots__ = ('seq', 'depth', 'plane', 'kind', 'payload')

    def __init__(self, seq, depth, plane, kind, payload):
        self.seq = seq
        self.depth = depth
        self.plane = plane
        self.kind = kind
        self.payload = payload

    def __repr__(self):  # pragma: no cover - debugging aid
        return 'LedgerEvent(seq=%d, depth=%d, plane=%r, kind=%r)' % (
            self.seq,
            self.depth,
            self.plane,
            self.kind,
        )


@implementer(IMaterializationLedger)
class MaterializationLedger:
    """Append-only materialization ledger bound to one request."""

    def __init__(self, request):
        self.request = request
        self.events = []
        # scope names, one entry per open scope; depth is len() - 1
        self._scopes = []
        # phase stack used to classify the csrf/session moment
        self._phases = []
        # id(response) -> {body field name: committing scope depth}
        self._body_fields = {}
        self._seq = 0

    # -- lifecycle -------------------------------------------------------

    @property
    def depth(self):
        """Depth of the currently open scope (0 is the root)."""
        return len(self._scopes) - 1

    @property
    def current_phase(self):
        """Currently active moment phase (check/render/explicit)."""
        if self._phases:
            return self._phases[-1]
        return PHASE_EXPLICIT

    @contextmanager
    def scope(self, name=''):
        """Open a nested commitment scope on the same request.

        Exiting the scope never rolls back recorded events, materialized
        body/header fields or deferred cookies; it only restores the
        scope depth so subsequent materializations are attributed to the
        outer scope again.
        """
        depth = len(self._scopes)
        self._scopes.append(name)
        self.record(SCOPE_ENTER, name=name, depth=depth)
        try:
            yield self
        finally:
            self.record(SCOPE_EXIT, name=name, depth=depth)
            self._scopes.pop()

    @contextmanager
    def phase(self, name):
        """Mark the current moment (e.g. ``check``/``render``) for
        events recorded by CSRF/session carriers within it."""
        self._phases.append(name)
        try:
            yield
        finally:
            self._phases.pop()

    # -- recording -------------------------------------------------------

    def record(self, kind, plane=None, **payload):
        """Append an event; carrier hooks call this at every fork."""
        event = LedgerEvent(
            self._seq, len(self._scopes) - 1, plane, kind, payload
        )
        self._seq += 1
        self.events.append(event)
        return event

    def filter(self, kind=None, plane=None, depth=None):
        """Return the recorded events filtered by kind/plane/depth in
        global order."""
        result = self.events
        if kind is not None:
            result = [e for e in result if e.kind == kind]
        if plane is not None:
            result = [e for e in result if e.plane == plane]
        if depth is not None:
            result = [e for e in result if e.depth == depth]
        return result

    def find(self, kind, plane=None):
        """Return the last event of ``kind`` (optionally on a plane) or
        ``None`` if no such event was recorded."""
        for event in reversed(self.events):
            if event.kind == kind and (plane is None or event.plane == plane):
                return event
        return None

    def kinds(self, plane=None):
        """Event kinds in global order, optionally for one plane."""
        return [e.kind for e in self.filter(plane=plane)]

    @property
    def deferred_cookies(self):
        """Append-only observations of deferred Set-Cookie
        registrations (cookie CSRF and session policies)."""
        return self.filter(kind=CSRF_COOKIE_DEFERRED) + self.filter(
            kind=SESSION_DEFERRED
        )

    @property
    def materializations(self):
        """Response materialization events (factory new, singleton
        body writes, guarded forks)."""
        return [
            e
            for e in self.events
            if e.kind
            in (RESPONSE_FACTORY_NEW, RESPONSE_BODY, RESPONSE_BODY_GUARDED)
        ]

    def committed_body_fields(self, response):
        """Body fields already committed on ``response`` and the scope
        depth that committed each of them."""
        return dict(self._body_fields.get(id(response), {}))

    # -- commitment guard used by RendererHelper._make_response ----------

    def _singleton(self, request):
        # the response is a singleton only once request.response has
        # been reified (or otherwise put into the request instance dict)
        return request.__dict__.get('response')

    def response_born(self, response):
        """Record a response freshly created by the response factory."""
        self._body_fields.setdefault(id(response), {})
        self.record(RESPONSE_FACTORY_NEW, plane=RENDER, response=response)

    def guard_singleton_write(self, request, response, registry):
        """Return the response a body field may be written to.

        If ``response`` is the reified ``request.response`` singleton
        and the current scope is nested inside a scope that already
        committed a body field on it, a fresh factory response is
        returned and the fork is recorded; the outer singleton is not
        written through.  Header/cookie-only commits do not fork, so the
        'set cookie then return a renderable value' flow keeps cookie
        and body on the same singleton.
        """
        singleton = self._singleton(request)
        if response is not singleton:
            return response
        owners = self._body_fields.get(id(response), {})
        outer = sorted(
            field
            for field, owner_depth in owners.items()
            if owner_depth < self.depth
        )
        if not outer:
            return response
        response_factory = _get_response_factory(registry)
        forked = response_factory(request)
        self._body_fields.setdefault(id(forked), {})
        self.record(
            RESPONSE_BODY_GUARDED,
            plane=RENDER,
            singleton=response,
            response=forked,
            fields=tuple(outer),
            outer_depth=min(owners[field] for field in outer),
        )
        return forked

    def body_committed(self, response, field, singleton):
        """Record that ``field`` (text/body/app_iter) was materialized
        on ``response`` at the current scope depth."""
        owners = self._body_fields.setdefault(id(response), {})
        owners[field] = self.depth
        self.record(
            RESPONSE_BODY,
            plane=RENDER,
            response=response,
            field=field,
            singleton=singleton,
        )


def get_materialization_ledger(request):
    """Return the :class:`MaterializationLedger` declared on ``request``
    or ``None`` when no ledger is active."""
    if request is None:
        return None
    return getattr(request, REQUEST_ATTR, None)


@contextmanager
def materialization_ledger(request, name=''):
    """Declare a materialization ledger scope on ``request``.

    The first use on a request creates and binds the per-request
    :class:`MaterializationLedger` and opens its root scope; nested uses
    on the same request open inner commitment scopes.  Leaving the
    context manager commits (never undoes) everything recorded.
    """
    ledger = get_materialization_ledger(request)
    if ledger is None:
        ledger = MaterializationLedger(request)
        setattr(request, REQUEST_ATTR, ledger)
    with ledger.scope(name=name):
        yield ledger


def materialization_ledger_tween_factory(handler, registry):
    """Tween that opens the root ledger scope for every request when
    the ``pyramid.materialization_ledger`` setting (or
    :meth:`pyramid.config.Configurator.set_materialization_ledger`) is
    enabled."""

    def materialization_ledger_tween(request):
        with materialization_ledger(request, name='request'):
            return handler(request)

    return materialization_ledger_tween
