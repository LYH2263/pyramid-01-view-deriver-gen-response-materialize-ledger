# -*- coding: utf-8 -*-
"""Acceptance tests for Channel synchronous-RPC blocked-enqueue settlement
and nested wait windows.

The scenarios exercise ordinary synchronous APIs that go through
`Channel._rpc` (exchange/queue/tx style), never publisher confirms:

* idle direct-send vs already-blocking enqueue fork;
* blocked requests continued in FIFO order by normal
  `_on_synchronous_complete`;
* nested wait windows: an inner wait never drains the outer queue or
  writes through the outer pending waiter, and abandoning the inner
  wait removes exactly its own queued/registered callbacks (no ghosts);
* remote-close drain fork: only a queued client Channel.Close is
  fulfilled through meta-close, other queued methods are dropped;
* Connection._rpc fork: no `_blocked` ledger and no settlement;
* the three host waiting shapes - BlockingChannel flush-wait, the
  asynchronous one-shot direct callback and TwistedChannel `_calls`
  Deferreds - obey the same nested settlement semantics while keeping
  their own scheduling.

"""
import unittest
from unittest import mock

from pika import callback as callback_module
from pika import exceptions, frame, spec
from pika.adapters import twisted_connection
from pika.adapters.blocking_connection import BlockingChannel
from pika.channel import Channel, _SyncRpcSettlement
from pika import connection as connection_module


class FakeConnection(object):
    """Minimal connection backing a real Channel with a real
    CallbackManager; records methods the channel puts on the wire.

    """

    def __init__(self):
        self.callbacks = callback_module.CallbackManager()
        self.sent = []

    def _send_method(self, channel_number, method, content=None):
        self.sent.append(method)


def make_channel(channel_number=1):
    connection = FakeConnection()
    on_open = mock.Mock(name='on_open_callback')
    channel = Channel(connection, channel_number, on_open)
    channel._state = Channel.OPEN
    return connection, channel


def _deliver(channel, reply_class, **reply_kwargs):
    """Drive a reply method frame through the channel's CallbackManager,
    exactly as connection frame handling would.

    """
    method = reply_class(**reply_kwargs)
    channel.callbacks.process(channel.channel_number, reply_class, channel,
                              frame.Method(channel.channel_number, method))
    return method


def _sent_names(connection):
    return [method.NAME for method in connection.sent]


def _pending_callbacks(channel, reply_class):
    return channel.callbacks.pending(channel.channel_number, reply_class)


class IdleSendVsBlockedEnqueueTests(unittest.TestCase):

    def setUp(self):
        self.connection, self.channel = make_channel()
        self.outer_cb = mock.Mock(name='outer_declareok')
        self.inner_cb = mock.Mock(name='inner_declareok')

    def test_idle_rpc_is_sent_immediately_with_active_settlement(self):
        self.channel.exchange_declare(exchange='outer',
                                      callback=self.outer_cb)

        self.assertEqual(_sent_names(self.connection),
                         ['Exchange.Declare'])
        self.assertEqual(self.channel._blocking, 'Exchange.Declare')
        self.assertEqual(len(self.channel._blocked), 0)

        active = self.channel._active_settlement
        self.assertIsNotNone(active)
        self.assertEqual(active.state, _SyncRpcSettlement.IN_FLIGHT)
        self.assertIsInstance(active.method, spec.Exchange.Declare)

        # osc plus the user callback, both one-shot on the reply key
        self.assertEqual(_pending_callbacks(self.channel,
                                           spec.Exchange.DeclareOk), 2)

        _deliver(self.channel, spec.Exchange.DeclareOk)

        self.outer_cb.assert_called_once()
        self.assertIsNone(self.channel._blocking)
        self.assertIsNone(self.channel._active_settlement)
        self.assertEqual(active.state, _SyncRpcSettlement.SETTLED)

    def test_blocking_rpc_enqueues_and_normal_completion_continues_fifo(self):
        self.channel.exchange_declare(exchange='outer',
                                      callback=self.outer_cb)
        self.channel.queue_declare(queue='inner', callback=self.inner_cb)

        # Fork: the second synchronous method is not sent; its raw triple
        # sits in _blocked in pin-compatible form.
        self.assertEqual(_sent_names(self.connection),
                         ['Exchange.Declare'])
        self.assertEqual(self.channel._blocking, 'Exchange.Declare')
        self.assertEqual(len(self.channel._blocked), 1)
        blocked_entry = self.channel._blocked[0]
        self.assertIsInstance(blocked_entry[0], spec.Queue.Declare)
        self.assertIs(blocked_entry[1], self.inner_cb)
        self.assertEqual(
            blocked_entry[2],
            [(spec.Queue.DeclareOk, {'queue': 'inner'})])
        self.assertEqual(blocked_entry.settlement.state,
                         _SyncRpcSettlement.ENQUEUED)

        # Outer reply drains exactly one queued method, in order.
        _deliver(self.channel, spec.Exchange.DeclareOk)
        self.outer_cb.assert_called_once()
        self.inner_cb.assert_not_called()
        self.assertEqual(_sent_names(self.connection),
                         ['Exchange.Declare', 'Queue.Declare'])
        self.assertEqual(self.channel._blocking, 'Queue.Declare')
        self.assertEqual(len(self.channel._blocked), 0)

        # Inner reply settles the chain and releases blocking.
        _deliver(self.channel, spec.Queue.DeclareOk, queue='inner')
        self.inner_cb.assert_called_once()
        self.assertIsNone(self.channel._blocking)
        self.assertIsNone(self.channel._active_settlement)

    def test_without_windows_the_ledger_is_pin_identical(self):
        # No wait window declared: raw FIFO continuation uses the same
        # enqueue/drain behavior with plain, settlement-less triples.
        self.channel.exchange_declare(exchange='outer',
                                      callback=self.outer_cb)
        raw_triple = [
            spec.Queue.Declare(queue='raw'), self.inner_cb,
            [spec.Queue.DeclareOk]
        ]
        self.channel._blocked.append(raw_triple)

        _deliver(self.channel, spec.Exchange.DeclareOk)

        self.assertEqual(_sent_names(self.connection),
                         ['Exchange.Declare', 'Queue.Declare'])
        self.assertEqual(self.channel._blocking, 'Queue.Declare')
        _deliver(self.channel, spec.Queue.DeclareOk, queue='raw')
        self.inner_cb.assert_called_once()


class NestedWaitWindowTests(unittest.TestCase):

    def setUp(self):
        self.connection, self.channel = make_channel()
        self.outer_cb = mock.Mock(name='outer_declareok')
        self.inner_cb = mock.Mock(name='inner_declareok')

    def test_abandon_queued_inner_restores_outer_without_ghosts(self):
        # Twisted-style ordering: window declared before the impl call.
        outer_window = self.channel._begin_sync_wait_window()
        self.channel.exchange_declare(exchange='outer',
                                      callback=self.outer_cb)

        inner_window = self.channel._begin_sync_wait_window()
        self.channel.queue_declare(queue='inner', callback=self.inner_cb)

        self.assertEqual(len(self.channel._sync_wait_windows), 2)
        self.assertEqual(len(self.channel._blocked), 1)
        self.assertEqual(self.channel._blocking, 'Exchange.Declare')

        # Give up the inner wait while its method is still queued:
        # exactly its own entry disappears, nothing goes on the wire and
        # no callback was ever registered for it.
        self.channel._end_sync_wait_window(inner_window, abandon=True)

        self.assertEqual(len(self.channel._blocked), 0)
        self.assertEqual(_sent_names(self.connection),
                         ['Exchange.Declare'])
        self.assertEqual(self.channel._blocking, 'Exchange.Declare')
        self.assertIsNone(
            _pending_callbacks(self.channel, spec.Queue.DeclareOk))
        self.inner_cb.assert_not_called()

        # The outer reply registrations survive untouched.
        self.assertEqual(_pending_callbacks(self.channel,
                                           spec.Exchange.DeclareOk), 2)
        _deliver(self.channel, spec.Exchange.DeclareOk)
        self.outer_cb.assert_called_once()
        self.inner_cb.assert_not_called()
        self.assertEqual(_sent_names(self.connection),
                         ['Exchange.Declare'])

        self.channel._end_sync_wait_window(outer_window)
        self.assertEqual(self.channel._sync_wait_windows, [])

    def test_claim_pending_matches_blocking_call_then_flush_ordering(self):
        # BlockingChannel-style ordering: the impl RPC is issued first,
        # the wait window declared afterwards and claims the pending
        # settlement.
        self.channel.exchange_declare(exchange='outer',
                                      callback=self.outer_cb)
        outer_window = self.channel._begin_sync_wait_window(
            claim_pending=True)
        self.assertIn(self.channel._active_settlement,
                      outer_window.settlements)

        # The inner RPC lands in the outer window first...
        self.channel.queue_declare(queue='inner', callback=self.inner_cb)
        inner_settlement = self.channel._blocked[-1].settlement
        self.assertIs(inner_settlement.window, outer_window)

        # ...and the nested flush claims it for the inner window.
        inner_window = self.channel._begin_sync_wait_window(
            claim_pending=True)
        self.assertIs(inner_settlement.window, inner_window)
        self.assertNotIn(inner_settlement, outer_window.settlements)

        self.channel._end_sync_wait_window(inner_window, abandon=True)
        self.assertEqual(len(self.channel._blocked), 0)
        self.assertEqual(self.channel._blocking, 'Exchange.Declare')
        self.assertEqual(_pending_callbacks(self.channel,
                                           spec.Exchange.DeclareOk), 2)
        self.channel._end_sync_wait_window(outer_window, abandon=True)

    def test_nested_normal_completion_does_not_write_through_outer(self):
        events = []

        def outer_cb(method_frame):
            events.append(
                ('outer_reply', list(_sent_names(self.connection))))
            self.outer_cb(method_frame)

        outer_window = self.channel._begin_sync_wait_window()
        self.channel.exchange_declare(exchange='outer', callback=outer_cb)

        inner_window = self.channel._begin_sync_wait_window()
        self.channel.queue_declare(queue='inner', callback=self.inner_cb)

        # Outer reply: osc drains the inner method BEFORE invoking the
        # outer user callback; the outer waiter is signalled once and the
        # inner waiter is still pending.
        _deliver(self.channel, spec.Exchange.DeclareOk)
        self.assertEqual(events, [
            ('outer_reply', ['Exchange.Declare', 'Queue.Declare'])
        ])
        self.outer_cb.assert_called_once()
        self.inner_cb.assert_not_called()

        self.channel._end_sync_wait_window(inner_window)

        _deliver(self.channel, spec.Queue.DeclareOk, queue='inner')
        self.inner_cb.assert_called_once()
        self.outer_cb.assert_called_once()

        self.channel._end_sync_wait_window(outer_window)
        self.assertEqual(self.channel._sync_wait_windows, [])
        self.assertIsNone(self.channel._blocking)
        self.assertEqual(len(self.channel._blocked), 0)

    def test_inflight_abandon_detaches_user_callback_but_keeps_chain(self):
        outer_window = self.channel._begin_sync_wait_window()
        self.channel.exchange_declare(exchange='outer',
                                      callback=self.outer_cb)
        follow_cb = mock.Mock(name='follow_declareok')

        # Popup while the outer method is in flight: its user reply
        # callback is detached, leaving only osc so the reply can still
        # release blocking and continue the chain later.
        self.channel._end_sync_wait_window(outer_window, abandon=True)
        self.assertEqual(_pending_callbacks(self.channel,
                                           spec.Exchange.DeclareOk), 1)
        self.assertEqual(self.channel._blocking, 'Exchange.Declare')

        # A later queued method must still get sent when the abandoned
        # in-flight reply arrives.
        self.channel.queue_declare(queue='follow', callback=follow_cb)

        _deliver(self.channel, spec.Exchange.DeclareOk)
        self.outer_cb.assert_not_called()
        self.assertEqual(_sent_names(self.connection),
                         ['Exchange.Declare', 'Queue.Declare'])
        # The abandoned reply still released blocking and promoted the
        # queued follow-up, which now owns the blocking state.
        self.assertEqual(self.channel._blocking, 'Queue.Declare')

        _deliver(self.channel, spec.Queue.DeclareOk, queue='follow')
        follow_cb.assert_called_once()
        self.outer_cb.assert_not_called()
        self.assertIsNone(self.channel._blocking)


class RemoteCloseDrainForkTests(unittest.TestCase):

    def setUp(self):
        self.connection, self.channel = make_channel()
        self.outer_cb = mock.Mock(name='outer_declareok')
        self.queued_cb = mock.Mock(name='queued_declareok')

    def test_remote_close_fulfills_only_queued_close_and_drops_rest(self):
        # One method in flight, one ordinary method and the client
        # Channel.Close queued behind it; channel is CLOSING.
        self.channel.exchange_declare(exchange='outer',
                                      callback=self.outer_cb)
        self.channel.queue_declare(queue='queued', callback=self.queued_cb)
        self.channel.close(reply_code=200, reply_text='client close')

        self.assertEqual(self.channel._blocking, 'Exchange.Declare')
        self.assertEqual(len(self.channel._blocked), 2)

        broker_close = frame.Method(
            1, spec.Channel.Close(reply_code=404, reply_text='broker gone'))
        self.channel._on_close_from_broker(broker_close)

        # CloseOk answered, the queued Queue.Declare never sent.
        self.assertEqual(_sent_names(self.connection),
                         ['Exchange.Declare', 'Channel.CloseOk'])
        self.assertTrue(self.channel.is_closed)
        self.assertIsInstance(self.channel._closing_reason,
                              exceptions.ChannelClosedByBroker)

        # Dropped waiters are not invoked and carry no ghost registrations.
        self.queued_cb.assert_not_called()
        self.outer_cb.assert_not_called()
        self.assertIsNone(
            _pending_callbacks(self.channel, spec.Queue.DeclareOk))
        self.assertIsNone(self.channel._blocking)
        self.assertEqual(len(self.channel._blocked), 0)

    def test_normal_completion_continues_where_close_drops(self):
        # Fork counterpart: same queue shape, but a normal outer reply
        # continues both queued methods instead of dropping them.
        self.channel.exchange_declare(exchange='outer',
                                      callback=self.outer_cb)
        self.channel.queue_declare(queue='queued', callback=self.queued_cb)

        _deliver(self.channel, spec.Exchange.DeclareOk)
        self.assertEqual(_sent_names(self.connection),
                         ['Exchange.Declare', 'Queue.Declare'])
        _deliver(self.channel, spec.Queue.DeclareOk, queue='queued')
        self.queued_cb.assert_called_once()
        self.assertIsNone(self.channel._blocking)


class ConnectionRpcForkTests(unittest.TestCase):
    """Connection._rpc (channel 0 handshakes) has no _blocked serial
    ledger and must never masquerade as Channel settlement.

    """

    def setUp(self):

        class ConstructibleConnection(connection_module.Connection):

            def _adapter_connect_stream(self):
                raise NotImplementedError

            def _adapter_disconnect_stream(self):
                raise NotImplementedError

            def _adapter_call_later(self, delay, callback):
                raise NotImplementedError

            def _adapter_remove_timeout(self, timeout_id):
                raise NotImplementedError

            def _adapter_add_callback_threadsafe(self, callback):
                raise NotImplementedError

            def _adapter_emit_data(self, data):
                raise NotImplementedError

        with mock.patch.object(ConstructibleConnection,
                               '_adapter_connect_stream'):
            self.connection = ConstructibleConnection()
            self.connection._set_connection_state(
                connection_module.Connection.CONNECTION_OPEN)
            self.connection._opened = True

    def test_connection_rpc_sends_directly_without_blocked_ledger(self):
        cb = mock.Mock(name='openok')
        with mock.patch.object(self.connection, '_send_method') as send:
            self.connection._rpc(0,
                                 spec.Connection.Open(virtual_host='/'),
                                 cb, [spec.Connection.OpenOk])

        # Sent immediately, exactly once - no enqueue fork exists here.
        send.assert_called_once_with(0, mock.ANY)

        # Only the user callback is registered: no
        # _on_synchronous_complete and no settlement.
        self.assertEqual(self.connection.callbacks.pending(
            0, spec.Connection.OpenOk), 1)
        registered = [
            entry[callback_module.CallbackManager.CALLBACK]
            for entry in self.connection.callbacks._stack['0'][
                'Connection.OpenOk']
        ]
        self.assertEqual(registered, [cb])

        self.assertFalse(hasattr(self.connection, '_blocked'))
        self.assertFalse(hasattr(self.connection, '_blocking'))
        self.assertFalse(hasattr(self.connection, '_active_settlement'))


class _ScriptedBlockingConnection(object):
    """Stand-in for BlockingConnection: runs scripted pump steps inside
    the exact connection-level `_flush_output(is_closed, *waiters)` call
    BlockingChannel delegates its synchronous waits to.

    A nested flush consumes the last pushed script, which mirrors
    reentrant pumping inside an outer flush-wait.

    """

    def __init__(self):
        self.scripts = []
        self.flush_calls = []

    def _flush_output(self, *waiters):
        self.flush_calls.append(waiters)
        while not any(waiter() for waiter in waiters):
            step = self.scripts[-1].pop(0)
            step()


class BlockingChannelNestedSettlementTests(unittest.TestCase):
    """BlockingChannel keeps its _CallbackResult + connection flush pump
    scheduling but brackets each synchronous wait with a window.

    """

    def setUp(self):
        self.fake_connection, self.impl = make_channel()
        self.pump = _ScriptedBlockingConnection()
        self.channel = BlockingChannel(self.impl, self.pump)

    def _nested(self, inner_call, inner_steps):

        def action():
            self.pump.scripts.append(list(inner_steps))
            try:
                return inner_call()
            finally:
                self.pump.scripts.pop()

        return action

    def test_nested_flush_waits_settle_without_waiter_write_through(self):
        inner_returns = []
        outer_script = [
            self._nested(
                lambda: inner_returns.append(
                    self.channel.queue_declare(queue='inner')), [
                        lambda: _deliver(self.impl,
                                        spec.Exchange.DeclareOk),
                        lambda: _deliver(self.impl,
                                        spec.Queue.DeclareOk,
                                        queue='inner'),
                    ])
        ]
        self.pump.scripts.append(outer_script)

        outer_return = self.channel.exchange_declare(exchange='outer')

        # Pump delegation pin: every flush is still the connection-level
        # (is_closed callable, result-ready waiter) pair.
        self.assertTrue(self.pump.flush_calls)
        for call in self.pump.flush_calls:
            self.assertTrue(callable(call[0]))
            self.assertTrue(callable(call[1]))

        # FIFO wire ordering; each _CallbackResult carries its own reply.
        self.assertEqual(_sent_names(self.fake_connection),
                         ['Exchange.Declare', 'Queue.Declare'])
        self.assertIsInstance(outer_return, frame.Method)
        self.assertIsInstance(outer_return.method, spec.Exchange.DeclareOk)
        self.assertEqual(len(inner_returns), 1)
        self.assertIsInstance(inner_returns[0], frame.Method)
        self.assertIsInstance(inner_returns[0].method,
                              spec.Queue.DeclareOk)
        self.assertIsNot(outer_return, inner_returns[0])

        # Ledger restored for the outer channel lifecycle.
        self.assertEqual(self.impl._sync_wait_windows, [])
        self.assertIsNone(self.impl._blocking)
        self.assertEqual(len(self.impl._blocked), 0)

    def test_inner_popup_abandons_its_queue_and_outer_still_completes(self):

        class PopupError(Exception):
            pass

        caught = []

        def popup_inner():

            def raise_popup():
                raise PopupError('inner wait aborted')

            self.pump.scripts.append([raise_popup])
            try:
                self.channel.queue_declare(queue='inner')
            except PopupError:
                caught.append(True)
            finally:
                self.pump.scripts.pop()

        self.pump.scripts.append([
            popup_inner,
            lambda: _deliver(self.impl, spec.Exchange.DeclareOk),
        ])

        outer_return = self.channel.exchange_declare(exchange='outer')

        self.assertEqual(caught, [True])
        # The abandoned inner method never reached the wire and left no
        # blocked slot or ghost reply registration; the outer reply still
        # completes its own _CallbackResult exactly once.
        self.assertEqual(_sent_names(self.fake_connection),
                         ['Exchange.Declare'])
        self.assertIsInstance(outer_return.method, spec.Exchange.DeclareOk)
        self.assertIsNone(_pending_callbacks(self.impl,
                                            spec.Queue.DeclareOk))
        self.assertEqual(self.impl._sync_wait_windows, [])
        self.assertIsNone(self.impl._blocking)
        self.assertEqual(len(self.impl._blocked), 0)


class TwistedChannelNestedSettlementTests(unittest.TestCase):
    """TwistedChannel keeps its Deferreds-in-_calls scheduling (the
    impl one-shot callback fires the Deferred) while each call rides a
    nested wait window.

    """

    def setUp(self):
        self.fake_connection, self.impl = make_channel()
        self.channel = twisted_connection.TwistedChannel(self.impl)

    def test_nested_calls_settle_fifo_without_deferred_write_through(self):
        outer_results = []
        d1 = self.channel.exchange_declare(exchange='outer')
        d1.addCallback(outer_results.append)

        # Reentrant inner call, as issued from a delivery callback while
        # the outer Deferred is still pending.
        inner_results = []
        d2 = self.channel.queue_declare(queue='inner')
        d2.addCallback(inner_results.append)

        self.assertEqual(_sent_names(self.fake_connection),
                         ['Exchange.Declare'])
        self.assertEqual(len(self.channel._calls), 2)
        self.assertEqual(len(self.impl._sync_wait_windows), 2)

        _deliver(self.impl, spec.Exchange.DeclareOk)

        # Deferred scheduling pin: fired directly with the method frame.
        self.assertEqual(len(outer_results), 1)
        self.assertIsInstance(outer_results[0], frame.Method)
        self.assertIsInstance(outer_results[0].method,
                              spec.Exchange.DeclareOk)
        self.assertEqual(inner_results, [])
        self.assertEqual(_sent_names(self.fake_connection),
                         ['Exchange.Declare', 'Queue.Declare'])
        # The outer window closes even though the inner window is still
        # open; the inner settlement is not touched.
        self.assertEqual(len(self.impl._sync_wait_windows), 1)

        _deliver(self.impl, spec.Queue.DeclareOk, queue='inner')
        self.assertEqual(len(inner_results), 1)
        self.assertIsInstance(inner_results[0].method,
                              spec.Queue.DeclareOk)
        self.assertEqual(self.channel._calls, set())
        self.assertEqual(self.impl._sync_wait_windows, [])
        self.assertIsNone(self.impl._blocking)

    def test_channel_close_errbacks_pending_call_and_clears_ledger(self):
        failures = []
        d = self.channel.exchange_declare(exchange='doomed')
        d.addErrback(failures.append)

        self.assertEqual(len(self.channel._calls), 1)
        self.channel._channel._on_close_from_broker(
            frame.Method(
                1,
                spec.Channel.Close(reply_code=404,
                                   reply_text='broker closed')))

        self.assertEqual(len(failures), 1)
        self.assertIsInstance(failures[0].value,
                              exceptions.ChannelClosedByBroker)
        self.assertEqual(self.channel._calls, set())
        self.assertEqual(self.impl._sync_wait_windows, [])
        # No ghost reply callbacks survive on acceptable_replies.
        self.assertIsNone(
            _pending_callbacks(self.impl, spec.Exchange.DeclareOk))
        self.assertIsNone(self.impl._blocking)
        self.assertEqual(len(self.impl._blocked), 0)


if __name__ == '__main__':
    unittest.main()
