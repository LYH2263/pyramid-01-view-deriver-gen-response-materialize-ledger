"""
Tests for the inbound delivery generation ledger on pika.channel.Channel and
on the scheduling surfaces of pika.adapters.blocking_connection.BlockingChannel.

The acceptance focus is cross-delivery timing causality on the SAME channel:
staged handoff intents must not reach user callbacks before an explicit
commit, advancement/pop must void the old generation's staged handoffs, half
assemblies and pending confirmation intents, and the Deliver/Get/Return and
waited/nowait/broker/close rules must diverge.
"""
import unittest
from unittest import mock

import pika.adapters.blocking_connection as blocking_connection
from pika import callback, exceptions, frame, spec
from pika.adapters.blocking_connection import (BlockingChannel,
                                               _ConsumerCancellationEvt,
                                               _ConsumerDeliveryEvt,
                                               _ConsumerInfo,
                                               _QueueConsumerGeneratorInfo)
from pika.channel import Channel, ContentFrameAssembler, DeliveryGeneration
from pika import connection as connection_module


class ConnectionTemplate(connection_module.Connection):
    """Mock spec exposing instance members defined by Connection.__init__."""
    callbacks = None

    # Concrete stubs for abstract adapter methods
    _adapter_connect_stream = connection_module.Connection._adapter_connect_stream
    _adapter_disconnect_stream = \
        connection_module.Connection._adapter_disconnect_stream
    _adapter_emit_data = connection_module.Connection._adapter_emit_data
    _adapter_call_later = connection_module.Connection._adapter_call_later
    _adapter_remove_timeout = \
        connection_module.Connection._adapter_remove_timeout
    _adapter_add_callback_threadsafe = \
        connection_module.Connection._adapter_add_callback_threadsafe


def _make_method(method):
    return frame.Method(1, method)


def deliver_frames(tag=1,
                   consumer_tag='ctag0',
                   body=b'hello',
                   redelivered=False):
    return [
        _make_method(
            spec.Basic.Deliver(consumer_tag=consumer_tag,
                               delivery_tag=tag,
                               redelivered=redelivered,
                               exchange='ex',
                               routing_key='rk')),
        frame.Header(1, len(body),
                     spec.BasicProperties(content_type='text/plain')),
        frame.Body(1, body)
    ]


def getok_frames(tag=2, body=b'got'):
    return [
        _make_method(spec.Basic.GetOk(tag, False, 'ex', 'rk', 0)),
        frame.Header(1, len(body),
                     spec.BasicProperties(content_type='text/plain')),
        frame.Body(1, body)
    ]


def return_frames(body=b'returned'):
    return [
        _make_method(spec.Basic.Return(313, 'NO_ROUTE', 'ex', 'rk')),
        frame.Header(1, len(body), spec.BasicProperties()),
        frame.Body(1, body)
    ]


class DeliveryGenerationTestBase(unittest.TestCase):

    @staticmethod
    def feed(channel, frames):
        for value in frames:
            channel._handle_content_frame(value)

    @staticmethod
    def sent_methods(connection_mock):
        return [
            call.args[1] for call in connection_mock._send_method.call_args_list
        ]

    def rejects(self, connection_mock):
        return [
            m for m in self.sent_methods(connection_mock)
            if isinstance(m, spec.Basic.Reject)
        ]

    def acks(self, connection_mock):
        return [
            m for m in self.sent_methods(connection_mock)
            if isinstance(m, spec.Basic.Ack)
        ]

    def nacks(self, connection_mock):
        return [
            m for m in self.sent_methods(connection_mock)
            if isinstance(m, spec.Basic.Nack)
        ]

    def make_channel(self, real_callbacks=False):
        conn = mock.Mock(spec=ConnectionTemplate)
        if real_callbacks:
            conn.callbacks = callback.CallbackManager()
        obj = Channel(conn, 1, None)
        obj._set_state(Channel.OPEN)
        return obj, conn

    def register_consumer(self,
                          obj,
                          consumer_callback=None,
                          tag='ctag0',
                          auto_ack=False):
        consumer_callback = consumer_callback or mock.Mock()
        obj.basic_consume(queue='q',
                          on_message_callback=consumer_callback,
                          auto_ack=auto_ack,
                          consumer_tag=tag)
        return consumer_callback


class StagingAndCommitTests(DeliveryGenerationTestBase):

    def test_deliver_staged_until_commit_via_real_consumer_registration(self):
        # Async direct-call path: registration goes through real
        # basic_consume; assembled frames are held until explicit commit.
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj)
        conn._send_method.reset_mock()

        generation = obj.begin_delivery_generation()
        self.assertEqual(generation.seq, 0)
        self.feed(obj, deliver_frames(tag=11))

        consumer.assert_not_called()
        self.assertEqual(generation.staged_handoff_count, 1)

        generation.commit()

        consumer.assert_called_once()
        args = consumer.call_args.args
        self.assertIs(args[0], obj)
        self.assertIsInstance(args[1], spec.Basic.Deliver)
        self.assertEqual(args[1].delivery_tag, 11)
        self.assertEqual(args[2].content_type, 'text/plain')
        self.assertEqual(args[3], b'hello')
        self.assertIsNone(obj.current_delivery_generation)

    def test_uncommitted_generation_cannot_commit_after_advance(self):
        # Staging into N, no commit, advancing to N+1: N is abandoned, its
        # ackable delivery is rejected, and N can never commit afterwards.
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj)
        conn._send_method.reset_mock()

        generation_zero = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        consumer.assert_not_called()

        generation_one = obj.advance_delivery_generation()

        self.assertEqual(generation_one.seq, 1)
        self.assertTrue(generation_zero.is_abandoned)
        self.assertEqual(self.rejects(conn), [spec.Basic.Reject(1, True)])
        consumer.assert_not_called()

        with self.assertRaises(exceptions.DeliveryGenerationClosed):
            generation_zero.commit()

        # Committing the new generation delivers nothing from the old one.
        generation_one.commit()
        consumer.assert_not_called()

    def test_no_generation_keeps_legacy_immediate_dispatch(self):
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj)

        self.feed(obj, deliver_frames(tag=5))

        consumer.assert_called_once()
        self.assertEqual(consumer.call_args.args[1].delivery_tag, 5)

    def test_double_commit_or_abandon_is_rejected(self):
        obj, _conn = self.make_channel()
        generation = obj.begin_delivery_generation()
        generation.commit()
        with self.assertRaises(exceptions.DeliveryGenerationClosed):
            generation.commit()
        with self.assertRaises(exceptions.DeliveryGenerationClosed):
            generation.abandon()


class SameGenerationKindForkTests(DeliveryGenerationTestBase):

    def _set_up_three_kinds(self, obj):
        consumer = self.register_consumer(obj)
        get_callback = mock.Mock()
        obj._on_getok_callback = get_callback
        return_callback = mock.Mock()
        obj.add_on_return_callback(return_callback)
        return consumer, get_callback, return_callback

    def test_commit_replays_deliver_get_return_in_arrival_order(self):
        obj, conn = self.make_channel(real_callbacks=True)
        consumer, get_callback, return_callback = self._set_up_three_kinds(obj)
        order = []
        consumer.side_effect = lambda *_args: order.append('deliver')
        get_callback.side_effect = lambda *_args: order.append('get')
        return_callback.side_effect = lambda *_args: order.append('return')

        generation = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        self.feed(obj, getok_frames(tag=2))
        self.feed(obj, return_frames())

        self.assertEqual(generation.staged_handoff_count, 3)
        # The one-shot get slot is released when the GetOk is staged.
        self.assertIsNone(obj._on_getok_callback)
        consumer.assert_not_called()
        get_callback.assert_not_called()
        return_callback.assert_not_called()

        generation.commit()

        self.assertEqual(order, ['deliver', 'get', 'return'])
        self.assertEqual(get_callback.call_args.args[1].delivery_tag, 2)
        self.assertEqual(return_callback.call_args.args[3], b'returned')

    def test_abandon_rules_fork_between_deliver_get_return(self):
        obj, conn = self.make_channel(real_callbacks=True)
        consumer, get_callback, return_callback = self._set_up_three_kinds(obj)

        generation = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        self.feed(obj, getok_frames(tag=2))
        self.feed(obj, return_frames())

        generation.abandon(requeue=False)

        self.assertTrue(generation.is_abandoned)
        self.assertEqual(generation.staged_handoff_count, 0)
        # Deliver (ackable) is rejected with requeue=False; Get and Return are
        # dropped with no wire action and their callbacks never fire.
        self.assertEqual(self.rejects(conn), [spec.Basic.Reject(1, False)])
        consumer.assert_not_called()
        get_callback.assert_not_called()
        return_callback.assert_not_called()
        # The released one-shot slot is reusable.
        new_get_callback = mock.Mock()
        obj._on_getok_callback = new_get_callback
        self.feed(obj, getok_frames(tag=3, body=b'again'))
        new_get_callback.assert_called_once()
        self.assertEqual(new_get_callback.call_args.args[1].delivery_tag, 3)

    def test_abandon_drops_noack_deliver_without_reject(self):
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj, auto_ack=True)

        generation = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=9))
        generation.abandon()

        self.assertEqual(self.rejects(conn), [])
        consumer.assert_not_called()


class HalfAssemblyTests(DeliveryGenerationTestBase):

    def test_deliver_partial_invalidated_then_next_message_recovers(self):
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj)

        generation_zero = obj.begin_delivery_generation()
        # Only the method frame has arrived when the generation is abandoned.
        method_frame = deliver_frames(tag=1)[0]
        obj._handle_content_frame(method_frame)
        generation_zero.abandon()

        # Straggler header and body frames must not assemble into a message.
        header_and_body = deliver_frames(tag=1)[1:]
        for value in header_and_body:
            obj._handle_content_frame(value)
        consumer.assert_not_called()
        self.assertEqual(self.rejects(conn), [])

        # The next content method frame resets the assembler.
        generation_one = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=2))
        self.assertEqual(generation_one.staged_handoff_count, 1)
        generation_one.commit()
        consumer.assert_called_once()
        self.assertEqual(consumer.call_args.args[1].delivery_tag, 2)

    def test_get_partial_invalidated_does_not_become_getok_message(self):
        obj, conn = self.make_channel()
        get_callback = mock.Mock()
        obj._on_getok_callback = get_callback

        generation_zero = obj.begin_delivery_generation()
        frames = getok_frames(tag=7)
        obj._handle_content_frame(frames[0])
        generation_zero.abandon()
        for value in frames[1:]:
            obj._handle_content_frame(value)

        get_callback.assert_not_called()
        # The dead flag guards the stragglers until the next content method.
        self.assertTrue(obj._content_assembler._partial_dead)

        generation_one = obj.begin_delivery_generation()
        get_callback_two = mock.Mock()
        obj._on_getok_callback = get_callback_two
        self.feed(obj, getok_frames(tag=8, body=b'next'))
        generation_one.commit()
        get_callback_two.assert_called_once()
        self.assertEqual(get_callback_two.call_args.args[1].delivery_tag, 8)

    def test_return_partial_invalidated_does_not_become_return_message(self):
        obj, conn = self.make_channel(real_callbacks=True)
        return_callback = mock.Mock()
        obj.add_on_return_callback(return_callback)

        generation = obj.begin_delivery_generation()
        frames = return_frames()
        obj._handle_content_frame(frames[0])
        generation.abandon()
        for value in frames[1:]:
            obj._handle_content_frame(value)

        return_callback.assert_not_called()

    def test_nowait_cancel_kills_deliver_partial_but_not_get_partial(self):
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj, tag='ctag0')
        self.register_consumer(obj, tag='ctag1')

        generation = obj.begin_delivery_generation()
        obj._handle_content_frame(
            deliver_frames(tag=1, consumer_tag='ctag0')[0])
        obj.basic_cancel(consumer_tag='ctag0')
        for value in deliver_frames(tag=1, consumer_tag='ctag0')[1:]:
            obj._handle_content_frame(value)
        # A half assembly is discarded, not rejected and never delivered.
        consumer.assert_not_called()
        self.assertEqual(self.rejects(conn), [])

        # A GetOk partial in flight is a different ledger: cancelling another
        # consumer must not kill it.
        get_callback = mock.Mock()
        obj._on_getok_callback = get_callback
        obj._handle_content_frame(getok_frames(tag=3)[0])
        obj.basic_cancel(consumer_tag='ctag1')
        for value in getok_frames(tag=3)[1:]:
            obj._handle_content_frame(value)
        self.assertEqual(generation.staged_handoff_count, 1)
        generation.commit()
        get_callback.assert_called_once()

    def test_bare_assembler_keeps_legacy_tuple_behavior(self):
        assembler = ContentFrameAssembler()
        result = None
        for value in deliver_frames(tag=4):
            result = assembler.process(value) or result
        self.assertIsNotNone(result)
        self.assertEqual(result[0].method.delivery_tag, 4)
        self.assertEqual(result[2], b'hello')


class CancelPathsTests(DeliveryGenerationTestBase):

    def test_waited_cancel_defers_settlement_and_preserves_intents(self):
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj)
        conn._send_method.reset_mock()

        generation = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        generation.ack(1)

        obj.basic_cancel(consumer_tag='ctag0', callback=mock.Mock())

        # Waited cancel: staged handoff and intents survive until CancelOk.
        self.assertEqual(generation.staged_handoff_count, 1)
        self.assertEqual(len(generation._confirmation_intents), 1)
        self.assertEqual(self.rejects(conn), [])

        obj._on_cancelok(_make_method(spec.Basic.CancelOk('ctag0')))

        self.assertEqual(generation.staged_handoff_count, 0)
        self.assertEqual(self.rejects(conn), [spec.Basic.Reject(1, True)])
        # Intents are preserved across the CancelOk boundary.
        self.assertEqual(len(generation._confirmation_intents), 1)
        generation.commit()
        self.assertEqual(self.acks(conn), [spec.Basic.Ack(1, False)])
        consumer.assert_not_called()

    def test_nowait_cancel_settles_immediately_and_drops_intents(self):
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj)
        conn._send_method.reset_mock()

        generation = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        generation.ack(1)

        obj.basic_cancel(consumer_tag='ctag0')

        self.assertEqual(self.rejects(conn), [spec.Basic.Reject(1, True)])
        self.assertEqual(generation._confirmation_intents, [])
        generation.commit()
        self.assertEqual(self.acks(conn), [])
        consumer.assert_not_called()

    def test_broker_cancel_settles_immediately_but_preserves_intents(self):
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj)
        conn._send_method.reset_mock()

        generation = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        generation.ack(1)

        obj._on_cancel(_make_method(spec.Basic.Cancel('ctag0')))

        self.assertEqual(self.rejects(conn), [spec.Basic.Reject(1, True)])
        self.assertNotIn('ctag0', obj._consumers)
        self.assertEqual(len(generation._confirmation_intents), 1)
        generation.commit()
        self.assertEqual(self.acks(conn), [spec.Basic.Ack(1, False)])
        consumer.assert_not_called()

    def test_close_tears_down_whole_ledger_across_all_kinds(self):
        obj, conn = self.make_channel(real_callbacks=True)
        consumer = self.register_consumer(obj)
        get_callback = mock.Mock()
        obj._on_getok_callback = get_callback
        return_callback = mock.Mock()
        obj.add_on_return_callback(return_callback)

        generation = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        self.feed(obj, getok_frames(tag=2))
        self.feed(obj, return_frames())
        generation.ack(1)

        obj.close()

        # Only the ackable staged Deliver is rejected; Get/Return handoffs and
        # all confirmation intents are dropped; every generation is sealed.
        self.assertEqual(self.rejects(conn), [spec.Basic.Reject(1, True)])
        self.assertEqual(self.acks(conn), [])
        self.assertTrue(generation.is_abandoned)
        self.assertIsNone(obj.current_delivery_generation)
        consumer.assert_not_called()
        get_callback.assert_not_called()
        return_callback.assert_not_called()

        with self.assertRaises(exceptions.ChannelWrongStateError):
            obj.begin_delivery_generation()


class ConfirmationIntentTests(DeliveryGenerationTestBase):

    def test_intents_are_sent_only_on_commit_in_order(self):
        obj, conn = self.make_channel()
        self.register_consumer(obj)
        conn._send_method.reset_mock()

        generation = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        generation.ack(1)
        self.feed(obj, deliver_frames(tag=2))
        generation.nack(2, requeue=False)
        self.assertEqual(self.acks(conn), [])
        self.assertEqual(self.nacks(conn), [])

        generation.commit()

        self.assertEqual(self.acks(conn), [spec.Basic.Ack(1, False)])
        self.assertEqual(self.nacks(conn), [spec.Basic.Nack(2, False, False)])

    def test_single_intent_for_unknown_tag_rejected(self):
        obj, _conn = self.make_channel()
        generation = obj.begin_delivery_generation()
        with self.assertRaises(exceptions.DeliveryTagNotInGeneration):
            generation.ack(99)
        with self.assertRaises(exceptions.DeliveryTagNotInGeneration):
            generation.reject(99)

    def test_multiple_intent_is_scoped_to_owned_tags(self):
        obj, conn = self.make_channel()
        self.register_consumer(obj)
        conn._send_method.reset_mock()

        generation = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        self.feed(obj, deliver_frames(tag=2))
        # Inner-style guard: multiple nack up to tag 2 expands per owned tag
        # and never sends a broker-wide multiple that could hit tags owned by
        # an outer generation.
        generation.nack(2, multiple=True)
        generation.commit()

        self.assertEqual(
            self.nacks(conn),
            [spec.Basic.Nack(1, False, True),
             spec.Basic.Nack(2, False, True)])

        # Tag zero covers all tags of the generation, not the whole channel.
        conn._send_method.reset_mock()
        generation_two = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=3))
        generation_two.ack(0, multiple=True)
        generation_two.commit()
        self.assertEqual(self.acks(conn), [spec.Basic.Ack(3, False)])

    def test_abandon_drops_pending_intents_and_seals_handle(self):
        obj, conn = self.make_channel()
        self.register_consumer(obj)
        conn._send_method.reset_mock()

        generation = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        generation.ack(1)
        generation.abandon()

        self.assertEqual(self.acks(conn), [])
        with self.assertRaises(exceptions.DeliveryGenerationClosed):
            generation.ack(1)


class NestedGenerationTests(DeliveryGenerationTestBase):

    def test_inner_generation_cannot_pollute_outer(self):
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj)
        conn._send_method.reset_mock()

        outer = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))
        self.assertEqual(outer.staged_handoff_count, 1)

        inner = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=2))
        self.assertEqual(inner.staged_handoff_count, 1)
        self.assertEqual(outer.staged_handoff_count, 1)

        inner.abandon()

        self.assertEqual(self.rejects(conn), [spec.Basic.Reject(2, True)])
        self.assertTrue(inner.is_abandoned)
        # Outer staging is untouched and its tag is still owned by the outer.
        self.assertEqual(outer.staged_handoff_count, 1)
        self.assertEqual(outer.owned_delivery_tags, {1})
        outer.ack(1)
        with self.assertRaises(exceptions.DeliveryGenerationClosed):
            inner.ack(2)

        outer.commit()

        consumer.assert_called_once()
        self.assertEqual(consumer.call_args.args[1].delivery_tag, 1)
        self.assertEqual(self.acks(conn), [spec.Basic.Ack(1, False)])

    def test_inner_context_exception_cleans_itself_up(self):
        obj, conn = self.make_channel()
        consumer = self.register_consumer(obj)
        conn._send_method.reset_mock()

        class InnerFailure(Exception):
            pass

        outer = obj.begin_delivery_generation()
        self.feed(obj, deliver_frames(tag=1))

        with self.assertRaises(InnerFailure):
            with obj.begin_delivery_generation() as inner:
                self.feed(obj, deliver_frames(tag=2))
                raise InnerFailure()

        self.assertTrue(inner.is_abandoned)
        self.assertEqual(self.rejects(conn), [spec.Basic.Reject(2, True)])
        self.assertIs(obj.current_delivery_generation, outer)
        self.assertEqual(outer.staged_handoff_count, 1)

        outer.commit()
        consumer.assert_called_once()
        self.assertEqual(consumer.call_args.args[1].delivery_tag, 1)

    def test_inner_generation_must_end_before_outer(self):
        obj, _conn = self.make_channel()
        outer = obj.begin_delivery_generation()
        inner = obj.begin_delivery_generation()
        with self.assertRaises(exceptions.DeliveryGenerationError):
            outer.commit()
        with self.assertRaises(exceptions.DeliveryGenerationError):
            outer.abandon()
        inner.commit()
        outer.commit()


class BlockingChannelGenerationTests(DeliveryGenerationTestBase):

    def make_blocking_channel(self):
        conn = mock.Mock(spec=ConnectionTemplate)
        conn.callbacks = callback.CallbackManager()
        impl = Channel(conn, 1, None)
        impl._set_state(Channel.OPEN)

        blocking_conn = mock.Mock(spec=blocking_connection.BlockingConnection)
        blk = BlockingChannel(impl, blocking_conn)
        impl._set_cookie(blk)

        user_callback = mock.Mock()
        # Register the Blocking bridge as the real implementation-channel
        # consumer, exactly as _basic_consume_impl does in production.
        impl.basic_consume(
            queue='q',
            on_message_callback=blk._on_consumer_message_delivery,
            consumer_tag='ctag0')
        blk._consumer_infos['ctag0'] = _ConsumerInfo(
            'ctag0', False, on_message_callback=user_callback)
        conn._send_method.reset_mock()
        return impl, blk, blocking_conn, user_callback, conn

    def _deliver_to_bridge(self, impl, tag=7):
        impl._consumers['ctag0'](
            impl, spec.Basic.Deliver(consumer_tag='ctag0', delivery_tag=tag),
            spec.BasicProperties(content_type='text/plain'), b'queued')

    def test_pending_event_queued_under_generation_is_voided_on_advance(self):
        impl, blk, _blocking_conn, user_callback, _conn = \
            self.make_blocking_channel()

        generation = impl.begin_delivery_generation()
        # Simulate the implementation channel handing a delivery to its
        # registered consumer bridge while the generation is current (the
        # exact call commit replay makes); it enters the real pending queue.
        self._deliver_to_bridge(impl)

        self.assertEqual(len(blk._pending_events), 1)
        queued = blk._pending_events[0]
        self.assertIsInstance(queued, _ConsumerDeliveryEvt)
        self.assertEqual(queued.generation_seq, generation.seq)
        user_callback.assert_not_called()

        impl.advance_delivery_generation()

        # The queued event was purged before dispatch.
        self.assertEqual(len(blk._pending_events), 0)
        blk._dispatch_events()
        user_callback.assert_not_called()

    def test_generator_sink_event_under_generation_is_voided_on_advance(self):
        impl, blk, _blocking_conn, user_callback, _conn = \
            self.make_blocking_channel()
        # Switch the registered consumer to the generator alternate sink.
        blk._consumer_infos['ctag0'] = _ConsumerInfo(
            'ctag0',
            False,
            alternate_event_sink=blk._on_consumer_generator_event)
        blk._queue_consumer_generator = _QueueConsumerGeneratorInfo(
            ('q', False, False), 'ctag0')

        generation = impl.begin_delivery_generation()
        self._deliver_to_bridge(impl)

        sink = blk._queue_consumer_generator.pending_events
        self.assertEqual(len(sink), 1)
        self.assertEqual(sink[0].generation_seq, generation.seq)

        impl.advance_delivery_generation()

        self.assertEqual(len(sink), 0)
        user_callback.assert_not_called()
        # Broker cancellation terminates the generator; the purged message must
        # not have been yielded before termination.
        sink.append(
            _ConsumerCancellationEvt(frame.Method(1,
                                                  spec.Basic.Cancel('ctag0'))))
        iterator = blk.consume('q')
        with self.assertRaises(StopIteration):
            next(iterator)
        user_callback.assert_not_called()

    def test_dead_generation_event_is_skipped_defensively_at_dispatch(self):
        _impl, blk, _blocking_conn, user_callback, _conn = \
            self.make_blocking_channel()

        blk._pending_events.append(
            _ConsumerDeliveryEvt(spec.Basic.Deliver(consumer_tag='ctag0',
                                                    delivery_tag=42),
                                 spec.BasicProperties(),
                                 b'late',
                                 generation_seq=99))
        blk._dead_delivery_generation_seqs.add(99)

        blk._dispatch_events()

        user_callback.assert_not_called()
        self.assertEqual(len(blk._pending_events), 0)

    def test_committed_generation_event_is_dispatched_end_to_end(self):
        impl, blk, _blocking_conn, user_callback, _conn = \
            self.make_blocking_channel()

        generation = impl.begin_delivery_generation()
        # Real frame assembly on the impl channel stages, then commit flushes
        # through the registered bridge into the real pending queue.
        self.feed(impl, deliver_frames(tag=5, body=b'end-to-end'))
        self.assertEqual(len(blk._pending_events), 0)

        generation.commit()

        self.assertEqual(len(blk._pending_events), 1)
        blk._dispatch_events()

        user_callback.assert_called_once()
        self.assertEqual(user_callback.call_args.args[1].delivery_tag, 5)
        self.assertEqual(user_callback.call_args.args[3], b'end-to-end')

    def test_blocking_channel_delegates_generation_api(self):
        impl, blk, _blocking_conn, _user_callback, _conn = \
            self.make_blocking_channel()

        generation = blk.begin_delivery_generation()
        self.assertIsInstance(generation, DeliveryGeneration)
        self.assertIs(blk.current_delivery_generation, generation)
        self.assertIs(impl.current_delivery_generation, generation)

        next_generation = blk.advance_delivery_generation()
        self.assertTrue(generation.is_abandoned)
        self.assertIs(blk.current_delivery_generation, next_generation)
        next_generation.commit()

    def test_without_generation_blocking_dispatch_is_unchanged(self):
        _impl, blk, _blocking_conn, user_callback, _conn = \
            self.make_blocking_channel()

        self._deliver_to_bridge(_impl, tag=8)
        queued = blk._pending_events[0]
        self.assertIsNone(queued.generation_seq)

        blk._dispatch_events()

        user_callback.assert_called_once()
        self.assertEqual(user_callback.call_args.args[1].delivery_tag, 8)


if __name__ == '__main__':
    unittest.main()
