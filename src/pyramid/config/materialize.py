from pyramid.config.actions import action_method
from pyramid.settings import asbool

_LEDGER_TWEEN = (
    'pyramid.materialize.materialization_ledger_tween_factory'
)


class MaterializationConfiguratorMixin:
    def add_default_tweens(self):
        # chain to the stock default tweens (EXCVIEW, ...) first; this
        # override also runs when pyramid.testing.setUp populates the
        # settings after constructing the configurator
        super().add_default_tweens()
        if asbool(self.registry.settings.get(
            'pyramid.materialization_ledger'
        )):
            self.add_tween(_LEDGER_TWEEN)

    @action_method
    def set_materialization_ledger(self, enabled=True):
        """Enable or disable the per-request materialization ledger.

        When enabled, a tween opens the root
        :class:`pyramid.materialize.MaterializationLedger` scope for
        every request, so that the MultiView probe/execution cuts,
        the ``rendered_view`` materialization forks,
        ``RendererHelper._make_response`` singleton body writes, CSRF
        moments/policies and the ``owrapped_view`` lookup plane are
        recorded on the request as an append-only commitment ledger.

        The same thing can be enabled declaratively via the
        ``pyramid.materialization_ledger`` deployment setting.
        Enabling the ledger changes no observable response behavior
        (the singleton write guard only forks inside explicitly nested
        ledger scopes).
        """
        if enabled:
            self.add_tween(_LEDGER_TWEEN)
