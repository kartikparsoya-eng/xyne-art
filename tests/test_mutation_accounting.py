from harness.replay import Stats


def test_late_ack_reconciles_an_abandoned_candidate() -> None:
    stats = Stats(
        mutations_sent=10,
        mutation_ok=8,
        mutation_abandoned=3,
    )
    assert stats.reconciled_mutation_abandoned() == 2


def test_unexpected_unobserved_attempt_is_not_classified_as_abandoned() -> None:
    stats = Stats(
        mutations_sent=10,
        mutation_ok=7,
        mutation_abandoned=2,
    )
    assert stats.reconciled_mutation_abandoned() == 2
    accountable = stats.mutations_sent - stats.reconciled_mutation_abandoned()
    assert stats.mutation_ok / accountable < 0.90
