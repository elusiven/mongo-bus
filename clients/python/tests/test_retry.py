from mongobus import retry


def test_backoff_is_exponential():
    assert retry.backoff_seconds(1) == 2
    assert retry.backoff_seconds(2) == 4
    assert retry.backoff_seconds(3) == 8


def test_should_dead_letter_at_or_above_max():
    assert retry.should_dead_letter(next_attempt=10, max_attempts=10) is True
    assert retry.should_dead_letter(next_attempt=11, max_attempts=10) is True


def test_should_not_dead_letter_below_max():
    assert retry.should_dead_letter(next_attempt=9, max_attempts=10) is False
