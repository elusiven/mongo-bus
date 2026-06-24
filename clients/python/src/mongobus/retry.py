def backoff_seconds(next_attempt: int) -> int:
    return 2 ** next_attempt


def should_dead_letter(*, next_attempt: int, max_attempts: int) -> bool:
    return next_attempt >= max_attempts
