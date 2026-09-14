import time
from datetime import datetime, timedelta, timezone

from pymongo.errors import PyMongoError

from mongobus import queries
from mongobus._sync.renewal import LockRenewer
from mongobus.context import LockStatus

MESSAGE_ID = "message-1"
PUMP_ID = "host:pump:ep"


class _UpdateResult:
    def __init__(self, matched_count: int):
        self.matched_count = matched_count


class _ScriptedInbox:
    """Answers update_one with the scripted outcomes in order, then keeps matching one document."""

    def __init__(self, *outcomes):
        self.updates = []
        self._outcomes = list(outcomes)

    def _answer(self, filter_, update):
        self.updates.append((filter_, update))
        outcome = self._outcomes.pop(0) if self._outcomes else 1
        if isinstance(outcome, Exception):
            raise outcome
        return _UpdateResult(outcome)

    def update_one(self, filter_, update):
        return self._answer(filter_, update)


def _renewer(inbox, status, lock_seconds=3):
    return LockRenewer(
        inbox, message_id=MESSAGE_ID, pump_id=PUMP_ID, lock_seconds=lock_seconds, lock_status=status
    )


def test_renew_once_extends_the_lock_while_this_delivery_owns_it():
    inbox, status = _ScriptedInbox(1), LockStatus()
    before = datetime.now(timezone.utc)

    assert _renewer(inbox, status, lock_seconds=120).renew_once() is True

    filter_, update = inbox.updates[0]
    assert filter_ == queries.renew_lock_filter(message_id=MESSAGE_ID, pump_id=PUMP_ID)
    assert update["$set"]["LockedUntilUtc"] >= before + timedelta(seconds=120)
    assert status.lost is False


def test_renew_once_marks_the_lock_lost_when_no_document_matches():
    inbox, status = _ScriptedInbox(0), LockStatus()

    assert _renewer(inbox, status).renew_once() is False
    assert status.lost is True


def test_renew_once_keeps_the_lock_state_on_a_driver_error():
    inbox, status = _ScriptedInbox(PyMongoError("network blip")), LockStatus()

    assert _renewer(inbox, status).renew_once() is True
    assert status.lost is False


def test_renewer_keeps_renewing_until_the_handler_finishes():
    inbox, status = _ScriptedInbox(), LockStatus()

    with _renewer(inbox, status):  # lock_seconds=3 → renews every second
        time.sleep(2.5)
    renewals_at_exit = len(inbox.updates)
    time.sleep(1.5)

    assert renewals_at_exit >= 2
    assert len(inbox.updates) == renewals_at_exit
    assert status.lost is False


def test_renewer_stops_renewing_once_the_lock_is_lost():
    inbox, status = _ScriptedInbox(0), LockStatus()

    with _renewer(inbox, status):
        time.sleep(2.5)

    assert len(inbox.updates) == 1
    assert status.lost is True
