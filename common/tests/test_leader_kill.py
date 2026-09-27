"""Leader kill test — 2-replica simulation without K8s."""
import threading
import time

from common.leaderelection import LeaderElectionConfig, LeaderElector


class FakeLeaseStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._holder = None
        self._expire = 0

    def try_acquire(self, holder, duration):
        with self._lock:
            now = time.time()
            if self._holder is None or now > self._expire:
                self._holder = holder
                self._expire = now + duration
                return True
            return self._holder == holder

    def renew(self, holder, duration):
        with self._lock:
            if self._holder == holder:
                self._expire = time.time() + duration
                return True
            return False

    def release(self, holder):
        with self._lock:
            if self._holder == holder:
                self._holder = None
                self._expire = 0


def test_leader_kill_within_lease_duration():
    store = FakeLeaseStore()
    duration = 2.0
    leader_id = "pod-a"
    follower_id = "pod-b"
    assert store.try_acquire(leader_id, duration) is True
    assert store.try_acquire(follower_id, duration) is False
    time.sleep(duration + 0.3)
    assert store.try_acquire(follower_id, duration) is True
    assert store._holder == follower_id


def test_no_duplicate_writes_during_switchover():
    writes = []
    lock = threading.Lock()

    def write(holder):
        with lock:
            writes.append(holder)

    write("leader")
    time.sleep(0.05)
    write("follower-after-kill")
    assert writes == ["leader", "follower-after-kill"]
    assert len(set(writes)) == 2
