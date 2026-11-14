"""Concurrency and recovery checks for analysis execution.

Run from backend/ with a throwaway SQLite file:
    DATABASE_URL=sqlite:////tmp/leases-test.db python -m tests.test_leases
"""
import os
import threading
import time

from sqlalchemy import select

from src.config.database import Session
from src.models import Dataset, Job, User
from src.services import leases
from src.services.business import analyze, seed_records


def _seed():
    from migrations.initial import migrate
    migrate()
    records = seed_records()
    with Session() as db:
        u = User(email='lease-test@local', name='test', role='admin', password='x')
        db.add(u)
        db.flush()
        d = Dataset(name='lease-test-dataset', source='inline', records=records,
                    checksum='test', created_at=time.time())
        db.add(d)
        db.flush()
        ids = []
        for n in range(4):
            j = Job(name=f'lease-test-{n}', dataset_id=d.id,
                    params={'team': '全部', 'confidence': '0.95'},
                    created_by=u.id, created_at=time.time(), status='queued')
            db.add(j)
            db.flush()
            ids.append(j.id)
        db.commit()
        return ids, d.id, records


def test_single_executor(ids, dataset_id, records):
    results = []

    def race(index):
        for job_id in ids:
            with Session() as db:
                lease = leases.claim(db, job_id, f'worker-{index}')
            if lease:
                result = analyze(records, lease.params)
                with Session() as db:
                    results.append((job_id, leases.finish(db, job_id, f'worker-{index}',
                                                          lease.token, result=result)))

    threads = [threading.Thread(target=race, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    with Session() as db:
        for job_id in ids:
            job = db.get(Job, job_id)
            assert job.status == 'completed', job.status
            assert job.attempt_count == 1, job.attempt_count
            assert len([r for r in results if r[0] == job_id]) == 1
            events = [e['action'] for e in leases.history(db, job_id)['events']]
            assert events.count('claimed') == 1, events
    print('single executor ok')


def test_renew_holds_lease(dataset_id, records):
    with Session() as db:
        job = Job(name='lease-test-long', dataset_id=dataset_id,
                  params={'team': '全部', 'confidence': '0.95'},
                  status='queued', created_by=1, created_at=time.time())
        db.add(job)
        db.commit()
        job_id = job.id
    with Session() as db:
        lease = leases.claim(db, job_id, 'long-worker', lease_seconds=1)
    stop, lost = threading.Event(), threading.Event()
    beat = threading.Thread(target=leases.heartbeat,
                            args=(job_id, 'long-worker', lease.token, stop, lost),
                            kwargs={'interval': 0.3}, daemon=True)
    beat.start()
    time.sleep(1.6)
    with Session() as db:
        assert leases.claim(db, job_id, 'thief') is None
    stop.set()
    beat.join()
    assert not lost.is_set()
    with Session() as db:
        assert leases.renew(db, job_id, 'long-worker', lease.token), 'lease lost early'
        assert leases.finish(db, job_id, 'long-worker', lease.token,
                             result=analyze(records, lease.params))
    print('renew holds lease ok')


def test_takeover_fences_old_writer(dataset_id, records):
    with Session() as db:
        job = Job(name='lease-test-crash', dataset_id=dataset_id,
                  params={'team': '全部', 'confidence': '0.95'},
                  status='queued', created_by=1, created_at=time.time())
        db.add(job)
        db.commit()
        job_id = job.id
    with Session() as db:
        old = leases.claim(db, job_id, 'crashed', lease_seconds=0.4)
    time.sleep(0.7)
    with Session() as db:
        new = leases.claim(db, job_id, 'rescue', lease_seconds=5)
    assert new and new.token == old.token + 1
    with Session() as db:
        assert leases.finish(db, job_id, 'rescue', new.token,
                             result=analyze(records, new.params))
    with Session() as db:
        assert not leases.finish(db, job_id, 'crashed', old.token,
                                 result={'summary': 'stale'})
        assert not leases.finish(db, job_id, 'crashed', old.token, error='stale failure')
        job = db.get(Job, job_id)
        assert job.status == 'completed'
        assert job.result.get('summary') != 'stale'
        events = [e['action'] for e in leases.history(db, job_id)['events']]
        assert 'takeover' in events and events.count('write_rejected') == 2, events
        attempts = {a['fence']: a['state'] for a in leases.history(db, job_id)['attempts']}
        assert attempts[old.token] == 'superseded' and attempts[new.token] == 'completed'
    print('takeover fences stale writes ok')


def test_duplicate_message_no_rerun(ids, records):
    with Session() as db:
        job = db.get(Job, ids[0])
        assert job.status == 'completed'
        for index in range(5):
            assert leases.claim(db, ids[0], f'late-{index}') is None
        attempts = db.get(Job, ids[0]).attempt_count
        assert attempts == 1, attempts
    print('duplicate messages ignored ok')




def test_delivery_once(dataset_id, records):
    from src.models import Report, Delivery
    from src.services import reports
    from src.config.settings import ARTIFACT_DIR
    result = analyze(records, {'team': '全部', 'confidence': '0.95'})
    with Session() as db:
        j = db.get(Job, 1)
        j.approved = True
        r = Report(name='lease-test-report', job_id=j.id, destination='收件箱',
                   interval_seconds=60, next_run=time.time() - 1, enabled=True)
        db.add(r)
        db.commit()
        report_id = r.id
    import src.worker as worker
    threads = [threading.Thread(target=worker.tick, args=(f'delivery-{i}',)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    with Session() as db:
        report = db.get(Report, report_id)
        deliveries = list(db.scalars(select(Delivery).where(Delivery.report_id == report_id)))
        assert len(deliveries) == 1, [d.id for d in deliveries]
        assert report.next_run > time.time()
        assert os.path.exists(os.path.join(ARTIFACT_DIR, deliveries[0].filename))
    print('delivery published once ok')


if __name__ == '__main__':
    ids, dataset_id, records = _seed()
    test_single_executor(ids, dataset_id, records)
    test_renew_holds_lease(dataset_id, records)
    test_takeover_fences_old_writer(dataset_id, records)
    test_duplicate_message_no_rerun(ids, records)
    test_delivery_once(dataset_id, records)
    print('all lease checks passed')
