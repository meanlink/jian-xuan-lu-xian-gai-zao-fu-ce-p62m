"""Database leases for analysis execution.

The database is the single source of truth. A worker may only start an analysis
by atomically claiming it, and may only write a result while it still holds the
matching fence token. Every claim increments the token, so a worker whose lease
was taken over can never overwrite the new executor's result.

Redis is only a wake-up hint in front of this: a lost or duplicated queue
message is harmless because SQL still lists every pending analysis.
"""
import logging
import os
import socket
import time
import uuid
from dataclasses import dataclass

from sqlalchemy import and_, func, or_, select, update

from src.models import Job, JobAttempt, JobEvent
from src.services.audit import record

LEASE_SECONDS = float(os.getenv('JOB_LEASE_SECONDS', '20'))
RENEW_SECONDS = float(os.getenv('JOB_LEASE_RENEW_SECONDS', '5'))


def worker_id():
    """Identity of this worker process; unique across restarts and replicas."""
    return f'{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}'


@dataclass
class Lease:
    job_id: int
    token: int
    attempt_id: int
    dataset_id: int
    params: dict


def _claimable_now(now):
    return or_(
        Job.status == 'queued',
        and_(Job.status == 'running',
             or_(Job.lease_expires_at.is_(None), Job.lease_expires_at <= now)),
    )


def _event(db, job_id, actor, action, fence=0, detail=None, audit=True):
    db.add(JobEvent(job_id=job_id, actor=actor, action=action, fence=fence,
                    at=time.time(), detail=detail or {}))
    if audit:
        record(db, actor, 'job.' + action, {'id': job_id, 'fence': fence, **(detail or {})})


def claim(db, job_id, owner, lease_seconds=None):
    """Atomically claim a queued job or take over one whose lease expired.

    Returns a Lease for the single winner, None for every other caller.
    """
    lease_seconds = LEASE_SECONDS if lease_seconds is None else lease_seconds
    now = time.time()
    # Locking read (current read under MySQL REPEATABLE READ) so the state we
    # inspect and the state the condition matches are the same committed row.
    # SQLite ignores the clause and serialises through its own write lock.
    job = db.get(Job, job_id, with_for_update=True)
    if job is None:
        db.rollback()
        return None
    previous_status = job.status
    previous_owner = job.lease_owner
    previous_token = job.lease_token or 0
    if previous_status != 'queued' and not (
            previous_status == 'running' and
            (job.lease_expires_at is None or job.lease_expires_at <= now)):
        db.rollback()
        return None

    # The conditional UPDATE is the authoritative gate: exactly one process can
    # match and increment the fence token, whatever Redis delivered.
    result = db.execute(
        update(Job).where(Job.id == job_id, _claimable_now(now)).values(
            status='running',
            lease_owner=owner,
            lease_token=func.coalesce(Job.lease_token, 0) + 1,
            lease_expires_at=now + lease_seconds,
            claimed_at=now,
            heartbeat_at=now,
            attempt_count=func.coalesce(Job.attempt_count, 0) + 1,
            finished_at=None,
            error='',
        )
    )
    if result.rowcount != 1:
        db.rollback()
        return None

    # We hold the row lock, so our own increment is the new token.
    token = previous_token + 1
    # Any older live attempt is no longer allowed to write.
    db.execute(
        update(JobAttempt).where(
            JobAttempt.job_id == job_id,
            JobAttempt.state == 'running',
            JobAttempt.fence < token,
        ).values(state='superseded', finished_at=now)
    )
    attempt = JobAttempt(job_id=job_id, worker=owner, fence=token, state='running',
                         claimed_at=now, heartbeat_at=now)
    db.add(attempt)
    db.flush()
    if previous_status == 'running':
        _event(db, job_id, owner, 'takeover', token, {
            'previous_worker': previous_owner,
            'previous_fence': previous_token,
            'attempt': attempt.id,
        })
    else:
        _event(db, job_id, owner, 'claimed', token, {'attempt': attempt.id})
    db.commit()
    return Lease(job_id=job_id, token=token, attempt_id=attempt.id,
                 dataset_id=job.dataset_id, params=dict(job.params or {}))


def renew(db, job_id, owner, token, lease_seconds=None):
    """Extend a live lease. False means the fence was lost; stop trusting it."""
    lease_seconds = LEASE_SECONDS if lease_seconds is None else lease_seconds
    now = time.time()
    result = db.execute(
        update(Job).where(
            Job.id == job_id,
            Job.status == 'running',
            Job.lease_owner == owner,
            Job.lease_token == token,
        ).values(lease_expires_at=now + lease_seconds, heartbeat_at=now)
    )
    if result.rowcount != 1:
        db.rollback()
        return False
    renewed = db.execute(
        update(JobAttempt).where(
            JobAttempt.job_id == job_id,
            JobAttempt.fence == token,
            JobAttempt.state == 'running',
        ).values(heartbeat_at=now, renew_count=JobAttempt.renew_count + 1)
    )
    if renewed.rowcount == 1:
        # Show the first renewal, then at most one entry every ~30s, so a
        # long analysis has visible liveness without a flooded trail.
        count = db.scalar(select(JobAttempt.renew_count).where(
            JobAttempt.job_id == job_id, JobAttempt.fence == token))
        every = max(1, int(30 // max(RENEW_SECONDS, 0.1)))
        if count and (count == 1 or count % every == 0):
            _event(db, job_id, owner, 'renewed', token, {'renewals': count}, audit=False)
    db.commit()
    return True


def finish(db, job_id, owner, token, result=None, error=None):
    """Commit a result only while still holding the fence.

    A stale executor's late success or failure returns False and is recorded
    instead of overwriting the current executor's result.
    """
    now = time.time()
    values = {'finished_at': now, 'lease_owner': None, 'lease_expires_at': None,
              'heartbeat_at': now}
    outcome = 'failed' if error is not None else 'completed'
    if error is None:
        values.update(status='completed', result=result, error='')
    else:
        values.update(status='failed', error=error[:990])
    updated = db.execute(
        update(Job).where(
            Job.id == job_id,
            Job.status == 'running',
            Job.lease_owner == owner,
            Job.lease_token == token,
        ).values(**values)
    )
    if updated.rowcount != 1:
        db.rollback()
        _event(db, job_id, owner, 'write_rejected', token, {
            'outcome': outcome, 'error': error[:500] if error else '',
        })
        db.commit()
        return False
    db.execute(
        update(JobAttempt).where(
            JobAttempt.job_id == job_id,
            JobAttempt.fence == token,
            JobAttempt.state == 'running',
        ).values(state=outcome, finished_at=now, heartbeat_at=now)
    )
    detail = {'attempt': token}
    if error is not None:
        detail['error'] = error[:500]
    _event(db, job_id, owner, outcome, token, detail)
    db.commit()
    return True


def heartbeat(job_id, owner, token, stop, lost, interval=None, on_beat=None):
    """Keep a long-running analysis alive; mark it lost if it was taken over."""
    from src.config.database import Session

    interval = RENEW_SECONDS if interval is None else interval
    while not stop.wait(interval):
        if on_beat is not None:
            on_beat()
        try:
            with Session() as db:
                if renew(db, job_id, owner, token):
                    continue
        except Exception:
            # A transient database error is not proof the lease was lost.
            logging.exception('Lease renewal failed; will retry')
            continue
        lost.set()
        try:
            with Session() as audit_db:
                _event(audit_db, job_id, owner, 'lease_lost', token, {}, audit=False)
                audit_db.commit()
        except Exception:
            logging.exception('Could not record lost lease')
        return


def claimable_ids(db, limit=3):
    """Pending work straight from SQL, independent of Redis delivery."""
    return list(db.scalars(
        select(Job.id).where(_claimable_now(time.time())).order_by(Job.id).limit(limit)
    ))


def history(db, job_id, limit=100):
    attempts = list(db.scalars(
        select(JobAttempt).where(JobAttempt.job_id == job_id)
        .order_by(JobAttempt.id.desc()).limit(limit)
    ))
    events = list(db.scalars(
        select(JobEvent).where(JobEvent.job_id == job_id)
        .order_by(JobEvent.id.desc()).limit(limit)
    ))
    return {
        'attempts': [{
            'id': a.id, 'worker': a.worker, 'fence': a.fence, 'state': a.state,
            'claimed_at': a.claimed_at, 'heartbeat_at': a.heartbeat_at,
            'finished_at': a.finished_at, 'renew_count': a.renew_count,
        } for a in attempts],
        'events': [{
            'id': e.id, 'actor': e.actor, 'action': e.action, 'fence': e.fence,
            'detail': e.detail, 'at': e.at,
        } for e in events],
    }
