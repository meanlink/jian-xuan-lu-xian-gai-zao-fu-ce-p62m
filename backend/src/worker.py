import time, json, logging, os, signal, threading
from pathlib import Path
from sqlalchemy import select
from redis import Redis
from src.config.database import Session
from src.config.settings import REDIS_URL
from src.models import Job, Dataset, Report
from src.services.business import analyze
from src.services.reports import deliver
from src.services import leases
from src.services.audit import record
logging.basicConfig(level=logging.INFO)
LEASE_SECONDS=float(os.getenv('JOB_LEASE_SECONDS','20'))
# Queue messages older than the lease can safely be ignored; the SQL scan still
# sees any job that genuinely needs an executor.
QUEUE_KEY='analysis:queue'
def execute(job_id, owner):
    with Session() as db:
        lease=leases.claim(db,job_id,owner)
    if not lease:return False
    stop=threading.Event();lost=threading.Event()
    beat=threading.Thread(target=leases.heartbeat,args=(lease.job_id,owner,lease.token,stop,lost),
                          kwargs={'on_beat':lambda:Path('/tmp/heartbeat').touch()},daemon=True)
    beat.start()
    try:
        with Session() as work:
            d=work.get(Dataset,lease.dataset_id)
            result=analyze(d.records,lease.params)
            json.dumps(result,allow_nan=False)
        stop.set();beat.join()
        if lost.is_set():
            logging.warning('Lease for job %s was taken over; result discarded',job_id)
            return False
        with Session() as db:
            leases.finish(db,lease.job_id,owner,lease.token,result=result)
        return True
    except Exception as e:
        stop.set();beat.join()
        logging.exception('Analysis failed')
        if lost.is_set():
            return False
        with Session() as db:
            leases.finish(db,lease.job_id,owner,lease.token,error=str(e))
        return True
def tick(owner):
    # SQL is authoritative for pending work; a lost queue message cannot strand
    # a job, and a duplicated message cannot double-run one.
    with Session() as db:
        pending=leases.claimable_ids(db,limit=3)
    for job_id in pending:execute(job_id,owner)
    with Session() as db:
        for report_id in list(db.scalars(select(Report.id).where(Report.enabled==True,Report.next_run<=time.time()))):
            try:
                r=db.get(Report,report_id)
                if not r:continue
                d=deliver(db,r)
                if d:record(db,'worker','report.delivered',{'id':r.id,'delivery':d.id})
                db.commit()
            except Exception:
                # A failed artifact write is retried on the next tick because the
                # delivery was claimed in the same transaction as it is rolled back.
                db.rollback();logging.exception('Delivery %s retry',report_id)
def _stopping(signum,frame):
    # Let the current analysis finish and commit; do not leave a half-written row.
    Path('/tmp/worker-stop').touch()
def main():
    owner=leases.worker_id()
    logging.info('Worker %s starting with %.0fs leases',owner,LEASE_SECONDS)
    try:r=Redis.from_url(REDIS_URL,socket_timeout=3)
    except Exception:r=None
    try:
        signal.signal(signal.SIGTERM,_stopping);signal.signal(signal.SIGINT,_stopping)
    except ValueError:
        logging.debug('Signal handlers unavailable outside the main thread')
    queue_ok=True
    while True:
        Path('/tmp/heartbeat').touch()
        if Path('/tmp/worker-stop').exists():
            Path('/tmp/worker-stop').unlink(missing_ok=True)
            logging.info('Worker %s stopping cleanly; unfinished leases will expire',owner)
            return
        job_id=None
        try:
            item=r.blpop(QUEUE_KEY,timeout=1) if r else None
            if item:
                try:job_id=int(item[1])
                except (TypeError,ValueError):logging.warning('Ignoring malformed queue message %r',item[1])
            queue_ok=True
        except Exception:
            # Redis is only a nudge; the SQL scan in tick() still finds work.
            if queue_ok:logging.warning('Queue unavailable; continuing with SQL fallback')
            queue_ok=False
            time.sleep(1)
        try:
            if job_id is not None:execute(job_id,owner)
            tick(owner)
        except Exception:
            logging.exception('Worker retry');time.sleep(2)
if __name__=='__main__':main()
