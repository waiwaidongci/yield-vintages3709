import time,traceback
from pathlib import Path
import redis
from src.config.database import Base,engine,Session
from src.config.settings import REDIS_URL
from src.models import identity,source,run,purge  # noqa: F401  建表需先注册所有模型
from src.models.run import Run
from src.services.calculation import calculate
from src.services.delivery import tick
from src.services.purge import resume_pending

def scrub(job):
 job.purged=True;job.snapshot=None;job.result=None;job.checksum=None;job.error=None;job.review='';job.approved=False;job.status='purged'

def claim(job_id):
 """Mark a queued report running and return its frozen snapshot, else None."""
 with Session() as db:
  job=db.query(Run).filter(Run.id==job_id).with_for_update().first()
  if not job: return None
  if job.purged or job.status=='purged':
   # A purge owns this report: never write a late result back into it.
   scrub(job);db.commit();return None
  if job.status!='queued': return None
  snapshot=job.snapshot
  job.status='running';db.commit()
  return snapshot

def publish(job_id,result,error):
 # Publish in a fresh locked transaction and re-check the purge tombstone, so a
 # report finished late (after its drafts were purged) stays scrubbed.
 with Session() as db:
  job=db.query(Run).filter(Run.id==job_id).with_for_update().first()
  if not job: return
  if job.purged or job.status=='purged':
   scrub(job)
  elif error is not None:
   job.status='failed';job.error=error
  else:
   job.result=result;job.status='completed'
  db.commit()

def run_job(job_id,r=None):
 snapshot=claim(job_id)
 if not snapshot: return
 error=None;result=None
 try: result=calculate(snapshot['sources'],snapshot['policy'])
 except Exception as exc: error=str(exc);traceback.print_exc()
 publish(job_id,result,error)

def main():
 Base.metadata.create_all(engine)
 r=redis.Redis.from_url(REDIS_URL)
 while True:
  Path('/tmp/worker-heartbeat').write_text(str(time.time()))
  try:
   item=r.lpop('report-jobs')
   if item: run_job(int(item))
   resume_pending()
   with Session() as db: tick(db)
  except Exception: traceback.print_exc()
  time.sleep(1)
if __name__=='__main__': main()
