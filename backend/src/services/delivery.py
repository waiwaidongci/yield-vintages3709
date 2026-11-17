import time
from fastapi import HTTPException
from src.models.run import Run,Schedule,Delivery
from src.services.audit import log
def add_schedule(db,user,body):
 run=db.query(Run).filter(Run.id==body.run_id).with_for_update().first()
 if not run or run.purged or run.status=='purged' or not run.approved: raise HTTPException(409,'定时交付需引用已复核报告')
 obj=Schedule(name=body.name,run_id=run.id,interval_seconds=body.interval_seconds,next_at=time.time()+body.interval_seconds,enabled=True)
 db.add(obj);log(db,user.email,'schedule.create',body.name);db.commit();return obj
def tick(db):
 now=time.time()
 due=[(row.id,row.run_id) for row in db.query(Schedule.id,Schedule.run_id).filter(Schedule.enabled==True,Schedule.next_at<=now).all()]
 for schedule_id,run_id in due:
  # Lock the run before the schedule, the same order the purge uses, so a purge
  # either clears the report first or finds this delivery copy afterwards.
  run=db.query(Run).filter(Run.id==run_id).with_for_update().first()
  schedule=db.query(Schedule).filter(Schedule.id==schedule_id).with_for_update().first()
  if not schedule or schedule.purged or not schedule.enabled or schedule.next_at>now: continue
  if run and not run.purged and run.status=='completed' and run.approved and run.snapshot and run.result:
   key=f'{schedule.id}:{schedule.next_at}'
   if not db.query(Delivery).filter_by(due_key=key).with_for_update().first():
    db.add(Delivery(schedule_id=schedule.id,run_id=run.id,due_key=key,created=now,payload={'name':run.name,'checksum':run.checksum,'result':run.result,'review':run.review,'policy':run.snapshot['policy']}))
    log(db,'worker','delivery.created',key)
  schedule.next_at=now+schedule.interval_seconds
 db.commit()
