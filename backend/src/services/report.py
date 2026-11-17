import hashlib,json,time
from fastapi import HTTPException
from src.models.source import Source,Policy
from src.models.run import Run
from src.services.audit import log
from src.services.purge import covered_source_ids
from src.config.settings import REDIS_URL
import redis
def submit(db,user,body):
 policy=db.get(Policy,body.policy_id)
 if not policy: raise HTTPException(404,'口径版本不存在')
 # FOR SHARE pairs with the purge's FOR UPDATE on the same rows: a purge that
 # begins now waits for this snapshot, and one already holding the drafts makes
 # this submission fail instead of freezing another copy.
 rows=db.query(Source).with_for_update(read=True).all()
 rows=[x for x in rows if body.period=='全部' or x.data.get('period')==body.period]
 covered=covered_source_ids(db)
 if any(x.id in covered for x in rows): raise HTTPException(409,'所选期间含已清退底稿，不能提交报告')
 sources=[x.data for x in rows]
 if not sources: raise HTTPException(422,'所选期间没有底稿')
 snapshot={'policy':policy.data,'sources':sources,'period':body.period,'source_refs':[{'id':x.id,'natural_key':x.natural_key} for x in rows]}
 checksum=hashlib.sha256(json.dumps(snapshot,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
 obj=Run(name=body.name,status='queued',snapshot=snapshot,checksum=checksum,created=time.time(),approved=False)
 db.add(obj);log(db,user.email,'run.queue',body.name);db.commit()
 redis.Redis.from_url(REDIS_URL).rpush('report-jobs',obj.id)
 return obj
def approve(db,user,id,note):
 # Row lock pairs with the purge's exclusive lock, so an approval racing with
 # a purge either finishes before it is scrubbed or is refused after.
 obj=db.query(Run).filter(Run.id==id).with_for_update().first()
 if not obj: raise HTTPException(404,'报告不存在')
 if obj.purged: raise HTTPException(410,'报告已清退，旧入口失效')
 if obj.status!='completed': raise HTTPException(409,'仅可复核已完成报告')
 if obj.approved: raise HTTPException(409,'报告已经复核')
 obj.approved=True;obj.review=note;log(db,user.email,'run.approve',id);db.commit();return obj
