from fastapi import APIRouter,Depends,HTTPException
from src.config.database import database
from src.controllers.auth import current,admin
from src.models.purge import PurgeJob
from src.schemas.requests import PurgeInput
from src.services.purge import request_purge,execute_purge,job_dict
router=APIRouter()
@router.get('/purges')
def purges(db=Depends(database),u=Depends(current)): return [job_dict(x) for x in db.query(PurgeJob).order_by(PurgeJob.id.desc()).all()]
@router.get('/purges/{id}')
def purge(id:int,db=Depends(database),u=Depends(current)):
 obj=db.get(PurgeJob,id)
 if not obj: raise HTTPException(404,'清退任务不存在')
 return job_dict(obj)
@router.post('/purges')
def create(b:PurgeInput,db=Depends(database),u=Depends(admin)):
 obj=request_purge(db,u,b.source_ids,b.note)
 return job_dict(execute_purge(obj.id) or obj)
@router.post('/purges/{id}/retry')
def retry(id:int,db=Depends(database),u=Depends(admin)):
 obj=db.get(PurgeJob,id)
 if not obj: raise HTTPException(404,'清退任务不存在')
 if obj.status=='completed': return job_dict(obj)
 return job_dict(execute_purge(id,force=True) or obj)
