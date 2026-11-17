from sqlalchemy import Column,Integer,String,JSON,Text,Float,UniqueConstraint
from src.config.database import Base
class PurgeJob(Base):
 __tablename__='purge_jobs'
 id=Column(Integer,primary_key=True); source_ids=Column(JSON,nullable=False); status=Column(String(20),nullable=False,default='pending'); step=Column(String(30),default='created'); scope=Column(JSON); error=Column(Text); note=Column(Text,default=''); created=Column(Float); updated=Column(Float); completed=Column(Float)
class SourceFingerprint(Base):
 __tablename__='source_fingerprints'
 id=Column(Integer,primary_key=True); source_id=Column(Integer,nullable=False,index=True); fingerprint=Column(String(64),nullable=False,index=True)
 __table_args__=(UniqueConstraint('source_id','fingerprint'),)
