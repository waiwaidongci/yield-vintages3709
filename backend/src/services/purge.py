import hashlib
import json
import time
import traceback

from fastapi import HTTPException
from sqlalchemy import or_

from src.config.database import Session
from src.models.purge import PurgeJob, SourceFingerprint
from src.models.run import Delivery, Run, Schedule
from src.models.source import Source
from src.services.audit import log

PURGE_STEP_LABELS = {
    'created': '待处理',
    'blocked': '已封锁读取与下载',
    'sources': '清除底稿',
    'runs': '清除报告快照与计算结果',
    'deliveries': '清除交付副本',
    'schedules': '停用交付计划',
    'verify': '核对副本与旧入口',
}
PURGE_STATUS_LABELS = {'pending': '待处理', 'running': '清退中', 'completed': '已完成', 'failed': '失败'}


def fingerprint(data):
    """Canonical hash of one draft row, used to recognise copies frozen earlier."""
    return hashlib.sha256(
        json.dumps(data, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()
    ).hexdigest()


def record_fingerprint(db, source_id, data):
    """Remember every content version a draft ever had, so old snapshots match."""
    if not isinstance(source_id, int) or not isinstance(data, dict):
        return
    value = fingerprint(data)
    if not db.query(SourceFingerprint).filter_by(source_id=source_id, fingerprint=value).first():
        db.add(SourceFingerprint(source_id=source_id, fingerprint=value))


def source_fingerprints(db, ids):
    values = set()
    if not ids:
        return values
    for _source_id, value in db.query(SourceFingerprint.source_id, SourceFingerprint.fingerprint).filter(
        SourceFingerprint.source_id.in_(ids)
    ).all():
        values.add(value)
    for source in db.query(Source).filter(Source.id.in_(ids)).all():
        values.add(fingerprint(source.data))
    return values


def _block_deliveries(db, known_ids, run_ids):
    """Lock and clear every delivery copy tied to these reports or already in scope."""
    ids = set(known_ids)
    condition = []
    if run_ids:
        condition.append(Delivery.run_id.in_(sorted(run_ids)))
    if ids:
        condition.append(Delivery.id.in_(sorted(ids)))
    query = db.query(Delivery).with_for_update().filter(or_(*condition)) if condition else db.query(Delivery).filter(Delivery.id < 0)
    for delivery in query.all():
        ids.add(delivery.id)
        scrub_delivery(delivery)
    return ids


def _block_schedules(db, known_ids, run_ids):
    """Lock and disable every schedule that could regenerate a purged copy."""
    ids = set(known_ids)
    condition = []
    if run_ids:
        condition.append(Schedule.run_id.in_(sorted(run_ids)))
    if ids:
        condition.append(Schedule.id.in_(sorted(ids)))
    query = db.query(Schedule).with_for_update().filter(or_(*condition)) if condition else db.query(Schedule).filter(Schedule.id < 0)
    for schedule in query.all():
        ids.add(schedule.id)
        block_schedule(schedule)
    return ids


def _normalise_ids(values):
    try:
        ids = sorted({int(value) for value in values})
    except (TypeError, ValueError) as exc:
        raise HTTPException(422, '底稿编号必须为正整数') from exc
    if not ids or any(value <= 0 for value in ids):
        raise HTTPException(422, '至少指定一个正整数底稿编号')
    return ids


def _current_scope(job):
    value = job.scope or {}
    return {
        'run_ids': sorted(set(value.get('run_ids', []))),
        'delivery_ids': sorted(set(value.get('delivery_ids', []))),
        'schedule_ids': sorted(set(value.get('schedule_ids', []))),
    }


def _save_scope(job, scope):
    job.scope = {
        'run_ids': sorted(set(scope['run_ids'])),
        'delivery_ids': sorted(set(scope['delivery_ids'])),
        'schedule_ids': sorted(set(scope['schedule_ids'])),
    }


def _snapshot_refs(run):
    snapshot = run.snapshot or {}
    refs = snapshot.get('source_refs') or []
    return {int(ref.get('id')) for ref in refs if isinstance(ref, dict) and ref.get('id') is not None}


def _legacy_hashes(run):
    snapshot = run.snapshot or {}
    sources = snapshot.get('sources') or []
    return {fingerprint(item) for item in sources if isinstance(item, dict)}


def run_is_affected(run, ids, hashes):
    """Does this report contain a copy of one of the drafts being purged?

    Reports submitted after this upgrade record exact draft ids, so only those
    ids decide. Older snapshots carry no ids; their frozen rows are compared by
    exact content against the hashes recorded for the purged drafts.
    """
    if run is None:
        return False
    refs = _snapshot_refs(run)
    if refs:
        return bool(refs & ids)
    return bool(_legacy_hashes(run) & hashes)


def scrub_run(run):
    """Remove every derived copy while leaving a tombstone that blocks reads."""
    run.purged = True
    run.status = 'purged'
    run.name = None
    run.snapshot = None
    run.result = None
    run.checksum = None
    run.error = None
    run.review = ''
    run.approved = False


def scrub_delivery(delivery):
    delivery.purged = True
    delivery.payload = None


def block_schedule(schedule):
    schedule.purged = True
    schedule.enabled = False


def covered_source_ids(db):
    """Draft ids covered by any purge job, so late queues can be refused."""
    covered = set()
    for job in db.query(PurgeJob).all():
        covered.update(job.source_ids or [])
    return covered


def _scrub_matching_runs(db, ids, hashes, known_run_ids):
    """Lock and clear every affected report, including ones committed late.

    The scan is a locking read and refreshes identity-mapped rows, because the
    session does not expire objects on commit: without that, a report written
    by a concurrent transaction between phases would be read from stale state.
    """
    run_ids = set(known_run_ids)
    for run in db.query(Run).with_for_update().populate_existing().all():
        if run.id in run_ids or run_is_affected(run, ids, hashes):
            run_ids.add(run.id)
            scrub_run(run)
    return run_ids


def request_purge(db, user, source_ids, note=''):
    """Register a purge and immediately block every affected entry point.

    Repeating the same draft ids returns the existing job, so retries never
    widen the scope. Draft rows stay locked while affected reports are found,
    which serialises against report submission (see report.submit).
    """
    ids = _normalise_ids(source_ids)
    for job in db.query(PurgeJob).order_by(PurgeJob.id.desc()).all():
        if sorted(set(job.source_ids or [])) == ids:
            return job

    sources = db.query(Source).filter(Source.id.in_(ids)).with_for_update().all()
    present = {source.id for source in sources}
    for source in sources:
        record_fingerprint(db, source.id, source.data)
    hashes = source_fingerprints(db, ids)
    missing = set(ids) - present
    if missing:
        referenced = any(_snapshot_refs(run) & missing for run in db.query(Run).all())
        if not hashes and not referenced:
            raise HTTPException(404, '底稿不存在：' + ','.join(map(str, sorted(missing))))

    now = time.time()
    job = PurgeJob(source_ids=ids, status='pending', step='created', note=str(note or ''), created=now, updated=now)
    job.scope = {'run_ids': [], 'delivery_ids': [], 'schedule_ids': []}
    db.add(job)
    db.flush()

    scope = _current_scope(job)
    scope['run_ids'] = sorted(_scrub_matching_runs(db, set(ids), hashes, set(scope['run_ids'])))
    run_ids = set(scope['run_ids'])
    scope['delivery_ids'] = sorted(_block_deliveries(db, scope['delivery_ids'], run_ids))
    scope['schedule_ids'] = sorted(_block_schedules(db, scope['schedule_ids'], run_ids))

    _save_scope(job, scope)
    job.step = 'blocked'
    job.updated = time.time()
    log(db, user.email, 'purge.requested', json.dumps({'job': job.id, 'source_ids': ids, 'runs': sorted(run_ids)}, ensure_ascii=False))
    db.commit()
    return job


def _phase_sources(job_id):
    with Session() as db:
        job = db.get(PurgeJob, job_id)
        ids = list(job.source_ids or [])
        for source in db.query(Source).filter(Source.id.in_(ids)).with_for_update().all():
            record_fingerprint(db, source.id, source.data)
            db.delete(source)
        job.step = 'sources'
        job.updated = time.time()
        db.commit()


def _phase_runs(job_id):
    with Session() as db:
        job = db.get(PurgeJob, job_id)
        ids = set(job.source_ids or [])
        hashes = source_fingerprints(db, ids)
        scope = _current_scope(job)
        scope['run_ids'] = sorted(_scrub_matching_runs(db, ids, hashes, set(scope['run_ids'])))
        _save_scope(job, scope)
        job.step = 'runs'
        job.updated = time.time()
        db.commit()


def _phase_deliveries(job_id):
    with Session() as db:
        job = db.get(PurgeJob, job_id)
        scope = _current_scope(job)
        scope['delivery_ids'] = sorted(_block_deliveries(db, scope['delivery_ids'], set(scope['run_ids'])))
        _save_scope(job, scope)
        job.step = 'deliveries'
        job.updated = time.time()
        db.commit()


def _phase_schedules(job_id):
    with Session() as db:
        job = db.get(PurgeJob, job_id)
        scope = _current_scope(job)
        scope['schedule_ids'] = sorted(_block_schedules(db, scope['schedule_ids'], set(scope['run_ids'])))
        _save_scope(job, scope)
        job.step = 'schedules'
        job.updated = time.time()
        db.commit()


def _verify_and_complete(job_id):
    with Session() as db:
        job = db.get(PurgeJob, job_id)
        ids = set(job.source_ids or [])
        hashes = source_fingerprints(db, ids)
        scope = _current_scope(job)
        # Late reports (queued or running when the purge began) may have
        # committed meanwhile; repeat the scan before marking complete.
        for _ in range(5):
            found = _scrub_matching_runs(db, ids, hashes, set(scope['run_ids']))
            changed = found != set(scope['run_ids'])
            scope['run_ids'] = sorted(found)
            if not changed:
                break
        _save_scope(job, scope)
        job.step = 'verify'
        job.updated = time.time()
        db.commit()

        run_ids = set(scope['run_ids'])
        if db.query(Source).filter(Source.id.in_(ids)).count():
            raise RuntimeError('底稿仍存在于数据库')
        # Compare in Python: JSON columns may store a cleared value as a JSON
        # null token rather than a SQL NULL, which SQL-side checks would miss.
        residual_runs = [
            run.id for run in db.query(Run).filter(Run.id.in_(run_ids)).all()
            if not run.purged or run.status != 'purged' or run.approved
            or run.snapshot is not None or run.result is not None or run.checksum is not None
        ]
        if residual_runs:
            raise RuntimeError('报告快照或计算结果仍有残留：' + ','.join(map(str, residual_runs)))
        residual_deliveries = [
            delivery.id for delivery in db.query(Delivery).filter(Delivery.run_id.in_(run_ids)).all()
            if not delivery.purged or delivery.payload is not None
        ]
        if residual_deliveries:
            raise RuntimeError('交付副本仍有残留：' + ','.join(map(str, residual_deliveries)))
        residual_schedules = [
            schedule.id for schedule in db.query(Schedule).filter(Schedule.run_id.in_(run_ids)).all()
            if not schedule.purged or schedule.enabled
        ]
        if residual_schedules:
            raise RuntimeError('交付计划仍可能生成新副本：' + ','.join(map(str, residual_schedules)))
        job.step = 'verify'
        job.status = 'completed'
        job.completed = time.time()
        job.updated = job.completed
        job.error = None
        log(db, 'system', 'purge.completed', json.dumps({'job': job.id, 'source_ids': sorted(ids), 'runs': sorted(run_ids)}, ensure_ascii=False))
        db.commit()


PURGE_LEASE_SECONDS = 10


def execute_purge(job_id, force=False, stale_seconds=PURGE_LEASE_SECONDS):
    """Run every idempotent phase; interruption leaves a resumable job.

    The claim is a conditional update, so two callers (API, worker, startup)
    can never run the same job at once; a crashed claimant is taken over only
    after its heartbeat goes stale, or immediately when `force` is used.
    """
    now = time.time()
    with Session() as db:
        job = db.get(PurgeJob, job_id)
        if not job or job.status == 'completed':
            return job
        changed = db.query(PurgeJob).filter(
            PurgeJob.id == job_id,
            PurgeJob.status.in_(('pending', 'failed', 'running')),
            or_(PurgeJob.status != 'running', PurgeJob.updated < now - stale_seconds, force),
        ).update({PurgeJob.status: 'running', PurgeJob.error: None, PurgeJob.updated: now}, synchronize_session=False)
        db.commit()
        if not changed:
            return db.get(PurgeJob, job_id)
    try:
        _phase_sources(job_id)
        _phase_runs(job_id)
        _phase_deliveries(job_id)
        _phase_schedules(job_id)
        _verify_and_complete(job_id)
    except Exception as exc:
        traceback.print_exc()
        with Session() as db:
            job = db.get(PurgeJob, job_id)
            if job:
                job.status = 'failed'
                job.error = f'{PURGE_STEP_LABELS.get(job.step, job.step)}：{exc}'
                job.updated = time.time()
                log(db, 'system', 'purge.failed', json.dumps({'job': job.id, 'error': job.error}, ensure_ascii=False))
                db.commit()
    with Session() as db:
        return db.get(PurgeJob, job_id)


def resume_pending(stale_seconds=PURGE_LEASE_SECONDS):
    """Continue jobs left pending or interrupted by a previous process."""
    now = time.time()
    with Session() as db:
        jobs = db.query(PurgeJob).filter(PurgeJob.status.in_(('pending', 'running'))).all()
        candidates = [job.id for job in jobs if job.status == 'pending' or now - (job.updated or 0) >= stale_seconds]
    for job_id in candidates:
        execute_purge(job_id, force=False, stale_seconds=stale_seconds)


def job_dict(job):
    scope = _current_scope(job)
    return {
        'id': job.id,
        'source_ids': job.source_ids or [],
        'status': job.status,
        'status_label': PURGE_STATUS_LABELS.get(job.status, job.status),
        'step': job.step,
        'step_label': PURGE_STEP_LABELS.get(job.step, job.step),
        'error': job.error,
        'note': job.note or '',
        'created': job.created,
        'updated': job.updated,
        'completed': job.completed,
        'scope': scope,
        'counts': {
            'runs': len(scope['run_ids']),
            'deliveries': len(scope['delivery_ids']),
            'schedules': len(scope['schedule_ids']),
        },
    }


def safe_schedule(schedule):
    value = {c.name: getattr(schedule, c.name) for c in schedule.__table__.columns}
    if schedule.purged:
        value['enabled'] = False
        value['run_id'] = schedule.run_id
    return value
