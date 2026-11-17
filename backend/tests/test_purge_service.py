"""清退服务：范围、封锁、并发迟到、续办与幂等。"""
import hashlib
import json
import time

import pytest

from src.models.purge import PurgeJob
from src.models.run import Delivery, Run, Schedule
from src.models.source import Source
from src.services import purge as P
from src.services.calculation import calculate
from src.services.delivery import tick
from conftest import POLICY, Actor, add_source, completed_run, new_run, schedule_for, unique_draft


def status(db, run_id):
    return db.get(Run, run_id)


def test_block_happens_before_cleanup_and_unrelated_survives(db_env):
    with db_env() as db:
        a = add_source(db, 'a', unique_draft('2026-11-01', 50))
        b = add_source(db, 'b', unique_draft('2026-11-02', 50))
        dup = add_source(db, 'dup', unique_draft('2026-11-01', 50))  # identical content, other id
        linked = completed_run(db, '报告甲')
        unrelated = completed_run(db, '报告乙', '2026-11-02')
        schedule_for(db, linked)
        tick(db)
        assert db.query(Delivery).count() == 1
        ids = dict(a=a.id, b=b.id, dup=dup.id, linked=linked.id, unrelated=unrelated.id)

    with db_env() as db:
        job = P.request_purge(db, Actor(), [ids['a']], '误采保密底稿')
        job_id = job.id
        assert job.status == 'pending' and job.step == 'blocked'
        # blocked immediately: old entry points must already refuse
        assert status(db, ids['linked']).purged
        assert status(db, ids['linked']).status == 'purged'
        assert status(db, ids['linked']).approved is False
        assert db.query(Delivery).filter_by(purged=True).count() == 1
        assert db.query(Schedule).filter_by(purged=True).count() == 1
        assert db.query(Schedule).filter_by(purged=True).one().enabled is False
        assert not status(db, ids['unrelated']).purged

    P.execute_purge(job_id)
    with db_env() as db:
        job = db.get(PurgeJob, job_id)
        assert job.status == 'completed', job.error
        assert db.query(Source).filter(Source.id == ids['a']).count() == 0
        run = status(db, ids['linked'])
        assert run.snapshot is None and run.result is None and run.checksum is None and run.review == ''
        delivery = db.query(Delivery).filter_by(purged=True).one()
        assert delivery.payload is None
        # same-content duplicate draft and its report are untouched
        assert db.query(Source).filter(Source.id.in_([ids['b'], ids['dup']])).count() == 2
        other = status(db, ids['unrelated'])
        assert other.result and other.snapshot and other.status == 'completed'
        assert other.approved is True


def test_legacy_snapshot_without_refs_matched_by_frozen_hash(db_env):
    with db_env() as db:
        a = add_source(db, 'legacy', unique_draft('2026-11-01', 55))
        snapshot = {'policy': POLICY, 'sources': [unique_draft('2026-11-01', 55)], 'period': '全部'}
        legacy = Run(name='旧报告', status='completed', snapshot=snapshot,
                     checksum=hashlib.sha256(json.dumps(snapshot, sort_keys=True).encode()).hexdigest(),
                     created=time.time(), approved=True)
        db.add(legacy)
        db.commit()
        legacy.result = calculate(snapshot['sources'], snapshot['policy'])
        db.commit()
        ids = dict(a=a.id, run=legacy.id)
    with db_env() as db:
        job = P.request_purge(db, Actor(), [ids['a']])
    P.execute_purge(job.id)
    with db_env() as db:
        run = db.get(Run, ids['run'])
        assert run.purged and run.snapshot is None and run.result is None


def test_repeat_same_ids_returns_same_job_and_wider_scope_is_new(db_env):
    with db_env() as db:
        a = add_source(db, 'a', unique_draft('2026-11-01', 50))
        b = add_source(db, 'b', unique_draft('2026-11-02', 50))
        r1 = completed_run(db, '报告甲')
        r2 = completed_run(db, '报告乙', '2026-11-02')
        ids = dict(a=a.id, b=b.id, r1=r1.id, r2=r2.id)
    with db_env() as db:
        first = P.request_purge(db, Actor(), [ids['a']])
        first_id = first.id
    P.execute_purge(first_id)
    with db_env() as db:
        again = P.request_purge(db, Actor(), [ids['a']])
        assert again.id == first_id
        assert db.query(PurgeJob).count() == 1
        wider = P.request_purge(db, Actor(), [ids['a'], ids['b']])
        assert wider.id != first_id
        assert sorted(db.get(PurgeJob, first_id).source_ids) == [ids['a']]
        assert status(db, ids['r2']).purged  # newly covered by the wider job
    P.execute_purge(wider.id)
    with db_env() as db:
        assert db.get(PurgeJob, first_id).status == 'completed'
        assert db.get(PurgeJob, wider.id).status == 'completed'
        assert db.query(Source).filter(Source.id.in_([ids['a'], ids['b']])).count() == 0


def test_interrupted_job_is_resumed(db_env):
    with db_env() as db:
        a = add_source(db, 'a', unique_draft('2026-11-01', 50))
        run = completed_run(db, '报告甲')
        ids = dict(a=a.id, run=run.id)
    with db_env() as db:
        job = P.request_purge(db, Actor(), [ids['a']])
        job_id = job.id
        job.status = 'running'
        job.step = 'sources'
        job.updated = 0  # crashed process: lease expired
        db.commit()
    P.resume_pending(stale_seconds=P.PURGE_LEASE_SECONDS)
    with db_env() as db:
        job = db.get(PurgeJob, job_id)
        assert job.status == 'completed', job.error
        assert db.query(Source).filter(Source.id == ids['a']).count() == 0
        assert db.get(Run, ids['run']).result is None


def test_failure_records_reason_stays_blocked_and_retry_finishes(db_env):
    with db_env() as db:
        a = add_source(db, 'a', unique_draft('2026-11-01', 50))
        run = completed_run(db, '报告甲')
        ids = dict(a=a.id, run=run.id)
    original = P._phase_deliveries

    def boom(job_id):
        raise RuntimeError('磁盘写入失败')

    P._phase_deliveries = boom
    try:
        with db_env() as db:
            job = P.request_purge(db, Actor(), [ids['a']])
            job_id = job.id
        P.execute_purge(job_id)
    finally:
        P._phase_deliveries = original
    with db_env() as db:
        job = db.get(PurgeJob, job_id)
        assert job.status == 'failed'
        assert '磁盘写入失败' in job.error
        blocked = db.get(Run, ids['run'])
        assert blocked.purged and blocked.result is None  # never restored while failed
    P.execute_purge(job_id, force=True)
    with db_env() as db:
        assert db.get(PurgeJob, job_id).status == 'completed'
        assert db.query(Source).filter(Source.id == ids['a']).count() == 0


def test_queued_report_claimed_by_worker_after_purge_stays_scrubbed(db_env):
    with db_env() as db:
        a = add_source(db, 'a', unique_draft('2026-11-01', 50))
        queued = new_run(db, '排队报告')
        ids = dict(a=a.id, run=queued.id)
    with db_env() as db:
        job = P.request_purge(db, Actor(), [ids['a']])
        job_id = job.id
    import src.worker as W
    W.run_job(ids['run'])
    with db_env() as db:
        run = db.get(Run, ids['run'])
        assert run.purged and run.status == 'purged'
        assert run.result is None and run.snapshot is None
    P.execute_purge(job_id)
    with db_env() as db:
        assert db.get(PurgeJob, job_id).status == 'completed'
        assert db.get(Run, ids['run']).result is None


def test_purge_arriving_while_worker_computes_never_restores_result(db_env):
    with db_env() as db:
        a = add_source(db, 'a', unique_draft('2026-11-01', 50))
        run = new_run(db, '并发报告')
        ids = dict(a=a.id, run=run.id)
    import src.worker as W

    def racing(sources, policy):
        # The purge lands between claim and publish.
        with db_env() as db:
            P.request_purge(db, Actor(), [ids['a']])
        return calculate(sources, policy)

    W.calculate = racing
    W.run_job(ids['run'])
    with db_env() as db:
        run = db.get(Run, ids['run'])
        assert run.purged and run.status == 'purged' and run.result is None
        job = db.query(PurgeJob).order_by(PurgeJob.id.desc()).first()
    P.execute_purge(job.id)
    with db_env() as db:
        assert db.get(PurgeJob, job.id).status == 'completed'
        assert db.get(Run, ids['run']).result is None


def test_worker_publishing_after_purge_does_not_leak(db_env):
    """A report claimed before the purge, finishing after it, stays scrubbed."""
    with db_env() as db:
        a = add_source(db, 'a', unique_draft('2026-11-01', 50))
        run = new_run(db, '迟到报告')
        ids = dict(a=a.id, run=run.id)
    import src.worker as W
    snapshot = W.claim(ids['run'])
    assert snapshot is not None
    with db_env() as db:
        job = P.request_purge(db, Actor(), [ids['a']])
        job_id = job.id
    P.execute_purge(job_id)
    result = calculate(snapshot['sources'], snapshot['policy'])
    W.publish(ids['run'], result, None)
    with db_env() as db:
        run = db.get(Run, ids['run'])
        assert run.purged and run.status == 'purged'
        assert run.result is None and run.snapshot is None and run.checksum is None
        assert db.get(PurgeJob, job_id).status == 'completed'


def test_worker_failure_after_purge_stays_scrubbed(db_env):
    with db_env() as db:
        a = add_source(db, 'a', unique_draft('2026-11-01', 50))
        run = new_run(db, '迟到失败')
        ids = dict(a=a.id, run=run.id)
    import src.worker as W
    W.claim(ids['run'])
    with db_env() as db:
        P.request_purge(db, Actor(), [ids['a']])
    W.publish(ids['run'], None, '计算异常')
    with db_env() as db:
        run = db.get(Run, ids['run'])
        assert run.purged and run.status == 'purged' and run.error is None


def test_deleted_draft_can_still_be_purged_by_id(db_env):
    with db_env() as db:
        a = add_source(db, 'a', unique_draft('2026-11-01', 50))
        run = completed_run(db, '报告甲')
        # Legacy plain delete: fingerprint recorded, row gone.
        P.record_fingerprint(db, a.id, a.data)
        db.delete(a)
        db.commit()
        ids = dict(a=a.id, run=run.id)
    with db_env() as db:
        job = P.request_purge(db, Actor(), [ids['a']])
    P.execute_purge(job.id)
    with db_env() as db:
        assert db.get(PurgeJob, job.id).status == 'completed'
        assert db.get(Run, ids['run']).snapshot is None


def test_unknown_id_is_rejected(db_env):
    from fastapi import HTTPException
    with db_env() as db:
        with pytest.raises(HTTPException) as exc:
            P.request_purge(db, Actor(), [99999])
        assert exc.value.status_code == 404


def test_timer_tick_cannot_regenerate_after_blocking(db_env):
    with db_env() as db:
        a = add_source(db, 'a', unique_draft('2026-11-01', 50))
        run = completed_run(db, '报告甲')
        schedule = schedule_for(db, run, interval=5)
        tick(db)
        assert db.query(Delivery).count() == 1
        ids = dict(a=a.id, run=run.id, schedule=schedule.id)
    with db_env() as db:
        job = P.request_purge(db, Actor(), [ids['a']])
        job_id = job.id
    with db_env() as db:
        schedule = db.get(Schedule, ids['schedule'])
        schedule.next_at = 0
        db.commit()
        tick(db)
        assert db.query(Delivery).count() == 1
        assert db.query(Schedule).filter_by(purged=True).one().enabled is False
    P.execute_purge(job_id)
    with db_env() as db:
        assert db.get(PurgeJob, job_id).status == 'completed'
