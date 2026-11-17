"""清退 API：权限、410 封锁、列表隐藏、重复提交与未完成时的提交拦截。"""
import pytest
from fastapi.testclient import TestClient

from src.config.database import Session
from src.models.run import Delivery, Run, Schedule
from src.models.source import Source
from src.services.calculation import calculate
from src.services.delivery import tick
from conftest import Actor, unique_draft


@pytest.fixture()
def client(db_env):
    from src.main import app
    with TestClient(app) as c:
        yield c


def login(client, email):
    r = client.post('/auth/login', json={'email': email, 'password': 'Demo2026!'})
    assert r.status_code == 200, r.text
    return {'Authorization': 'Bearer ' + r.json()['token']}


def setup_report(client):
    """Two reports: one covers all drafts, one only the second period."""
    from src.config.database import Session as SessionMaker
    with SessionMaker() as db:
        db.add(Source(natural_key='a', data=unique_draft('p1', 50)))
        db.add(Source(natural_key='b', data=unique_draft('p2', 70)))
        db.commit()
    analyst = login(client, 'analyst@demo.local')
    all_report = client.post('/runs', json={'name': '全部报告', 'policy_id': 1, 'period': '全部'}, headers=analyst)
    second = client.post('/runs', json={'name': '二期报告', 'policy_id': 1, 'period': 'p2'}, headers=analyst)
    assert all_report.status_code == 200, all_report.text
    assert second.status_code == 200, second.text
    with SessionMaker() as db:
        for rid in (all_report.json()['id'], second.json()['id']):
            run = db.get(Run, rid)
            run.result = calculate(run.snapshot['sources'], run.snapshot['policy'])
            run.status = 'completed'
            db.commit()
    return all_report.json()['id'], second.json()['id']


def test_purge_end_to_end_blocks_old_entries_and_keeps_unrelated(client):
    admin = login(client, 'admin@demo.local')
    viewer = login(client, 'viewer@demo.local')
    analyst = login(client, 'analyst@demo.local')
    all_id, second_id = setup_report(client)
    assert client.post(f'/runs/{all_id}/approve', json={'note': '已核对口径'}, headers=admin).status_code == 200
    assert client.post(f'/runs/{second_id}/approve', json={'note': '已核对口径'}, headers=admin).status_code == 200
    assert client.post('/schedules', json={'name': '计划一', 'run_id': all_id, 'interval_seconds': 60}, headers=admin).status_code == 200
    with Session() as db:
        schedule = db.query(Schedule).first()
        schedule.next_at = 0
        db.commit()
        tick(db)
        delivery_id = db.query(Delivery).first().id
        source_id = db.query(Source).filter(Source.natural_key == 'a').first().id

    # only admin may purge
    assert client.post('/purges', json={'source_ids': [source_id]}, headers=viewer).status_code == 403
    assert client.post('/purges', json={'source_ids': [source_id]}, headers=analyst).status_code == 403

    r = client.post('/purges', json={'source_ids': [source_id], 'note': '误采保密底稿'}, headers=admin)
    assert r.status_code == 200, r.text
    job = r.json()
    assert job['status'] == 'completed', job
    assert job['step_label'] == '核对副本与旧入口'
    assert job['counts']['runs'] == 1

    # affected reports and deliveries refuse both reads and downloads
    assert client.get(f'/runs/{all_id}', headers=viewer).status_code == 410
    assert client.get(f'/runs/{all_id}/csv', headers=viewer).status_code == 410
    assert client.get(f'/runs/{all_id}/csv', headers=admin).status_code == 410
    assert client.get(f'/deliveries/{delivery_id}/csv', headers=viewer).status_code == 410
    assert client.post(f'/runs/{all_id}/approve', json={'note': '再次复核'}, headers=admin).status_code == 410
    assert client.post(f'/schedules/{job["scope"]["schedule_ids"][0]}/toggle', json={}, headers=admin).status_code == 410
    # hidden from lists
    assert all(x['id'] != all_id for x in client.get('/runs', headers=viewer).json())
    assert all(x['id'] != delivery_id for x in client.get('/deliveries', headers=viewer).json())

    # unrelated report and its download still work
    assert client.get(f'/runs/{second_id}', headers=viewer).status_code == 200
    assert client.get(f'/runs/{second_id}/csv', headers=viewer).status_code == 200
    # unrelated draft kept
    assert any(x['natural_key'] == 'b' for x in client.get('/sources', headers=viewer).json())
    assert all(x['natural_key'] != 'a' for x in client.get('/sources', headers=viewer).json())

    # page-facing status shows the timeline and the failure-reason field
    listing = client.get('/purges', headers=viewer).json()
    assert listing[0]['status_label'] == '已完成'
    assert 'error' in listing[0]

    # repeating the same ids returns the same job and does not widen
    again = client.post('/purges', json={'source_ids': [source_id]}, headers=admin).json()
    assert again['id'] == job['id']
    assert client.get(f'/runs/{second_id}', headers=viewer).status_code == 200

    # validation
    assert client.post('/purges', json={'source_ids': [99999]}, headers=admin).status_code == 404
    assert client.post('/purges', json={'source_ids': []}, headers=admin).status_code == 422


def test_submission_refused_while_purge_still_pending(client):
    admin = login(client, 'admin@demo.local')
    analyst = login(client, 'analyst@demo.local')
    with Session() as db:
        db.add(Source(natural_key='a', data=unique_draft('p1', 50)))
        db.add(Source(natural_key='b', data=unique_draft('p2', 70)))
        db.commit()
        source_id = db.query(Source).filter(Source.natural_key == 'a').first().id
        from src.services.purge import request_purge
        job = request_purge(db, Actor(), [source_id])
        job_id = job.id
    # the draft row still exists because cleanup has not run, but it is claimed
    assert any(x['id'] == source_id for x in client.get('/sources', headers=analyst).json())
    assert client.post('/runs', json={'name': '并发补跑', 'policy_id': 1, 'period': 'p1'}, headers=analyst).status_code == 409
    assert client.post('/runs', json={'name': '全量补跑', 'policy_id': 1, 'period': '全部'}, headers=analyst).status_code == 409
    item = client.get(f'/purges/{job_id}', headers=admin).json()
    assert item['status'] == 'pending'
    assert item['step_label'] == '已封锁读取与下载'


def test_retry_endpoint_completes_failed_job(client):
    admin = login(client, 'admin@demo.local')
    with Session() as db:
        db.add(Source(natural_key='a', data=unique_draft('p1', 50)))
        db.commit()
        source_id = db.query(Source).first().id
        from src.services.purge import request_purge
        job = request_purge(db, Actor(), [source_id])
        job_id = job.id
        job.status = 'failed'
        job.error = '清除交付副本：磁盘写入失败'
        db.commit()
    r = client.post(f'/purges/{job_id}/retry', json={}, headers=admin)
    assert r.status_code == 200, r.text
    assert r.json()['status'] == 'completed'
    assert client.get(f'/purges/{job_id}', headers=admin).json()['error'] is None
