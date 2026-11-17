"""Shared fixtures for the purge tests.

The tests run against SQLite so they need no containers. They are executed from
the backend directory (``PYTHONPATH=. python -m pytest tests``).
"""
import os
import pathlib
import sys
import tempfile

os.environ.setdefault('DATABASE_URL', 'sqlite:///' + tempfile.mkstemp(suffix='.db')[1])
os.environ.setdefault('JWT_SECRET', 'test-secret')
os.environ.setdefault('REDIS_URL', 'redis://localhost:6379/15')
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

# settings.py hardcodes /app/project.json (the container layout). Serve the
# repository copy instead so the tests run outside Docker.
_original_read_text = pathlib.Path.read_text
_PROJECT_JSON = pathlib.Path(__file__).resolve().parents[1] / 'project.json'


def _read_text(self, *args, **kwargs):
    if str(self) == '/app/project.json':
        return _original_read_text(_PROJECT_JSON, *args, **kwargs)
    return _original_read_text(self, *args, **kwargs)


pathlib.Path.read_text = _read_text

import pytest  # noqa: E402

import src.services.report as report_service  # noqa: E402
from src.config.database import Base, engine, Session  # noqa: E402
from src.models.identity import User  # noqa: E402
from src.models.source import Policy, Source  # noqa: E402
from src.schemas.requests import RunInput, ScheduleInput  # noqa: E402
from src.services.calculation import calculate  # noqa: E402
from src.services.delivery import add_schedule  # noqa: E402
from src.services.report import submit  # noqa: E402
from src.utils.security import password_hash  # noqa: E402


class FakeRedis:
    def rpush(self, *args, **kwargs):
        return 1

    def lpop(self, *args, **kwargs):
        return None


report_service.redis.Redis.from_url = lambda *args, **kwargs: FakeRedis()

POLICY = {'version': 'v1', 'numerator': 'first_pass', 'denominator': 'input', 'description': '测试口径'}


class Actor:
    def __init__(self, email='admin@demo.local', role='admin'):
        self.email = email
        self.role = role


def unique_draft(day='2026-11-01', first_pass=50):
    """A valid draft row whose content is unique for the given first_pass."""
    return {
        'period': day, 'department': '试制线A', 'shift': '早班',
        'input': 100, 'first_pass': first_pass,
        'reworked_good': 90 - first_pass, 'scrap': 10,
    }


@pytest.fixture()
def db_env():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    with Session() as db:
        for role in ('admin', 'analyst', 'viewer'):
            db.add(User(email=role + '@demo.local', role=role, password=password_hash('Demo2026!')))
        db.add(Policy(version=POLICY['version'], data=POLICY))
        db.commit()
    return Session


def add_source(db, key, data):
    obj = Source(natural_key=key, data=data)
    db.add(obj)
    db.commit()
    return obj


def new_run(db, name, period='全部'):
    run = submit(db, Actor(), RunInput(name=name, policy_id=1, period=period))
    return run


def completed_run(db, name, period='全部'):
    run = new_run(db, name, period)
    run.result = calculate(run.snapshot['sources'], run.snapshot['policy'])
    run.status = 'completed'
    run.approved = True
    db.commit()
    return run


def schedule_for(db, run, interval=60):
    obj = add_schedule(db, Actor(), ScheduleInput(name='计划-' + str(run.id), run_id=run.id, interval_seconds=interval))
    obj.next_at = 0
    db.commit()
    return obj
