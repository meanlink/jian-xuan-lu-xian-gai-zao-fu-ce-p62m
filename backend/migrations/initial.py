from sqlalchemy import inspect, text
from src.config.database import Base, engine
from src import models


def _add_missing_columns():
    """Small additive migration so existing deployments keep their jobs table.

    Interrupted jobs from older workers have no lease; they are treated as
    claimable, so pending work continues after the upgrade. Completed and
    approved rows are never rewritten here.
    """
    inspector = inspect(engine)
    if 'jobs' not in inspector.get_table_names():
        return
    existing = {c['name'] for c in inspector.get_columns('jobs')}
    additions = {
        'lease_owner': 'VARCHAR(120) NULL',
        'lease_token': 'INTEGER NOT NULL DEFAULT 0',
        'lease_expires_at': 'DOUBLE NULL',
        'claimed_at': 'DOUBLE NULL',
        'heartbeat_at': 'DOUBLE NULL',
        'attempt_count': 'INTEGER NOT NULL DEFAULT 0',
    }
    with engine.begin() as conn:
        for name, ddl in additions.items():
            if name not in existing:
                conn.execute(text(f'ALTER TABLE jobs ADD COLUMN {name} {ddl}'))
        # Older workers left interrupted jobs as "running" with no lease. Give
        # them an owner-less expired lease so any worker may adopt them.
        conn.execute(text(
            "UPDATE jobs SET lease_token = COALESCE(lease_token, 0), "
            "attempt_count = COALESCE(attempt_count, 0) "
            "WHERE lease_token IS NULL OR attempt_count IS NULL"
        ))


def migrate():
    _add_missing_columns()
    Base.metadata.create_all(engine)


if __name__ == '__main__':
    migrate()
