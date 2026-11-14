from sqlalchemy import String, Double, ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column
from src.config.database import Base


class JobAttempt(Base):
    """One execution attempt. Kept after the job finishes as a review trail."""
    __tablename__ = 'job_attempts'
    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey('jobs.id'), index=True)
    worker: Mapped[str] = mapped_column(String(120))
    fence: Mapped[int]
    state: Mapped[str] = mapped_column(String(20), default='running')
    claimed_at: Mapped[float] = mapped_column(Double)
    heartbeat_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    finished_at: Mapped[float | None] = mapped_column(Double, nullable=True)
    renew_count: Mapped[int] = mapped_column(default=0)
    detail: Mapped[dict] = mapped_column(JSON, default=dict)


class JobEvent(Base):
    """Execution trail shown on the results page: claim, takeover, stale write."""
    __tablename__ = 'job_events'
    id: Mapped[int] = mapped_column(primary_key=True)
    job_id: Mapped[int] = mapped_column(ForeignKey('jobs.id'), index=True)
    actor: Mapped[str] = mapped_column(String(120))
    action: Mapped[str] = mapped_column(String(40))
    fence: Mapped[int] = mapped_column(default=0)
    at: Mapped[float] = mapped_column(Double)
    detail: Mapped[dict] = mapped_column(JSON, default=dict)
