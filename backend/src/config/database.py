from sqlalchemy import create_engine, event
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from .settings import DATABASE_URL

class Base(DeclarativeBase):
    pass

_is_sqlite = DATABASE_URL.startswith('sqlite')
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_recycle=1800,
    connect_args={'timeout': 30, 'check_same_thread': False} if _is_sqlite else {},
)

if _is_sqlite:
    @event.listens_for(engine, 'connect')
    def _sqlite_pragmas(conn, _record):
        # Several workers may claim jobs at once; WAL plus a busy timeout lets
        # the conditional claim UPDATE serialise instead of failing outright.
        cur = conn.cursor()
        cur.execute('PRAGMA journal_mode=WAL')
        cur.execute('PRAGMA busy_timeout=30000')
        cur.close()

Session = sessionmaker(engine, expire_on_commit=False)

def get_db():
    with Session() as db:
        yield db
