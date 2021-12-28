import os
from sqlalchemy.orm import sessionmaker
from sqlalchemy import exc, create_engine, event

engine = create_engine(
    f"sqlite:///tasks.db",
    encoding="utf-8",
    echo=False,
    # pool_size=10, max_overflow=20,
    pool_pre_ping=True
)


@event.listens_for(engine, "connect")
def connect(dbapi_connection, connection_record):
    connection_record.info['pid'] = os.getpid()


@event.listens_for(engine, "checkout")
def checkout(dbapi_connection, connection_record, connection_proxy):
    pid = os.getpid()
    if connection_record.info['pid'] != pid:
        connection_record.connection = connection_proxy.connection = None
        raise exc.DisconnectionError(
            "Connection record belongs to pid %s, "
            "attempting to check out in pid %s" %
            (connection_record.info['pid'], pid)
        )


Session = sessionmaker(bind=engine)
