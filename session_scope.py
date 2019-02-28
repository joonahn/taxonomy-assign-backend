from contextlib import contextmanager

@contextmanager
def session_scope(session_maker, expunge=False):
    """Provide a transactional scope around a series of operations."""
    session = session_maker()
    try:
        yield session
        if expunge:
            session.expunge_all()
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()
