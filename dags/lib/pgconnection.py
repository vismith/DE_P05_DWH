from contextlib import contextmanager
from psycopg import connect
class PGConnection:
    def __init__(self, uri: str) -> None:
        self.uri = uri


    def client(self):
        return connect(self.uri)

    @contextmanager
    def connection(self):
        conn = connect(self.uri)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()
