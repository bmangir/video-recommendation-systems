import psycopg2
from psycopg2.extensions import connection as PGConnection

class PostgresConnector:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(PostgresConnector, cls).__new__(cls)
        return cls._instance

    def __init__(self, PG_DB_NAME, PG_USER, PG_PW, PG_PORT):
        self.db_name = PG_DB_NAME
        self.pg_user = PG_USER
        self.pg_pw = PG_PW
        self.pg_port = PG_PORT
        if not hasattr(self, "connection"):
            self.connection = None

    def create_connection(self) -> PGConnection:
        if self.connection is not None:
            return self.connection

        try:
            self.connection = psycopg2.connect(database=self.db_name,
                                               user=self.pg_user,
                                               password=self.pg_pw,
                                               host='127.0.0.1',
                                               port=self.pg_port)

            self.connection.autocommit = True
        except psycopg2.Error as error:
            print("Failed to connection ", error)

        return self.connection

    def close_connection(self):
        if self.connection is not None:
            self.connection.close()
            print("PostgresSQL connection closed")
            self.connection = None
