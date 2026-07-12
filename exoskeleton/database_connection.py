"""
Database connection management for the exoskeleton framework.
~~~~~~~~~~~~~~~~~~~~~
Source: https://github.com/RuedigerVoigt/exoskeleton
(c) 2019-2025 Rüdiger Voigt and contributors:
Released under the Apache License 2.0
"""
# standard library:
from contextlib import contextmanager
import logging
from typing import cast, Iterator

# external dependencies:
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, URL
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import OperationalError, DatabaseError
import userprovided

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Database connection management for the exoskeleton framework using SQLAlchemy."""

    def __init__(self,
                 database_settings: dict) -> None:
        """
        Initialize database connection using SQLAlchemy.

        Args:
            database_settings: Dictionary containing database credentials
                Required keys: 'database', 'username'
                Optional keys: 'host', 'port', 'passphrase'
        """
        if database_settings is None:
            raise ValueError('You must supply database credentials.')

        # Are the parameters valid?
        userprovided.parameters.validate_dict_keys(
            dict_to_check=database_settings,
            allowed_keys={'host', 'port', 'database', 'username', 'passphrase'},
            necessary_keys={'database', 'username'},
            dict_name='database_settings')

        # Necessary settings (existence ensured, but must not be None)
        self.db_name: str = database_settings['database']
        if not self.db_name:
            raise ValueError('You must provide the name of the database.')
        self.db_username: str = database_settings['username']
        if not self.db_username:
            raise ValueError('You must provide a database user.')

        # Check settings and fallback to default if necessary:
        self.db_host: str = cast(str, database_settings.get('host', None))
        if not self.db_host:
            logger.warning('No hostname provided. Will try localhost.')
            self.db_host = 'localhost'

        self.db_port: int = cast(int, database_settings.get('port', None))
        if not self.db_port:
            logger.info('No port number supplied: will try port 3306.')
            self.db_port = 3306
        elif not userprovided.parameters.is_port(self.db_port):
            raise ValueError('Database port outside valid range!')

        self.db_passphrase: str = database_settings.get('passphrase', '')
        if self.db_passphrase == '':
            logger.warning(
                'No database passphrase provided. Trying to connect without.')

        # Establish the database connection using SQLAlchemy
        self.engine: Engine | None = None
        self.Session: sessionmaker | None = None
        self.establish_db_connection()

    def __del__(self) -> None:
        """Cleanup: dispose the engine and its connection pool."""
        try:
            if self.engine:
                self.engine.dispose()
        except Exception:  # pylint: disable=broad-except
            pass

    def establish_db_connection(self) -> None:
        """Establish a connection to MariaDB using SQLAlchemy."""
        try:
            logger.debug('Trying to connect to database.')

            # Build connection URL from components so credentials with
            # reserved characters (@ : / ? #) are escaped correctly.
            # Using pymysql as the driver for MariaDB
            connection_url = URL.create(
                "mysql+pymysql",
                username=self.db_username,
                password=self.db_passphrase,
                host=self.db_host,
                port=self.db_port,
                database=self.db_name,
                query={"charset": "utf8mb4"},
            )

            # Create engine with connection pooling
            self.engine = create_engine(
                connection_url,
                pool_pre_ping=True,  # Verify connections before using
                pool_recycle=3600,   # Recycle connections after 1 hour
                echo=False,          # Set to True for SQL debugging
            )

            # Test the connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            # Create session factory
            self.Session = sessionmaker(bind=self.engine, autoflush=True, autocommit=False)

            logger.info('Successfully established database connection.')

        except OperationalError:
            logger.exception(
                "Cannot connect to DBMS. Did you forget a parameter?",
                exc_info=True)
            raise
        except DatabaseError:
            logger.exception('Database related exception', exc_info=True)
            raise
        except Exception:
            logger.exception(
                'Exception while connecting to the DBMS.', exc_info=True)
            raise

    def get_session(self) -> Session:
        """
        Create and return a *new* SQLAlchemy session.

        The caller is responsible for closing it. For transactional
        work prefer session_scope(), which commits, rolls back and
        closes automatically.
        """
        if self.Session is None:
            logger.info("No session factory. Trying to (re)connect...")
            self.establish_db_connection()
        assert self.Session is not None, "Database connection not established"
        return cast(Session, self.Session())

    @contextmanager
    def session_scope(self) -> Iterator[Session]:
        """
        Provide a transactional scope for a series of operations:
        commit on success, roll back on exception, always close.

        Usage:
            with db_connection.session_scope() as session:
                session.add(...)
        """
        session = self.get_session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
