"""Manages alembic and sqlalchemy environments."""

import os

# Hackity hack to get around the fact that alembic runs this file as a top-level
# module rather than a child of the recidiviz module
import sys
module_path = os.path.abspath(__file__)
# Walk up directories to reach main package
while not module_path.split('/')[-1] == 'recidiviz':
    if module_path == '/':
        raise RuntimeError('Top-level recidiviz package not found')
    module_path = os.path.dirname(module_path)
# Must insert parent directory of main package
sys.path.insert(0, os.path.dirname(module_path))

from logging.config import fileConfig

from sqlalchemy import create_engine

from alembic import context
from recidiviz.persistence.database.schema import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
fileConfig(config.config_file_name)

# Metadata from schema
target_metadata = Base.metadata

# String defining database implementation used by SQLAlchemy engine
_DB_TYPE = 'postgresql'


def get_sqlalchemy_url():
    """Returns string needed to connect to database"""

    # Boolean int (0 or 1) indicating whether to use SSL to connect to the
    # database
    use_ssl = int(os.getenv('SQLALCHEMY_USE_SSL'))

    if use_ssl == 1:
        return _get_sqlalchemy_url_with_ssl()
    elif use_ssl == 0:
        return _get_sqlalchemy_url_without_ssl()
    else:
        raise RuntimeError('Invalid value for use_ssl: {use_ssl}'.format(
            use_ssl=use_ssl))


def run_migrations_offline():
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = get_sqlalchemy_url()
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        compare_type=True)

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online():
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = create_engine(get_sqlalchemy_url())

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True)

        with context.begin_transaction():
            context.run_migrations()


def _get_sqlalchemy_url_without_ssl():
    """Returns string used for SQLAlchemy engine, without SSL params"""

    user = os.getenv('SQLALCHEMY_DB_USER')
    password = os.getenv('SQLALCHEMY_DB_PASSWORD')
    host = os.getenv('SQLALCHEMY_DB_HOST')
    db_name = os.getenv('SQLALCHEMY_DB_NAME')

    return '{db_type}://{user}:{password}@{host}/{db_name}'.format(
        db_type=_DB_TYPE,
        user=user,
        password=password,
        host=host,
        db_name=db_name)


def _get_sqlalchemy_url_with_ssl():
    """Returns string used for SQLAlchemy engine, with SSL params"""

    ssl_key_path = os.getenv('SQLALCHEMY_SSL_KEY_PATH')
    ssl_cert_path = os.getenv('SQLALCHEMY_SSL_CERT_PATH')

    ssl_params = '?sslkey={ssl_key_path}&sslcert={ssl_cert_path}'.format(
        ssl_key_path=ssl_key_path,
        ssl_cert_path=ssl_cert_path)

    url_without_ssl = _get_sqlalchemy_url_without_ssl()

    return url_without_ssl + ssl_params


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
