from __future__ import with_statement
from alembic import context
from sqlalchemy import create_engine
from logging.config import fileConfig

from recidiviz.persistence.database.schema import Base
from recidiviz.utils import secrets

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

    use_ssl = secrets.get_secret('sqlalchemy_use_ssl')

    if use_ssl == True:
        return _get_sqlalchemy_url_with_ssl()
    elif use_ssl == False:
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
        url=url, target_metadata=target_metadata, literal_binds=True)

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
            target_metadata=target_metadata
        )

        with context.begin_transaction():
            context.run_migrations()


def _get_sqlalchemy_url_without_ssl():
    """Returns string used for SQLAlchemy engine, without SSL params"""

    user = secrets.get_secret('sqlalchemy_db_user')
    password = secrets.get_secret('sqlalchemy_db_password')
    host = secrets.get_secret('sqlalchemy_db_host')
    db_name = secrets.get_secret('sqlalchemy_db_name')

    return '{db_type}://{user}:{password}@{host}/{db_name}'.format(
        db_type=_DB_TYPE,
        user=user,
        password=password,
        host=host,
        db_name=db_name)


def _get_sqlalchemy_url_with_ssl():
    """Returns string used for SQLAlchemy engine, with SSL params"""

    ssl_ca_path = secrets.get_secret('sqlalchemy_ssl_ca_path')
    ssl_key_path = secrets.get_secret('sqlalchemy_ssl_key_path')
    ssl_cert_path = secrets.get_secret('sqlalchemy_ssl_cert_path')

    ssl_params = '?ssl_ca={ssl_ca_path}&ssl_key={ssl_key_path}' \
                 '&ssl_cert={ssl_cert_path}'.format(
                     ssl_ca_path=ssl_ca_path,
                     ssl_key_path=ssl_key_path,
                     ssl_cert_path=ssl_cert_path)

    url_without_ssl = _get_sqlalchemy_url_without_ssl()

    return url_without_ssl + ssl_params


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
