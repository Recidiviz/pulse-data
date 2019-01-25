# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
# =============================================================================

"""Top-level recidiviz package."""
from typing import Optional

import sqlalchemy
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from recidiviz.persistence.database.schema import Base
from recidiviz.utils import environment
from recidiviz.utils import secrets

Session = sessionmaker()
db_engine: Optional[Engine]

if environment.in_prod():
    db_user = secrets.get_secret('sqlalchemy_db_user')
    db_password = secrets.get_secret('sqlalchemy_db_password')
    db_name = secrets.get_secret('sqlalchemy_db_name')
    cloudsql_instance_id = secrets.get_secret('cloudsql_instance_id')

    sqlalchemy_url = \
        ('postgresql://{db_user}:{db_password}@/{db_name}'
         '?host=/cloudsql/{cloudsql_instance_id}') \
            .format(db_user=db_user, db_password=db_password,
                    db_name=db_name, cloudsql_instance_id=cloudsql_instance_id)
    db_engine = sqlalchemy.create_engine(sqlalchemy_url)
    Base.metadata.create_all(db_engine)
    Session.configure(bind=db_engine)
