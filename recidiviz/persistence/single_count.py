# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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
"""Store single count.
"""

import logging

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.jid import validate_jid
from recidiviz.ingest.models.ingest_info import to_string
from recidiviz.ingest.models.single_count import SingleCount
from recidiviz.persistence.database.schema.aggregate.schema import SingleCountAggregate
from recidiviz.persistence.database.session_factory import SessionFactory
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.persistence_utils import should_persist


def store_single_count(sc: SingleCount, jurisdiction_id: str) -> bool:
    """Store a single count"""

    jurisdiction_id = validate_jid(jurisdiction_id)

    sca = SingleCountAggregate(
        jid=jurisdiction_id,
        ethnicity=sc.ethnicity.value if sc.ethnicity else None,
        gender=sc.gender.value if sc.gender else None,
        race=sc.race.value if sc.race else None,
        count=sc.count,
        date=sc.date,
    )

    logging.info("Writing single count to the database: %s", to_string(sc))
    if not should_persist():
        return True

    session = SessionFactory.for_database(
        SQLAlchemyDatabaseKey.for_schema(SystemLevel.COUNTY.schema_type())
    )
    session.add(sca)
    session.commit()

    return True
