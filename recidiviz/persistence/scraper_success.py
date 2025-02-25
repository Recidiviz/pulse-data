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

from recidiviz.common.ingest_metadata import SystemLevel
from recidiviz.common.jid import validate_jid
from recidiviz.ingest.models.scraper_success import ScraperSuccess as \
    ScraperSuccessModel
from recidiviz.persistence.database.schema.county.schema import \
    ScraperSuccess as ScraperSuccessEntry
from recidiviz.persistence.database.schema_utils import \
    schema_base_for_system_level
from recidiviz.persistence.database.session_factory import SessionFactory


def store_scraper_success(scraper_success: ScraperSuccessModel,
                          jurisdiction_id: str):
    """Store a scraper success event"""

    jurisdiction_id = validate_jid(jurisdiction_id)

    ss = ScraperSuccessEntry(
        jid=jurisdiction_id,
        date=scraper_success.date,
    )

    session = SessionFactory.for_schema_base(
        schema_base_for_system_level(SystemLevel.COUNTY))
    session.add(ss)
    session.commit()

    return True
