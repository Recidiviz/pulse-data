# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""US_MO specific entity matching tests."""
import datetime
from typing import List

from recidiviz.common.ingest_metadata import IngestMetadata
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.persistence.database.session import Session
from recidiviz.persistence.database.sqlalchemy_database_key import SQLAlchemyDatabaseKey
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity_matching import entity_matching
from recidiviz.persistence.entity_matching.entity_matching_types import MatchedEntities
from recidiviz.tests.persistence.entity_matching.state.base_state_entity_matcher_test_classes import (
    BaseStateEntityMatcherTest,
)

_EXTERNAL_ID = "EXTERNAL_ID"
_EXTERNAL_ID_2 = "EXTERNAL_ID_2"
_EXTERNAL_ID_3 = "EXTERNAL_ID_3"
_EXTERNAL_ID_WITH_SUFFIX = "EXTERNAL_ID-SEO-FSO"
_FULL_NAME = "FULL_NAME"
_ID_TYPE = "ID_TYPE"
_US_MO = "US_MO"
_DATE_1 = datetime.date(year=2019, month=1, day=1)
_DATE_2 = datetime.date(year=2019, month=2, day=1)
_DATE_3 = datetime.date(year=2019, month=3, day=1)
_DATE_4 = datetime.date(year=2019, month=4, day=1)
DEFAULT_METADATA = IngestMetadata(
    region=_US_MO,
    ingest_time=datetime.datetime(year=1000, month=1, day=1),
    database_key=SQLAlchemyDatabaseKey.canonical_for_schema(
        schema_type=SchemaType.STATE
    ),
)


class TestMoEntityMatching(BaseStateEntityMatcherTest):
    """Test class for US_MO specific entity matching logic."""

    @staticmethod
    def _match(
        session: Session, ingested_people: List[EntityPersonType]
    ) -> MatchedEntities:
        return entity_matching.match(session, _US_MO, ingested_people, DEFAULT_METADATA)
