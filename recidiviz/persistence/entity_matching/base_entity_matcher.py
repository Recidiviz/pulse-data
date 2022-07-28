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
# ============================================================================

"""Contains logic to match database entities with ingested entities."""

from abc import abstractmethod
from typing import List

from opencensus.stats import aggregation, measure, view

from recidiviz.persistence.database.session import Session
from recidiviz.persistence.entity.entities import EntityPersonType
from recidiviz.persistence.entity_matching.entity_matching_types import MatchedEntities
from recidiviz.utils import monitoring

m_matching_errors = measure.MeasureInt(
    "persistence/entity_matching/error_count",
    "Number of EntityMatchingErrors thrown for a specific entity type",
    "1",
)

matching_errors_by_entity_view = view.View(
    "recidiviz/persistence/entity_matching/error_count",
    "Sum of the errors in the entity matching layer, by entity",
    [monitoring.TagKey.REGION, monitoring.TagKey.ENTITY_TYPE],
    m_matching_errors,
    aggregation.SumAggregation(),
)

monitoring.register_views([matching_errors_by_entity_view])


class BaseEntityMatcher:
    """Base class for all entity matchers."""

    @abstractmethod
    def run_match(
        self,
        session: Session,
        region_code: str,
        ingested_people: List[EntityPersonType],
    ) -> MatchedEntities:
        """
        Attempts to match all people from |ingested_people| with corresponding
        people in our database for the given |region|. Returns an
        MatchedEntities object that contains the results of matching.
        """


def increment_error(entity_name: str) -> None:
    mtags = {monitoring.TagKey.ENTITY_TYPE: entity_name}
    with monitoring.measurements(mtags) as measurements:
        measurements.measure_int_put(m_matching_errors, 1)
