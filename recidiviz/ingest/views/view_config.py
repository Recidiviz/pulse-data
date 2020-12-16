# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Ingest metadata view configuration."""

from typing import Dict, Sequence

from recidiviz.big_query.big_query_view import BigQueryViewBuilder
from recidiviz.ingest.views.dataset_config import VIEWS_DATASET
from recidiviz.ingest.views.enum_counter import StateTableEnumCounterBigQueryViewCollector
from recidiviz.ingest.views.non_enum_counter import StateTableNonEnumCounterBigQueryViewCollector
from recidiviz.ingest.views.state_person_counter import StatePersonBigQueryViewCollector


INGEST_METADATA_BUILDERS = StateTableEnumCounterBigQueryViewCollector().collect_view_builders() + \
    StateTableNonEnumCounterBigQueryViewCollector().collect_view_builders() + \
    StatePersonBigQueryViewCollector().collect_view_builders()

VIEW_BUILDERS_FOR_VIEWS_TO_UPDATE: Dict[str, Sequence[BigQueryViewBuilder]] = {
    VIEWS_DATASET: INGEST_METADATA_BUILDERS,
}
