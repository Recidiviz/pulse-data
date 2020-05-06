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
"""County-level view configuration."""

from typing import Dict, List

from recidiviz.big_query.big_query_view import BigQueryView
from recidiviz.calculator.query.county.dataset_config import VIEWS_DATASET
from recidiviz.calculator.query.county.views.bonds import bond_views
from recidiviz.calculator.query.county.views.charges import charge_views
from recidiviz.calculator.query.county.views.population import population_views
from recidiviz.calculator.query.county.views.state_aggregates import state_aggregate_views
from recidiviz.calculator.query.county.views.stitch import stitch_views
from recidiviz.calculator.query.county.views.vera import vera_views

VIEWS_TO_UPDATE: Dict[str, List[BigQueryView]] = {
    VIEWS_DATASET: (
        state_aggregate_views.STATE_AGGREGATE_VIEWS +
        vera_views.VERA_VIEWS +
        bond_views.BOND_VIEWS +
        charge_views.CHARGE_VIEWS +
        population_views.POPULATION_VIEWS +
        stitch_views.STITCH_VIEWS
    )
}
