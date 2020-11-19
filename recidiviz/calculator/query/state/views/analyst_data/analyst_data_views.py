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
"""All views needed for analyst data"""
from typing import List
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.calculator.query.state.views.analyst_data.compartment_sessions import COMPARTMENT_SESSIONS_VIEW_BUILDER
from recidiviz.calculator.query.state.views.analyst_data.compartment_sentences import COMPARTMENT_SENTENCES_VIEW_BUILDER

ANALYST_DATA_VIEW_BUILDERS: List[SimpleBigQueryViewBuilder] = [
    COMPARTMENT_SESSIONS_VIEW_BUILDER,
    COMPARTMENT_SENTENCES_VIEW_BUILDER
]
