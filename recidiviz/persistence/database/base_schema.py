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
"""Defines base classes for each of the database schemas."""
from typing import Union

from recidiviz.persistence.database.schema.case_triage.schema import CaseTriageBase
from recidiviz.persistence.database.schema.insights.schema import InsightsBase
from recidiviz.persistence.database.schema.justice_counts.schema import (
    JusticeCountsBase,
)
from recidiviz.persistence.database.schema.operations.schema import OperationsBase
from recidiviz.persistence.database.schema.pathways.schema import PathwaysBase
from recidiviz.persistence.database.schema.state.schema import StateBase
from recidiviz.persistence.database.schema.workflows.schema import WorkflowsBase

SQLAlchemyModelType = Union[
    StateBase,
    OperationsBase,
    JusticeCountsBase,
    CaseTriageBase,
    PathwaysBase,
    WorkflowsBase,
    InsightsBase,
]
