#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  ============================================================================
"""Querier class to encapsualte requests to the Workflows postgres DBs."""
from functools import cached_property
from typing import List

import attr
from sqlalchemy.orm import sessionmaker

from recidiviz.calculator.query.state.views.outliers.workflows_enabled_states import (
    get_workflows_enabled_states,
)
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.database.database_managers.state_segmented_database_manager import (
    StateSegmentedDatabaseManager,
)
from recidiviz.persistence.database.schema.workflows.schema import Opportunity
from recidiviz.persistence.database.schema_type import SchemaType
from recidiviz.workflows.types import OpportunityInfo


@attr.s(auto_attribs=True)
class WorkflowsQuerier:
    """Implements Querier abstractions for Workflows data sources"""

    state_code: StateCode = attr.ib()
    database_manager: StateSegmentedDatabaseManager = attr.ib(
        factory=lambda: StateSegmentedDatabaseManager(  # type: ignore
            get_workflows_enabled_states(), SchemaType.WORKFLOWS
        )
    )

    @cached_property
    def database_session(self) -> sessionmaker:
        return self.database_manager.get_session(self.state_code)

    def get_opportunities(self) -> List[OpportunityInfo]:
        with self.database_session() as session:
            opportunities = session.query(Opportunity).with_entities(
                Opportunity.state_code,
                Opportunity.opportunity_type,
                Opportunity.gating_feature_variant,
            )

            return [OpportunityInfo(**opportunity) for opportunity in opportunities]
