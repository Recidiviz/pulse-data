# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Delegate class to ETL US_IX Moves Management records into Firestore."""

from recidiviz.calculator.query.state.views.workflows.firestore.us_ix_moves_management_record import (
    US_IX_MOVES_MANAGEMENT_RECORD_VIEW_NAME,
)
from recidiviz.workflows.etl.workflows_person_external_id_keyed_etl_delegate import (
    WorkflowsPersonExternalIdKeyedETLDelegate,
)

US_IX_MOVES_MANAGEMENT_RECORD_FILENAME = (
    f"{US_IX_MOVES_MANAGEMENT_RECORD_VIEW_NAME}.json"
)

US_IX_MOVES_MANAGEMENT_RECORDS_COLLECTION_NAME = "usIxMovesManagementRecords"


class WorkflowsUsIxMovesManagementETLDelegate(
    WorkflowsPersonExternalIdKeyedETLDelegate
):
    """Delegate class to ETL the us_ix_moves_management_record.json file into
    Firestore, one document per resident, under the same document id as that
    resident's document in the residents collection."""

    COLLECTION_BY_FILENAME = {
        US_IX_MOVES_MANAGEMENT_RECORD_FILENAME: US_IX_MOVES_MANAGEMENT_RECORDS_COLLECTION_NAME
    }

    def get_supported_files(self) -> list[str]:
        return [US_IX_MOVES_MANAGEMENT_RECORD_FILENAME]
