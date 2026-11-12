# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""Classes defining delegate interface and implementations for ETLing demo data into Firestore."""
import asyncio
import logging
from pathlib import Path
from typing import IO, Iterator

from recidiviz.common.constants.states import StateCode
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.workflows_client_etl_delegate import (
    WorkflowsClientETLDelegate,
)
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate
from recidiviz.workflows.etl.workflows_opportunity_etl_delegate import (
    WorkflowsOpportunityETLDelegate,
)


async def load_demo_fixture(
    delegate_cls: type[WorkflowsFirestoreETLDelegate], state_code: str, filename: str
) -> None:
    """Uses the ETL logic in the delegate, but substitutes a local fixture file as the data source
    and a different Firestore collection as the destination."""

    # seems to be an unresolved mypy issue causing this error: https://github.com/python/mypy/issues/5865
    class DemoDelegate(delegate_cls):  # type: ignore
        """Abstract class containing the ETL logic for exporting a specified demo data fixture to Firestore."""

        def __init__(self, state_code: StateCode, filename: str):
            super().__init__(state_code, filename)
            self.collection_name: str = "DEMO_" + self.collection_name

        def fixture_filepath(self) -> Path:
            return Path(__file__).parent / "demo_fixtures" / self.filename

        def filepath_url(self) -> str:
            return self.fixture_filepath().as_uri()

        def get_file_stream(self) -> Iterator[IO]:
            """Returns a stream of the contents of the file this delegate is watching for."""
            # bypassing self.get_filepath since it assumes a GCS file
            with open(self.fixture_filepath(), "r", encoding="utf8") as f:
                yield f

    delegate = DemoDelegate(StateCode(state_code), filename)
    await delegate.run_etl()


async def load_all_demo_data() -> None:
    await load_demo_fixture(WorkflowsClientETLDelegate, "US_TN", "client_record.json")
    await load_demo_fixture(
        WorkflowsOpportunityETLDelegate,
        "US_TN",
        "us_tn_supervision_level_downgrade_record.json",
    )
    await load_demo_fixture(
        WorkflowsOpportunityETLDelegate,
        "US_TN",
        "us_tn_full_term_supervision_discharge_record.json",
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        asyncio.run(load_all_demo_data())
