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
import logging
from pathlib import Path
from typing import Dict, Iterator, TextIO

from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.regions.us_tn.client_record_etl_delegate import (
    ClientRecordETLDelegate,
)
from recidiviz.workflows.etl.regions.us_tn.compliant_reporting_referral_record_etl_delegate import (
    CompliantReportingReferralRecordETLDelegate,
)
from recidiviz.workflows.etl.workflows_etl_delegate import WorkflowsFirestoreETLDelegate
from recidiviz.workflows.etl.workflows_staff_etl_delegate import (
    WorkflowsStaffETLDelegate,
)


def load_demo_fixture(
    delegate_cls: type[WorkflowsFirestoreETLDelegate], state_code: str, filename: str
) -> None:
    """Uses the ETL logic in the delegate, but substitutes a local fixture file as the data source
    and a different Firestore collection as the destination."""

    # seems to be an unresolved mypy issue causing this error: https://github.com/python/mypy/issues/5865
    class DemoDelegate(delegate_cls):  # type: ignore
        """Abstract class containing the ETL logic for exporting a specified demo data fixture to Firestore."""

        def fixture_filepath(self, filename: str) -> Path:
            return Path(__file__).parent / "demo_fixtures" / filename

        def filepath_url(self, _state_code: str, filename: str) -> str:
            return self.fixture_filepath(filename).as_uri()

        def get_file_stream(self, _state_code: str, filename: str) -> Iterator[TextIO]:
            """Returns a stream of the contents of the file this delegate is watching for."""
            # bypassing self.get_filepath since it assumes a GCS file
            with open(self.fixture_filepath(filename), "r", encoding="utf8") as f:
                yield f

        @property
        def COLLECTION_BY_FILENAME(self) -> Dict[str, str]:
            originalMapping = super().COLLECTION_BY_FILENAME
            return {k: f"DEMO_{v}" for k, v in originalMapping.items()}

    delegate = DemoDelegate()
    delegate.run_etl(state_code, filename)


def load_all_demo_data() -> None:
    load_demo_fixture(WorkflowsStaffETLDelegate, "US_TN", "staff_record.json")
    load_demo_fixture(ClientRecordETLDelegate, "US_TN", "client_record.json")
    load_demo_fixture(
        CompliantReportingReferralRecordETLDelegate,
        "US_TN",
        "compliant_reporting_referral_record.json",
    )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        load_all_demo_data()
