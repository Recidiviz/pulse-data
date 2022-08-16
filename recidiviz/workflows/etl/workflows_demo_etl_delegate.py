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
from typing import Iterator, TextIO

from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.workflows.etl.regions.us_tn.client_record_etl_delegate import (
    ClientRecordETLDelegate,
)
from recidiviz.workflows.etl.regions.us_tn.compliant_reporting_referral_record_etl_delegate import (
    CompliantReportingReferralRecordETLDelegate,
)
from recidiviz.workflows.etl.regions.us_tn.staff_record_etl_delegate import (
    StaffRecordETLDelegate,
)
from recidiviz.workflows.etl.workflows_etl_delegate import (
    WorkflowsSingleStateETLDelegate,
)


class WorkflowsFirestoreDemoETLDelegate(WorkflowsSingleStateETLDelegate):
    """Abstract class containing the ETL logic for exporting a specified demo data fixture to Firestore."""

    @property
    def fixture_filepath(self) -> Path:
        return Path(__file__).parent / "demo_fixtures" / self.EXPORT_FILENAME

    def filepath_url(self, _state_code: str, _filename: str) -> str:
        return self.fixture_filepath.as_uri()

    def get_file_stream(self, _state_code: str, _filename: str) -> Iterator[TextIO]:
        """Returns a stream of the contents of the file this delegate is watching for."""
        # bypassing self.get_filepath since it assumes a GCS file
        with open(self.fixture_filepath, "r", encoding="utf8") as f:
            yield f

    @property
    def COLLECTION_BY_FILENAME(self) -> dict[str, str]:
        """Name of the Firestore collection this delegate will ETL into."""
        return {self.EXPORT_FILENAME: f"DEMO_{self._COLLECTION_NAME_BASE}"}


class StaffRecordDemoETLDelegate(
    WorkflowsFirestoreDemoETLDelegate, StaffRecordETLDelegate
):
    """Delegate class to ETL demo_fixtures/staff_record.json into Firestore."""


class ClientRecordDemoETLDelegate(
    WorkflowsFirestoreDemoETLDelegate, ClientRecordETLDelegate
):
    """Delegate class to ETL demo_fixtures/client_record.json into Firestore."""


class CompliantReportingReferralRecordDemoETLDelegate(
    WorkflowsFirestoreDemoETLDelegate, CompliantReportingReferralRecordETLDelegate
):
    """Delegate class to ETL demo_fixtures/compliant_reporting_referral_record.json into Firestore."""


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        StaffRecordDemoETLDelegate().run_etl("US_TN", "staff_record.json")
        ClientRecordDemoETLDelegate().run_etl("US_TN", "client_record.json")
        CompliantReportingReferralRecordDemoETLDelegate().run_etl(
            "US_TN", "compliant_reporting_referral_record.json"
        )
