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

from recidiviz.practices.etl.client_record_etl_delegate import ClientRecordETLDelegate
from recidiviz.practices.etl.compliant_reporting_referral_record_etl_delegate import (
    CompliantReportingReferralRecordETLDelegate,
)
from recidiviz.practices.etl.practices_etl_delegate import PracticesFirestoreETLDelegate
from recidiviz.practices.etl.staff_record_etl_delegate import StaffRecordETLDelegate
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


class PracticesFirestoreDemoETLDelegate(PracticesFirestoreETLDelegate):
    """Abstract class containing the ETL logic for exporting a specified demo data fixture to Firestore."""

    @property
    def fixture_filepath(self) -> Path:
        return Path(__file__).parent / "demo_fixtures" / self.EXPORT_FILENAME

    @property
    def filepath_url(self) -> str:
        return self.fixture_filepath.as_uri()

    def get_file_stream(self) -> Iterator[TextIO]:
        """Returns a stream of the contents of the file this delegate is watching for."""
        # bypassing self.get_filepath since it assumes a GCS file
        with open(self.fixture_filepath, "r", encoding="utf8") as f:
            yield f

    @property
    def collection_name(self) -> str:
        """Name of the Firestore collection this delegate will ETL into."""
        return f"DEMO_{self._COLLECTION_NAME_BASE}"


class StaffRecordDemoETLDelegate(
    PracticesFirestoreDemoETLDelegate, StaffRecordETLDelegate
):
    """Delegate class to ETL demo_fixtures/staff_record.json into Firestore."""


class ClientRecordDemoETLDelegate(
    PracticesFirestoreDemoETLDelegate, ClientRecordETLDelegate
):
    """Delegate class to ETL demo_fixtures/client_record.json into Firestore."""


class CompliantReportingReferralRecordDemoETLDelegate(
    PracticesFirestoreDemoETLDelegate, CompliantReportingReferralRecordETLDelegate
):
    """Delegate class to ETL demo_fixtures/compliant_reporting_referral_record.json into Firestore."""


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    with local_project_id_override(GCP_PROJECT_STAGING):
        StaffRecordDemoETLDelegate().run_etl()
        ClientRecordDemoETLDelegate().run_etl()
        CompliantReportingReferralRecordDemoETLDelegate().run_etl()
