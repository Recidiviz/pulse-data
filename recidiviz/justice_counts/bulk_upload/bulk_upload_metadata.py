# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2024 Recidiviz, Inc.
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
"""
This module provides the `BulkUploadMetadata` class, which manages metadata and auxiliary processing
for BulkUpload.
"""
import datetime
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from sqlalchemy.exc import InvalidRequestError
from sqlalchemy.orm import Session

from recidiviz.common.text_analysis import TextAnalyzer, TextMatchingConfiguration
from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.justice_counts.exceptions import JusticeCountsBulkUploadException
from recidiviz.justice_counts.metric_setting import MetricSettingInterface
from recidiviz.justice_counts.metricfiles.metricfile_registry import (
    SYSTEM_TO_METRICFILES,
)
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.justice_counts.types import DatapointJson
from recidiviz.justice_counts.utils.constants import UploadMethod
from recidiviz.persistence.database.schema.justice_counts import schema


class BulkUploadMetadata:
    """
    Handles metadata and auxiliary processing for BulkUpload. This class manages the
    relationship between an agency's metrics, tracks any errors encountered during
    the bulk upload, and prepares data for further analysis or review.
    """

    def __init__(
        self,
        system: schema.System,
        agency: schema.Agency,
        session: Session,
        user_account: Optional[schema.UserAccount] = None,
        chunk_size: int = 10000,
    ):
        self.system = system
        self.agency = agency
        self.session = session
        self.user_account = user_account
        self.upload_method = UploadMethod.BULK_UPLOAD
        self.is_single_page_upload = False
        self.chunk_size = chunk_size
        self.inserts: List[schema.Datapoint] = []
        self.updates: List[schema.Datapoint] = []
        self.histories: List[schema.DatapointHistory] = []
        self.metric_files = SYSTEM_TO_METRICFILES[system]
        self.agency_id_to_time_range_to_reports: Dict[
            int, Dict[Tuple[Any, Any], List[schema.Report]]
        ] = defaultdict(dict)
        self.sheet_name_to_seen_rows: Dict[str, Set[Tuple[str, ...]]] = defaultdict(set)
        self.agency_name_to_metric_key_to_timerange_to_total_value: Dict[
            str,
            Dict[
                str,
                Dict[Tuple[datetime.date, datetime.date], Optional[Union[int, float]]],
            ],
        ] = defaultdict(lambda: defaultdict(dict))
        # A list of existing report IDs

        # metric_key_to_errors starts out empty and will be populated with
        # each metric's errors.
        self.metric_key_to_errors: Dict[
            Optional[str], List[JusticeCountsBulkUploadException]
        ] = defaultdict(list)

        # metric_key_to_datapoint_jsons starts out empty and will be populated with
        # the ingested datapoints for each metric. We need these datapoints because
        # we send these to the frontend after the upload so it can render
        # the review page.
        self.metric_key_to_datapoint_jsons: Dict[
            str, List[DatapointJson]
        ] = defaultdict(list)
        self.text_analyzer = TextAnalyzer(
            configuration=TextMatchingConfiguration(
                # We don't want to treat "other" as a stop word,
                # because it's a valid breakdown category. We
                # also don't want to treat "not" as a stop word because
                # it is an important distinction between breakdowns
                # (i.e Not Hispanic v. Hispanic).
                stop_words_to_remove={"other", "not"}
            )
        )

        # Fetch all child agencies before expunging them
        self.child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=self.session, agency=self.agency
        )

        self.metric_key_to_metric_interface: Dict[
            str, MetricInterface
        ] = MetricSettingInterface.get_metric_key_to_metric_interface(
            session=self.session, agency=self.agency, expunge_metric_settings=True
        )

        self.child_agency_name_to_agency: Dict[str, schema.Agency] = {}
        self.child_agency_id_to_metric_key_to_metric_interface: Dict[
            int, Dict[str, MetricInterface]
        ] = defaultdict(dict)

        for child_agency in self.child_agencies:
            self.child_agency_id_to_metric_key_to_metric_interface[
                child_agency.id
            ] = MetricSettingInterface.get_metric_key_to_metric_interface(
                session=session,
                agency=child_agency,
                expunge_metric_settings=True,
            )
            self.child_agency_name_to_agency[
                child_agency.name.strip().lower()
            ] = child_agency
            if child_agency.custom_child_agency_name is not None:
                self.child_agency_name_to_agency[
                    child_agency.custom_child_agency_name.strip().lower()
                ] = child_agency

        # Expunge agencies after fetching
        self.expunge_agencies()

    def expunge_agencies(self) -> None:
        """
        Expunges all agency objects after all required properties have been initialized.

        This ensures that:
        - Agencies are **not tracked by SQLAlchemy**, preventing unnecessary memory usage.
        - The session remains **clean** for further processing without unintended modifications.
        - Agencies are available in `child_agency_name_to_agency` and `child_agency_id_to_metric_key_to_metric_interface` before expunging.

        This should be called **after all agency-dependent properties have been accessed**.
        """

        for child_agency in self.child_agencies:
            self.session.expunge(child_agency)

        try:
            self.session.expunge(self.agency)  # Expunge the main agency
        except InvalidRequestError:
            # The main agency might not be in the session, safely ignore
            pass
