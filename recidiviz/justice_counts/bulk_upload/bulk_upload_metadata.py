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
from functools import cached_property
from typing import Any, Dict, List, Optional, Tuple

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
    ):
        self.system = system
        self.agency = agency
        self.session = session
        self.user_account = user_account
        self.upload_method = UploadMethod.BULK_UPLOAD
        self.user_account = user_account
        self.metric_files = SYSTEM_TO_METRICFILES[system]
        self.agency_id_to_time_range_to_reports: Dict[
            int, Dict[Tuple[Any, Any], List[schema.Report]]
        ] = defaultdict(dict)
        self.agency_name_to_metric_key_to_timerange_to_total_value: Dict[
            str,
            Dict[str, Dict[Tuple[datetime.date, datetime.date], Optional[int]]],
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

    @cached_property
    def child_agency_name_to_agency(self) -> Dict[str, schema.Agency]:
        """
        Constructs a dictionary mapping normalized child agency names and
        custom child agency names to their corresponding agency objects.

        Returns:
            A dictionary mapping normalized (lowercase, stripped) child agency names to
            their corresponding `schema.Agency` objects.
        """
        child_agencies = AgencyInterface.get_child_agencies_for_agency(
            session=self.session, agency=self.agency
        )
        child_agency_name_to_agency = {}
        for child_agency in child_agencies:
            child_agency_name_to_agency[
                child_agency.name.strip().lower()
            ] = child_agency
            if child_agency.custom_child_agency_name is not None:
                # Add the custom_child_agency_name of the agency as a key in
                # child_agency_name_to_agency. That way, Bulk Upload will
                # be successful if the user uploads with EITHER the name
                # or the original name of the agency
                child_agency_name_to_agency[
                    child_agency.custom_child_agency_name.strip().lower()
                ] = child_agency

        return child_agency_name_to_agency

    @cached_property
    def metric_key_to_metric_interface(self) -> Dict[str, MetricInterface]:
        """
        Retrieves a mapping of metric keys to metric interfaces for the agency.

        The metric interfaces contain only the metric setting information (e.g.,
        enabled/disabled status, custom reporting frequency) without the report data.

        Returns:
            Dict[str, MetricInterface]: A dictionary mapping metric keys to their corresponding
            `MetricInterface` objects for the agency.
        """
        return MetricSettingInterface.get_metric_key_to_metric_interface(
            session=self.session, agency=self.agency
        )

    @cached_property
    def child_agency_id_to_metric_key_to_metric_interface(
        self,
    ) -> Dict[int, Dict[str, MetricInterface]]:
        """
        Retrieves a mapping of child agency IDs to their corresponding metric key
        to metric interface mappings.

        Returns:
            Dict[int, Dict[str, MetricInterface]]: A dictionary where the keys are
            child agency IDs and the values are dictionaries mapping metric keys to
            `MetricInterface` objects for those child agencies.
        """
        return MetricSettingInterface.get_agency_id_to_metric_key_to_metric_interface(
            session=self.session,
            agencies=list(self.child_agency_name_to_agency.values()),
        )
