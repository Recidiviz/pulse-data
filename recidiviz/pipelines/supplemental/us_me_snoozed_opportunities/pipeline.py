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
"""The US_ME pipeline for generating snooze events from ME OMS case notes.

ME has requested that all information, including product state data like snooze events,
be stored in their OMS. To accomplish this, we save snooze state information in
CIS_204_GEN_NOTE as a JSON blob. This view validates this filters to only snooze notes,
validates the snooze note, and saves the note to a BigQuery table.

See
recidiviz/tools/calculator/run_sandbox_calculation_pipeline.py for details on how to launch a
local run.
"""
import json
from typing import Dict, List, Type

import apache_beam as beam
import attr
from apache_beam import Pipeline
from more_itertools import one

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_query_provider import BigQueryQueryProvider
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.persistence.entity.serialization import json_serializable_dict
from recidiviz.pipelines.dataflow_config import (
    DATAFLOW_SUPPLEMENTAL_TABLE_TO_TABLE_FIELDS,
    US_ME_SNOOZED_OPPORTUNITIES_TABLE_NAME,
)
from recidiviz.pipelines.supplemental.base_supplemental_dataset_pipeline import (
    SupplementalDatasetPipeline,
)
from recidiviz.pipelines.supplemental.us_me_snoozed_opportunities.us_me_snoozed_opportunity_notes_query_provider import (
    US_ME_SNOOZED_OPPORTUNITY_NOTES_QUERY_NAME,
    get_us_me_snoozed_opportunity_notes_query_provider,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import (
    ReadFromBigQuery,
    WriteToBigQuery,
)
from recidiviz.pipelines.utils.execution_utils import TableRow
from recidiviz.utils import metadata

# These are specified manually so as not to depend on every workflows ViewBuilder. The unit test
# test_opportunities_list_matches_workflows_config verifies we're in sync with configured ME opportunities
ME_OPPORTUNITY_TYPES = [
    "usMeEarlyTermination",
    "usMeOverdueForDischarge",
    "usMeSCCP",
    "usMeWorkRelease",
    "usMeFurloughRelease",
    "usMeMediumTrustee",
    "usMeReclassificationReview",
]


@attr.s
class Snooze:
    is_recidiviz_snooze_note: bool = attr.ib(validator=attr_validators.is_bool)
    person_external_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    opportunity_type: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # TODO(#55650): Migrate to the new `is_valid_email` validator once we've confirmed
    #  the us_me_snoozed_opportunity pipeline won't crash.
    officer_email: str = attr.ib(validator=attr_validators.is_valid_email_legacy)
    start_date: str = attr.ib(validator=attr_validators.is_str_iso_formatted_date)
    end_date: str = attr.ib(validator=attr_validators.is_str_iso_formatted_date)
    denial_reasons: List[str] = attr.ib(validator=attr_validators.is_list_of(str))
    other_text: str = attr.ib(validator=attr_validators.is_str)

    def __attrs_post_init__(self) -> None:
        if self.opportunity_type not in ME_OPPORTUNITY_TYPES:
            raise ValueError(f"Unexpected opportunity_type: [{self.opportunity_type}]")


class UsMeSnoozedOpportunitiesPipeline(SupplementalDatasetPipeline):
    """Defines the US_ME pipeline for generating snooze events from ME OMS case notes."""

    @classmethod
    def pipeline_name(cls) -> str:
        return "US_ME_SNOOZED_OPPORTUNITIES_SUPPLEMENTAL"

    @classmethod
    def input_reference_query_providers(
        cls,
        state_code: StateCode,
        address_overrides: BigQueryAddressOverrides | None,
    ) -> Dict[str, BigQueryQueryProvider]:
        return {
            US_ME_SNOOZED_OPPORTUNITY_NOTES_QUERY_NAME: get_us_me_snoozed_opportunity_notes_query_provider(
                project_id=metadata.project_id(), address_overrides=address_overrides
            )
        }

    @classmethod
    def table_id(cls) -> str:
        return US_ME_SNOOZED_OPPORTUNITIES_TABLE_NAME

    @classmethod
    def table_fields(cls) -> Dict[str, Type]:
        return DATAFLOW_SUPPLEMENTAL_TABLE_TO_TABLE_FIELDS[
            US_ME_SNOOZED_OPPORTUNITIES_TABLE_NAME
        ]

    @staticmethod
    def extract_snooze_event_from_case_note_row(row: TableRow) -> TableRow:
        """Parses each row containing a snooze case note and returns a row
        containing the parsed note.
        """
        try:
            # We build the snooze object to validate it
            Snooze(**json.loads(row["Note_Tx"]))
            is_valid_snooze_note = True
            error_str = None
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            error_str = f"{e.__class__.__name__}: {str(e)}"
            is_valid_snooze_note = False

        final_row = {
            "person_id": row["person_id"],
            "person_external_id": row["Cis_100_Client_Id"],
            "Note_Id": row["Note_Id"],
            "Note_Date": row["Note_Date"],
            "state_code": StateCode.US_ME.value,
            "is_valid_snooze_note": is_valid_snooze_note,
            "note": row["Note_Tx"],
            "error": error_str,
        }
        return json_serializable_dict(final_row)

    def run_pipeline(self, p: Pipeline) -> None:
        state_code = StateCode(self.pipeline_parameters.state_code)
        query_name, query_provider = one(
            self.all_input_reference_query_providers(
                state_code=state_code,
                address_overrides=self.pipeline_parameters.input_dataset_overrides,
            ).items()
        )
        _ = (
            p
            | f"Load [{query_name}] query results"
            >> ReadFromBigQuery(
                query=query_provider.get_query(),
                resource_labels=self.pipeline_parameters.resource_labels,
            )
            | "Extract snooze notes"
            >> beam.Map(self.extract_snooze_event_from_case_note_row)
            | f"Write extracted snooze notes to {self.pipeline_parameters.output}.{self.table_id()}"
            >> WriteToBigQuery(
                output_table=self.table_id(),
                output_dataset=self.pipeline_parameters.output,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )


if __name__ == "__main__":
    with metadata.local_project_id_override("recidiviz-staging"):
        _, provider = one(
            UsMeSnoozedOpportunitiesPipeline.all_input_reference_query_providers(
                StateCode.US_ME, address_overrides=None
            ).items()
        )
        print(provider.get_query())
