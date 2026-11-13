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
"""PTransform that writes each SentinelStrippedIdentityFragment to the
sentinel_stripped_identity_fragment table.
"""
import datetime

import apache_beam as beam

from recidiviz.pipelines.ingest.identity.sentinel_stripped_identity_fragment import (
    SENTINEL_STRIPPED_IDENTITY_FRAGMENT_TABLE_ID,
    SentinelStrippedIdentityFragment,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery


class WriteSentinelStrippedFragmentsToBQ(beam.PTransform):
    """PTransform that writes each SentinelStrippedIdentityFragment to the
    sentinel_stripped_identity_fragment table."""

    def __init__(
        self,
        *,
        output_dataset: str,
        stripped_at: datetime.datetime,
    ) -> None:
        super().__init__()
        # Dataset holding the sentinel_stripped_identity_fragment table (the
        # tenant's identity rejections dataset).
        self.output_dataset = output_dataset
        # Run timestamp stamped on every row this run writes.
        self.stripped_at = stripped_at

    def expand(
        self, input_or_inputs: beam.PCollection[SentinelStrippedIdentityFragment]
    ) -> beam.pvalue.PDone:
        # Each run fully replaces the table. WriteToBigQuery clears it when this
        # run strips nothing, so an empty run does not leave a prior run's rows
        # behind.
        return (
            input_or_inputs
            | "Convert stripped fragment records to BQ rows"
            >> beam.Map(
                lambda stripped: stripped.to_bq_row(stripped_at=self.stripped_at)
            )
            | f"Write stripped fragment records to {SENTINEL_STRIPPED_IDENTITY_FRAGMENT_TABLE_ID}"
            >> WriteToBigQuery(
                output_dataset=self.output_dataset,
                output_table=SENTINEL_STRIPPED_IDENTITY_FRAGMENT_TABLE_ID,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )
