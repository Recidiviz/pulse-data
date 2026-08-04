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
"""PTransform that writes each RejectedIdentityCluster to the
rejected_identity_cluster table.
"""
import datetime

import apache_beam as beam

from recidiviz.pipelines.ingest.identity.rejected_identity_cluster import (
    REJECTED_IDENTITY_CLUSTER_TABLE_ID,
    RejectedIdentityCluster,
)
from recidiviz.pipelines.utils.beam_utils.bigquery_io_utils import WriteToBigQuery


class WriteRejectedIdentityClustersToBQ(beam.PTransform):
    """PTransform that writes each RejectedIdentityCluster to the
    rejected_identity_cluster table."""

    def __init__(
        self,
        *,
        output_dataset: str,
        rejected_at: datetime.datetime,
    ) -> None:
        super().__init__()
        # Dataset holding the rejected_identity_cluster table (the tenant's
        # identity rejections dataset).
        self.output_dataset = output_dataset
        # Run timestamp stamped on every row this run writes.
        self.rejected_at = rejected_at

    def expand(
        self, input_or_inputs: beam.PCollection[RejectedIdentityCluster]
    ) -> beam.pvalue.PDone:
        # Each run fully replaces the table. WriteToBigQuery clears it when this
        # run rejects nothing, so an empty run does not leave a prior run's rows
        # behind.
        return (
            input_or_inputs
            | "Convert rejected clusters to BQ rows"
            >> beam.Map(
                lambda rejected: rejected.to_bq_row(rejected_at=self.rejected_at)
            )
            | f"Write rejected clusters to {REJECTED_IDENTITY_CLUSTER_TABLE_ID}"
            >> WriteToBigQuery(
                output_dataset=self.output_dataset,
                output_table=REJECTED_IDENTITY_CLUSTER_TABLE_ID,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            )
        )
