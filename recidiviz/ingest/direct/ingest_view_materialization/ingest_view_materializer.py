# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Class that manages logic related to materializing ingest views for a region so
the results can be processed and merged into our Postgres database.
"""

from recidiviz.big_query.big_query_utils import datetime_clause
from recidiviz.ingest.direct.types.cloud_task_args import IngestViewMaterializationArgs
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    MATERIALIZATION_TIME_COL_NAME,
    UPPER_BOUND_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.utils.string import StrictStringFormatter

_DATAFLOW_INGEST_VIEW_OUTPUT_QUERY_TEMPLATE = f"""
WITH view_results AS (
    {{view_query}}
)
SELECT *,
    CURRENT_DATETIME('UTC') AS {MATERIALIZATION_TIME_COL_NAME},
    {{upper_bound_datetime_inclusive}} AS {UPPER_BOUND_DATETIME_COL_NAME}
FROM view_results;
"""


# TODO(#20930): Delete this class and move the dataflow_query_for_args() to
#  generate_ingest_view_results.py.
class IngestViewMaterializerImpl:
    """Class that manages logic related to materializing ingest views for a region so
    the results can be processed and merged into our Postgres database.
    """

    @classmethod
    def dataflow_query_for_args(
        cls,
        view_builder: DirectIngestViewQueryBuilder,
        raw_data_source_instance: DirectIngestInstance,
        ingest_view_materialization_args: IngestViewMaterializationArgs,
    ) -> str:
        """Returns a version of the ingest view query for the provided args that can
        be run in Dataflow. Augments the ingest view query with metadata columns that
        will be output to materialized ingest view results tables.

        A note that this query for Dataflow cannot use materialized tables or temporary
        tables."""
        upper_bound_datetime_inclusive = (
            ingest_view_materialization_args.upper_bound_datetime_inclusive
        )

        view_query = (
            view_builder.build_query(
                config=DirectIngestViewQueryBuilder.QueryStructureConfig(
                    raw_data_source_instance=raw_data_source_instance,
                    raw_data_datetime_upper_bound=upper_bound_datetime_inclusive,
                    use_order_by=False,
                ),
                using_dataflow=True,
            )
            .rstrip()
            .rstrip(";")
        )

        upper_bound_datetime_inclusive_clause = datetime_clause(
            upper_bound_datetime_inclusive
        )

        return StrictStringFormatter().format(
            _DATAFLOW_INGEST_VIEW_OUTPUT_QUERY_TEMPLATE,
            view_query=view_query,
            upper_bound_datetime_inclusive=upper_bound_datetime_inclusive_clause,
        )
