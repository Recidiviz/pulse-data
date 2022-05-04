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
"""GCS Store used to keep counts of column values across the state ingest dataset
specifically."""

import json
from typing import Dict, List, Optional, Union

from recidiviz.admin_panel.admin_panel_store import AdminPanelStore
from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.ingest.direct.ingest_view_materialization.ingest_view_materialization_gating_context import (
    IngestViewMaterializationGatingContext,
)
from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_launched_in_env,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.persistence.database.bq_refresh.cloud_sql_to_bq_refresh_config import (
    CloudSqlToBQConfig,
)
from recidiviz.persistence.database.schema_utils import SchemaType
from recidiviz.utils import metadata
from recidiviz.utils.types import assert_type


class IngestDataFreshnessStore(AdminPanelStore):
    """An AdminPanelStore for or tracking data freshness of data ingested from our
    states.
    """

    def __init__(self) -> None:
        self.data_freshness_results: List[Dict[str, Union[Optional[str], bool]]] = []
        self.gcs_fs = GcsfsFactory.build()

    def recalculate_store(self) -> None:
        self.update_data_freshness_results()

    def update_data_freshness_results(self) -> None:
        """Refreshes information in the metadata store about freshness of ingested data
        for all states."""
        bq_export_config = CloudSqlToBQConfig.for_schema_type(
            SchemaType.STATE,
            yaml_path=GcsfsFilePath.from_absolute_path(
                f"gs://{metadata.project_id()}-configs/cloud_sql_to_bq_config.yaml"
            ),
        )
        if bq_export_config is None:
            raise ValueError("STATE CloudSqlToBQConfig unexpectedly None.")

        regions_paused = bq_export_config.region_codes_to_exclude

        latest_upper_bounds_path = GcsfsFilePath.from_absolute_path(
            f"gs://{metadata.project_id()}-ingest-metadata/ingest_metadata_latest_ingested_upper_bounds.json"
        )
        latest_upper_bounds_json = self.gcs_fs.download_as_string(
            latest_upper_bounds_path
        )
        latest_upper_bounds: List[Dict[str, Union[Optional[str], bool]]] = []

        ingest_view_materialization_gating_context = (
            IngestViewMaterializationGatingContext.load_from_gcs()
        )

        processed_date_by_state_code: Dict[str, str] = {}
        for line in latest_upper_bounds_json.splitlines():
            line = line.strip()
            if not line:
                continue
            struct = json.loads(line)
            state_code_str = assert_type(struct["state_code"], str)
            processed_date_by_state_code[state_code_str.upper()] = struct.get(
                "processed_date"
            )

        for state_code in get_direct_ingest_states_launched_in_env():
            # Check PRIMARY for bq materialization since that is where BQ is exported to.
            if ingest_view_materialization_gating_context.is_bq_ingest_view_materialization_enabled(
                state_code, DirectIngestInstance.PRIMARY
            ):
                latest_upper_bounds.append(
                    {
                        "state": state_code.name,
                        # TODO(#11413): Update to pass the correct date through here (PR 9).
                        "date": processed_date_by_state_code.get(state_code.name),
                        "ingestPaused": state_code.name in regions_paused,
                        # TODO(#11413): Delete this flag and frontend usage once we
                        #  have proper support for BQ materialization.
                        "isBQMaterializationEnabled": True,
                    }
                )
            else:
                latest_upper_bounds.append(
                    {
                        "state": state_code.name,
                        "date": processed_date_by_state_code.get(state_code.name),
                        "ingestPaused": state_code.name in regions_paused,
                        # TODO(#11413): Delete this flag and frontend usage once we
                        #  have proper support for BQ materialization.
                        "isBQMaterializationEnabled": False,
                    }
                )
        self.data_freshness_results = latest_upper_bounds
