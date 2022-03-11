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
"""Gating configuration for the ingest view file -> BQ materialization migration.
Determines which states/ingest instances should use BQ-based ingest view gating.

To update the configuration, take the following steps:
- Announce in #ingest-questions that you intend to change the config file for staging or prod [acquires pseudo-lock]
- Download file from gs://{project-name}-configs/bq_materialization_gating_config.yaml
- Update and re-upload file
- Announce in #ingest-questions that the change is complete [releases pseudo-lock]

DO NOT UPDATE THE CONFIG UNLESS YOU ARE ANNA OR HAVE TALKED TO ANNA.

The yaml file format for bq_materialization_gating_config.yaml is:

states:
- US_XX:
   PRIMARY: FILE
   SECONDARY: BQ
- US_YY:
   PRIMARY: FILE
   SECONDARY: FILE

TODO(#11424): Delete classes in this file and also associated config files in GCS once
 BQ materialization migration is complete.
"""
from collections import defaultdict
from enum import Enum
from typing import Dict

import yaml
from more_itertools import one

from recidiviz.cloud_storage.gcsfs_factory import GcsfsFactory
from recidiviz.cloud_storage.gcsfs_path import GcsfsFilePath
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.utils import metadata
from recidiviz.utils.yaml_dict import YAMLDict


class _MaterializationType(Enum):
    # Legacy format of materialized ingest view results. Results are written to/read
    # from GCS files.
    FILE = "FILE"

    # New format of materialized ingest view results. All states / ingest instances will
    # eventually be migrated to this format.
    BQ = "BQ"


class IngestViewMaterializationGatingContext:
    """Gating configuration for the ingest view file -> BQ materialization migration.
    Determines which states/ingest instances should use BQ-based ingest view gating.
    """

    def __init__(
        self,
        all_states_gating_context: Dict[
            StateCode, Dict[DirectIngestInstance, _MaterializationType]
        ],
    ):
        self._all_states_gating_context = all_states_gating_context

    def is_bq_ingest_view_materialization_enabled(
        self, state_code: StateCode, ingest_instance: DirectIngestInstance
    ) -> bool:
        """For a given state/ingest instance, returns whether ingest should be using
        BQ-based ingest view materialization.
        """
        return (
            self._all_states_gating_context[state_code][ingest_instance]
            == _MaterializationType.BQ
        )

    @staticmethod
    def gating_config_path() -> GcsfsFilePath:
        """Returns path to the gating config file for this project."""
        return GcsfsFilePath.from_absolute_path(
            f"gs://{metadata.project_id()}-configs/bq_materialization_gating_config.yaml"
        )

    @classmethod
    def load_from_gcs(cls) -> "IngestViewMaterializationGatingContext":
        """Loads a YAML file from
        gs://{project-name}-configs/bq_materialization_gating_config.yaml uses it to
        build a IngestViewMaterializationGatingContext object.
        """
        fs = GcsfsFactory.build()
        yaml_path = cls.gating_config_path()
        yaml_string = fs.download_as_string(yaml_path)
        try:
            yaml_config = YAMLDict(yaml.safe_load(yaml_string))
        except yaml.YAMLError as e:
            raise ValueError(f"Could not parse YAML in [{yaml_path.abs_path()}]") from e

        all_states_gating_context: Dict[
            StateCode, Dict[DirectIngestInstance, _MaterializationType]
        ] = defaultdict(dict)

        for state_info in yaml_config.pop_dicts("states"):
            # Load config for state into context dictionary
            state_code_str = one(state_info.keys())
            state_instance_info = state_info.pop_dict(state_code_str)
            state_code = StateCode(state_code_str)
            for ingest_instance in DirectIngestInstance:
                materialization_type = _MaterializationType(
                    state_instance_info.pop(ingest_instance.value, str)
                )
                all_states_gating_context[state_code][
                    ingest_instance
                ] = materialization_type

            # Validate that SECONDARY is migrated to BQ before PRIMARY
            if (
                all_states_gating_context[state_code][DirectIngestInstance.PRIMARY]
                == _MaterializationType.BQ
                and all_states_gating_context[state_code][
                    DirectIngestInstance.SECONDARY
                ]
                != _MaterializationType.BQ
            ):
                raise ValueError(
                    f"State [{state_code_str}] has the PRIMARY instance migrated to "
                    f"BQ materialization before the SECONDARY instance. Must first "
                    f"migrate SECONDARY to BQ."
                )

        return IngestViewMaterializationGatingContext(all_states_gating_context)
