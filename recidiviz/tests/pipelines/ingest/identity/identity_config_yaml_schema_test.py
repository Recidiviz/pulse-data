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
"""Tests that every tenant's identity_config.yaml conforms to the JSON schema."""
import unittest
from pathlib import Path

from recidiviz.ingest.direct.regions.direct_ingest_region_utils import (
    get_direct_ingest_states_existing_in_env,
)
from recidiviz.pipelines.ingest.identity import yaml_schema
from recidiviz.pipelines.ingest.identity.identity_ingest_pipeline_config import (
    identity_config_path_for_state_code,
)
from recidiviz.utils.yaml_dict import YAMLDict
from recidiviz.utils.yaml_dict_validator import validate_yaml_matches_schema


class IdentityConfigYamlSchemaTest(unittest.TestCase):
    """Tests that every tenant's identity_config.yaml conforms to the JSON schema."""

    def test_validate_all_identity_config_schemas(self) -> None:
        schema_path = Path(yaml_schema.__file__).parent / "schema.json"
        for state_code in get_direct_ingest_states_existing_in_env():
            yaml_path = identity_config_path_for_state_code(state_code)
            with self.subTest(yaml_file=yaml_path):
                validate_yaml_matches_schema(
                    yaml_dict=YAMLDict.from_path(yaml_path),
                    json_schema_path=str(schema_path),
                )
