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
"""Functionality for collecting SourceTableConfigs from defined configuration YAMLs."""
import glob
import os
from collections import defaultdict

from recidiviz.source_tables.source_table_config import SourceTableConfig


def collect_source_tables_from_yamls_by_dataset(
    yamls_root_path: str,
) -> dict[str, list[SourceTableConfig]]:
    """Returns a list of SourceTableConfig for all source tables defined in YAMLs
    that can be found inside the |yamls_root_path|.
    """
    yaml_paths = glob.glob(os.path.join(yamls_root_path, "**/*.yaml"))

    source_tables_by_dataset = defaultdict(list)
    for yaml_path in yaml_paths:
        config = SourceTableConfig.from_file(yaml_path)
        source_tables_by_dataset[config.address.dataset_id].append(config)
    return source_tables_by_dataset
