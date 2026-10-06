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
"""Loads the config for a single yaml-managed source table from its YAML file."""
import os

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.source_tables import yaml_managed
from recidiviz.source_tables.source_table_config import SourceTableConfig


def load_yaml_managed_source_table_config(
    address: BigQueryAddress,
) -> SourceTableConfig:
    """Returns the config for the yaml-managed source table at address, reading
    only that table's YAML file. View builders call this at import time, so it
    avoids building the whole yaml-managed source table repository.
    """
    return SourceTableConfig.from_file(
        os.path.join(
            os.path.dirname(yaml_managed.__file__),
            address.dataset_id,
            f"{address.table_id}.yaml",
        )
    )
