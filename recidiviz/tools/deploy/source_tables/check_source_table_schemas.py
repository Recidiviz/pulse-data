# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
Script that checks whether our source table schemas, as defined in code, match the
schemas of the corresponding tables already deployed to BigQuery.

Usage:
python -m recidiviz.tools.deploy.source_tables.check_source_table_schemas \
    --project-id PROJECT_ID \
    --source_table_type {externally_managed,protected,regenerable,all}

To check if if all protected tables (e.g. raw data tables) have matching schemas:
python -m recidiviz.tools.deploy.source_tables.check_source_table_schemas \
    --project-id recidiviz-staging \
    --source_table_type protected
"""

import argparse
import logging

from recidiviz.source_tables.collect_all_source_table_configs import (
    build_source_table_repository_for_collected_schemata,
)
from recidiviz.source_tables.update_big_query_table_schemas import (
    SourceTableCheckType,
    check_source_table_schemas,
)
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--project-id",
        choices=[GCP_PROJECT_STAGING, GCP_PROJECT_PRODUCTION],
        required=True,
    )
    parser.add_argument(
        "--source_table_type",
        type=str,
        required=True,
        choices=[t.value for t in SourceTableCheckType],
    )
    return parser.parse_args()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    args = parse_args()

    with local_project_id_override(args.project_id):
        source_table_repository = build_source_table_repository_for_collected_schemata(
            project_id=args.project_id,
        )

        source_table_check_type = SourceTableCheckType(args.source_table_type)

        check_source_table_schemas(
            source_table_repository=source_table_repository,
            source_table_check_type=source_table_check_type,
        )
