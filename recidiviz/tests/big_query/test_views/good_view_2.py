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
"""A test view builder file for big_query_view_collector_test.py"""

from recidiviz.ingest.direct.controllers.direct_ingest_big_query_view_types import DirectIngestPreProcessedIngestView
from recidiviz.ingest.direct.controllers.direct_ingest_raw_file_import_manager import DirectIngestRegionRawFileConfig
from recidiviz.tests.big_query.fake_big_query_view_builder import FakeBigQueryViewBuilder
from recidiviz.utils.metadata import local_project_id_override

with local_project_id_override('my-project-id'):
    GOOD_VIEW_2 = DirectIngestPreProcessedIngestView(
        ingest_view_name='ingest_view_name',
        view_query_template='SELECT * FROM table1',
        region_raw_table_config=DirectIngestRegionRawFileConfig(
            region_code='us_xx',
            yaml_config_file_path='/a/path/to/a.yaml',
            raw_file_configs={}
        ),
        order_by_cols=None,
        is_detect_row_deletion_view=False,
        primary_key_tables_for_entity_deletion=[],
    )

VIEW_BUILDER = FakeBigQueryViewBuilder(GOOD_VIEW_2)
