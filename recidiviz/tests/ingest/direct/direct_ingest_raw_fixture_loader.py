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
"""Contains a class to handle loading raw data fixtures to the BigQueryEmulator
for ingest tests.
"""
import datetime
import json
from concurrent import futures
from types import ModuleType

import pytz

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import (
    DirectIngestRawFileConfig,
    get_region_raw_file_config,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct.raw_data_fixture import RawDataFixture


class DirectIngestRawDataFixtureLoader:
    """Finds relevant raw data fixtures for ingest tests and loads them to the BigQueryEmulator."""

    def __init__(
        self,
        state_code: StateCode,
        emulator_test: BigQueryEmulatorTestCase,
        region_module: ModuleType | None = None,
    ):
        self.state_code = state_code
        self.bq_client = emulator_test.bq_client
        self.emulator_test = emulator_test
        self.raw_tables_dataset_id = raw_tables_dataset_for_region(
            state_code=self.state_code, instance=DirectIngestInstance.PRIMARY
        )
        self.raw_region_config = get_region_raw_file_config(
            self.state_code.value, region_module
        )
        # The default upper bound datetime for raw data fixtures
        # is now, in UTC without timezone info.
        self.default_upper_bound_dates_json = json.dumps(
            {
                file_tag: datetime.datetime.now(tz=pytz.UTC)
                .replace(tzinfo=None)
                .isoformat()
                for file_tag in self.raw_region_config.raw_file_tags
            }
        )

    def load_raw_fixtures_to_emulator(
        self,
        ingest_views: list[DirectIngestViewQueryBuilder],
        ingest_test_identifier: str,
        create_tables: bool,
    ) -> None:
        """
        Loads raw data tables to the emulator for the given ingest views.
        All raw fixture files must have names matching the ingest_test_identifier.
        For example, an ingest test named "test_person_001" will look for fixture
        files called "person_001.csv".
        """
        self.bq_client.create_dataset_if_necessary(
            dataset_id=self.raw_tables_dataset_id
        )
        # We load all data for a table as if it were in a us_xx_raw_data dataset.
        # Since we only want to load a table to the BigQuery emulator once, we
        # use the raw_file_config as our base.
        raw_file_configs = {
            dependency.raw_file_config
            for ingest_view in ingest_views
            for dependency in ingest_view.raw_table_dependency_configs
        }
        with futures.ThreadPoolExecutor() as executor:
            create_table_futures = [
                executor.submit(
                    self._load_dependency_to_emulator,
                    raw_file_config=raw_file_config,
                    ingest_test_identifier=ingest_test_identifier,
                    create_tables=create_tables,
                )
                for raw_file_config in raw_file_configs
            ]
        for future in futures.as_completed(create_table_futures):
            future.result()

    def _load_dependency_to_emulator(
        self,
        raw_file_config: DirectIngestRawFileConfig,
        ingest_test_identifier: str,
        create_tables: bool,
    ) -> None:
        """Reads the given dependency into the emulator."""
        fixture = RawDataFixture(raw_file_config)
        fixture_df = fixture.read_fixture_file_into_dataframe(ingest_test_identifier)

        if create_tables:
            self.emulator_test.create_mock_table(fixture.address, fixture.schema)
        try:
            self.bq_client.stream_into_table(
                fixture.address,
                rows=fixture_df.to_dict("records"),
            )
        except Exception as e:
            raise ValueError(
                f"Failed to load DataFrame records into BigQueryEmulator"
                f" for {raw_file_config.file_tag} for test "
                f"{ingest_test_identifier}"
            ) from e
