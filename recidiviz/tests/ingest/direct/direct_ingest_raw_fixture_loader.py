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
from collections import defaultdict
from concurrent import futures
from types import ModuleType
from typing import Dict, Iterable, List

import pytz
from more_itertools import one

from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
    DirectIngestViewRawFileDependency,
    RawFileHistoricalRowsFilterType,
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
        self.raw_file_config = get_region_raw_file_config(
            self.state_code.value, region_module
        )
        # The default upper bound datetime for raw data fixtures
        # is now, in UTC without timezone info.
        self.default_upper_bound_dates_json = json.dumps(
            {
                file_tag: datetime.datetime.now(tz=pytz.UTC)
                .replace(tzinfo=None)
                .isoformat()
                for file_tag in self.raw_file_config.raw_file_tags
            }
        )

    def load_raw_fixtures_to_emulator(
        self,
        ingest_views: List[DirectIngestViewQueryBuilder],
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
        with futures.ThreadPoolExecutor() as executor:
            create_table_futures = [
                executor.submit(
                    self._load_dependency_to_emulator,
                    dependency=dependency,
                    ingest_test_identifier=ingest_test_identifier,
                    create_tables=create_tables,
                )
                for dependency in self._raw_dependencies_for_ingest_views(ingest_views)
            ]
        for future in futures.as_completed(create_table_futures):
            future.result()

    def _load_dependency_to_emulator(
        self,
        dependency: DirectIngestViewRawFileDependency,
        ingest_test_identifier: str,
        create_tables: bool,
    ) -> None:
        """Reads the given dependency into the emulator."""
        fixture = RawDataFixture(dependency)
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
                f" for {dependency.raw_table_dependency_arg_name} for test "
                f"{ingest_test_identifier}"
            ) from e

    # TODO(#38355) This will just be the set of file tags
    # when all fixtures are @ALL fixtures
    def _raw_dependencies_for_ingest_views(
        self,
        ingest_views: List[DirectIngestViewQueryBuilder],
    ) -> Iterable[DirectIngestViewRawFileDependency]:
        """
        Generates the minimum set of DirectIngestViewRawFileDependency objects.
        If we use both LATEST and ALL for a data source, we'll only yield the ALL.
        """
        configs_by_file_tag: Dict[
            str, List[DirectIngestViewRawFileDependency]
        ] = defaultdict(list)
        for ingest_view in ingest_views:
            for config in ingest_view.raw_table_dependency_configs:
                configs_by_file_tag[config.file_tag].append(config)

        for dependency_configs in configs_by_file_tag.values():
            dependencies_by_type = defaultdict(set)
            for dependency in dependency_configs:
                dependencies_by_type[dependency.filter_type].add(
                    dependency.raw_table_dependency_arg_name
                )

            # For a given raw file tag, we expect to only have exactly one distinct
            # raw_table_dependency_arg_name for a given filter type.
            # If there are any @ALL dependencies, we choose he fixture associated with
            # that filter type, otherwise we choose the LATEST fixture.
            if RawFileHistoricalRowsFilterType.ALL in dependencies_by_type:
                raw_table_dependency_arg_name = one(
                    dependencies_by_type[RawFileHistoricalRowsFilterType.ALL]
                )
            else:
                raw_table_dependency_arg_name = one(
                    dependencies_by_type[RawFileHistoricalRowsFilterType.LATEST]
                )

            # We use the selected raw_table_dependency_arg_name to build a single
            # dependency for this raw file which will be used to locate the fixture we
            # care about.
            yield DirectIngestViewRawFileDependency.from_raw_table_dependency_arg_name(
                raw_table_dependency_arg_name=raw_table_dependency_arg_name,
                region_raw_table_config=self.raw_file_config,
            )
