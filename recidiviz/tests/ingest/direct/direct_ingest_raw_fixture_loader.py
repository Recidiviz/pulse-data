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

import attr
import pandas as pd
import pytz
from google.cloud.bigquery import SchemaField
from more_itertools import one

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_client import BQ_CLIENT_MAX_POOL_SIZE
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.dataset_config import raw_tables_dataset_for_region
from recidiviz.ingest.direct.raw_data.direct_ingest_raw_table_schema_builder import (
    RawDataTableBigQuerySchemaBuilder,
)
from recidiviz.ingest.direct.raw_data.legacy_direct_ingest_raw_file_import_manager import (
    check_found_columns_are_subset_of_config,
)
from recidiviz.ingest.direct.raw_data.raw_file_configs import get_region_raw_file_config
from recidiviz.ingest.direct.types.direct_ingest_constants import (
    FILE_ID_COL_NAME,
    IS_DELETED_COL_NAME,
    UPDATE_DATETIME_COL_NAME,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.ingest.direct.views.direct_ingest_view_query_builder import (
    DirectIngestViewQueryBuilder,
    DirectIngestViewRawFileDependency,
    RawFileHistoricalRowsFilterType,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.ingest.direct.fixture_util import load_dataframe_from_path
from recidiviz.tests.ingest.direct.legacy_fixture_path import (
    DirectIngestTestFixturePath,
)
from recidiviz.utils import csv


@attr.define
class RawDataFixture:
    config: DirectIngestViewRawFileDependency
    fixture_data_df: pd.DataFrame
    bq_dataset_id: str

    @property
    def address(self) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=self.bq_dataset_id,
            table_id=self.config.file_tag,
        )

    @property
    def schema(self) -> List[SchemaField]:
        return RawDataTableBigQuerySchemaBuilder.build_bq_schema_for_config(
            raw_file_config=self.config.raw_file_config
        )


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
        with futures.ThreadPoolExecutor(
            # Conservatively allow only half as many workers as allowed connections.
            # Lower this number if we see "urllib3.connectionpool:Connection pool is
            # full, discarding connection" errors.
            max_workers=int(BQ_CLIENT_MAX_POOL_SIZE / 2)
        ) as executor:
            create_table_futures = [
                executor.submit(
                    self._load_fixture_to_emulator,
                    fixture=fixture,
                    create_tables=create_tables,
                )
                for fixture in self._generate_raw_data_fixtures(
                    ingest_views, ingest_test_identifier
                )
            ]
        for future in futures.as_completed(create_table_futures):
            future.result()

    def _load_fixture_to_emulator(
        self, fixture: RawDataFixture, create_tables: bool
    ) -> None:
        if create_tables:
            self.emulator_test.create_mock_table(fixture.address, fixture.schema)
        self.bq_client.stream_into_table(
            fixture.address,
            rows=fixture.fixture_data_df.to_dict("records"),
        )

    def _generate_raw_data_fixtures(
        self,
        ingest_views: List[DirectIngestViewQueryBuilder],
        ingest_test_identifier: str,
    ) -> Iterable[RawDataFixture]:
        """
        Generates the unique set of RawDataFixture objects for the given
        ingest views, assumed file update datetime, and raw fixture filename.
        All raw fixture files must have names matching the ingest_test_identifier.
        """
        for config in self._raw_dependencies_for_ingest_views(ingest_views):
            try:
                fixture_df = self._read_raw_data_fixture(
                    config,
                    ingest_test_identifier,
                )
            except Exception as e:
                raise ValueError(
                    f"Failed to load fixture for "
                    f"{config.raw_table_dependency_arg_name} for test "
                    f"{ingest_test_identifier}"
                ) from e
            yield RawDataFixture(
                config=config,
                bq_dataset_id=self.raw_tables_dataset_id,
                fixture_data_df=fixture_df,
            )

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

    def _check_valid_fixture_columns(
        self,
        raw_file_dependency_config: DirectIngestViewRawFileDependency,
        fixture_file: str,
    ) -> List[str]:
        """Checks that the raw data fixture columns are valid given it's config."""
        fixture_columns = csv.get_csv_columns(fixture_file)
        metadata_columns = {
            IS_DELETED_COL_NAME,
            UPDATE_DATETIME_COL_NAME,
            FILE_ID_COL_NAME,
        }
        found_metadata = set(fixture_columns).intersection(metadata_columns)
        if missing_required := metadata_columns - found_metadata:
            raise ValueError(
                f"Did NOT find metadata columns [{missing_required}] for fixture {fixture_file}"
            )
        columns_to_check = [
            col for col in fixture_columns if col not in metadata_columns
        ]
        check_found_columns_are_subset_of_config(
            raw_file_config=raw_file_dependency_config.raw_file_config,
            found_columns=columns_to_check,
        )
        return fixture_columns

    # TODO(#38355) Read all raw data fixtures from "raw data" version, not "latest".
    def _read_raw_data_fixture(
        self,
        raw_file_dependency_config: DirectIngestViewRawFileDependency,
        csv_fixture_file_name: str,
    ) -> pd.DataFrame:
        """Reads the raw data fixture file for the provided dependency into a Dataframe."""
        raw_fixture_path = DirectIngestTestFixturePath.for_raw_file_fixture(
            region_code=self.state_code.value,
            raw_file_dependency_config=raw_file_dependency_config,
            file_name=csv_fixture_file_name,
        ).full_path()
        print(
            f"Loading fixture data for raw file [{raw_file_dependency_config.file_tag}] "
            f"from file path [{raw_fixture_path}]."
        )
        fixture_columns = self._check_valid_fixture_columns(
            raw_file_dependency_config, raw_fixture_path
        )
        df = load_dataframe_from_path(raw_fixture_path, fixture_columns)
        file_ids_per_update_dt = set(
            df.groupby("update_datetime").file_id.nunique().values
        )
        if file_ids_per_update_dt and file_ids_per_update_dt != {1}:
            raise ValueError(
                f"{raw_fixture_path} has multiple file_id values per update_datetime"
            )
        return df
