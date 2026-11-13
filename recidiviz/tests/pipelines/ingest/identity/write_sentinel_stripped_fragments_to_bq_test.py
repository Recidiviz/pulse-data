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
"""Tests for WriteSentinelStrippedFragmentsToBQ, run against the BQ emulator."""
import datetime
from functools import partial
from unittest.mock import patch

import apache_beam as beam

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.common.constants.tenants import Tenant
from recidiviz.pipelines.ingest.identity import write_sentinel_stripped_fragments_to_bq
from recidiviz.pipelines.ingest.identity.sentinel_stripped_identity_fragment import (
    SENTINEL_STRIPPED_IDENTITY_FRAGMENT_TABLE_ID,
    SentinelStrippedIdentityFragment,
    sentinel_stripped_identity_fragment_schema_fields,
)
from recidiviz.pipelines.ingest.identity.write_sentinel_stripped_fragments_to_bq import (
    WriteSentinelStrippedFragmentsToBQ,
)
from recidiviz.tests.big_query.big_query_emulator_test_case import (
    BigQueryEmulatorTestCase,
)
from recidiviz.tests.pipelines.beam_test_utils import create_test_pipeline
from recidiviz.tests.pipelines.fake_bigquery import FakeWriteToBigQueryEmulator

_TENANT = Tenant.US_DD
_TABLE_ADDRESS = BigQueryAddress(
    dataset_id="us_dd_identity_rejections",
    table_id=SENTINEL_STRIPPED_IDENTITY_FRAGMENT_TABLE_ID,
)
_STALE_STRIPPED_AT = datetime.datetime(2026, 10, 1, 0, 0, 0, tzinfo=datetime.UTC)
_RUN_STRIPPED_AT = datetime.datetime(2026, 10, 28, 0, 0, 0, tzinfo=datetime.UTC)
_UPPER_BOUND_TS = datetime.datetime(
    2026, 10, 1, 0, 0, 0, tzinfo=datetime.UTC
).timestamp()

_STALE_RECORD = SentinelStrippedIdentityFragment(
    tenant=_TENANT,
    ingest_view_name="dd_person",
    upper_bound_date=_UPPER_BOUND_TS,
    prestripped_identity_fragment_id="stale_prestrip_hash",
    stripped_external_ids=(("000000", "US_DD_IDENTITY_PERSON_ID"),),
    surviving_external_ids=(("A", "US_DD_SID"),),
)

_NEW_RECORD = SentinelStrippedIdentityFragment(
    tenant=_TENANT,
    ingest_view_name="dd_person",
    upper_bound_date=_UPPER_BOUND_TS,
    prestripped_identity_fragment_id="new_prestrip_hash",
    stripped_external_ids=(("999999", "US_DD_IDENTITY_PERSON_ID"),),
    surviving_external_ids=(),
)

_PRESTRIPPED_IDS_QUERY = (
    f"SELECT prestripped_identity_fragment_id FROM `{_TABLE_ADDRESS.to_str()}` "
    f"ORDER BY prestripped_identity_fragment_id"
)


class WriteSentinelStrippedFragmentsToBQTest(BigQueryEmulatorTestCase):
    """Tests for WriteSentinelStrippedFragmentsToBQ, run against the BQ emulator.
    Each run replaces the table's rows. The empty-input case is the interesting
    one: WriteToBigQuery clears the table when the run strips nothing, so no
    prior run's rows survive."""

    def setUp(self) -> None:
        # Created per test: the emulator harness wipes all data in tearDown, so
        # tables seeded once at class setup would vanish after the first test.
        super().setUp()
        self.create_mock_table(
            _TABLE_ADDRESS, schema=sentinel_stripped_identity_fragment_schema_fields()
        )

    def _run_write(
        self, stripped_records: list[SentinelStrippedIdentityFragment]
    ) -> None:
        """Runs the transform over the given records against the emulator."""
        with patch(
            f"{write_sentinel_stripped_fragments_to_bq.__name__}.WriteToBigQuery",
            partial(FakeWriteToBigQueryEmulator, test_case=self),
        ):
            test_pipeline = create_test_pipeline()
            _ = (
                test_pipeline
                # Create-then-flatten so an empty input list still runs.
                | beam.Create([stripped_records])
                | beam.FlatMap(lambda records: records)
                | WriteSentinelStrippedFragmentsToBQ(
                    output_dataset=_TABLE_ADDRESS.dataset_id,
                    stripped_at=_RUN_STRIPPED_AT,
                )
            )
            test_pipeline.run()

    def _seed_stale_row(self) -> None:
        self.load_rows_into_table(
            _TABLE_ADDRESS,
            data=[_STALE_RECORD.to_bq_row(stripped_at=_STALE_STRIPPED_AT)],
        )

    def test_clears_stale_rows_when_run_strips_nothing(self) -> None:
        self._seed_stale_row()

        self._run_write([])

        self.run_query_test(_PRESTRIPPED_IDS_QUERY, expected_result=[])

    def test_replaces_stale_rows(self) -> None:
        self._seed_stale_row()

        self._run_write([_NEW_RECORD])

        self.run_query_test(
            _PRESTRIPPED_IDS_QUERY,
            expected_result=[{"prestripped_identity_fragment_id": "new_prestrip_hash"}],
        )
