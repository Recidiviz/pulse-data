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
"""Fake implementation of InstanceIngestViewContents that just tracks outstanding
batches.
"""
import datetime
from collections import defaultdict
from math import ceil
from typing import Dict, Iterable, List, Optional

import attr
import numpy as np
import pandas as pd

from recidiviz.big_query.big_query_client import BigQueryClient
from recidiviz.big_query.big_query_results_contents_handle import (
    BigQueryResultsContentsHandle,
)
from recidiviz.cloud_storage.gcs_file_system import generate_random_temp_path
from recidiviz.ingest.direct.ingest_view_materialization.instance_ingest_view_contents import (
    InstanceIngestViewContents,
    ResultsBatchInfo,
)
from recidiviz.ingest.direct.types.direct_ingest_instance import DirectIngestInstance
from recidiviz.tests.big_query.fakes.fake_big_query_client import FakeQueryJob
from recidiviz.utils.types import assert_type


@attr.define
class _MockBatchData:
    batch_info: ResultsBatchInfo
    data_file: Optional[str]
    is_processed: bool


class FakeInstanceIngestViewContents(InstanceIngestViewContents):
    """Fake implementation of InstanceIngestViewContents that just tracks outstanding
    batches.
    """

    def __init__(
        self,
        big_query_client: BigQueryClient,
        region_code: str,
        ingest_instance: DirectIngestInstance,
        dataset_prefix: Optional[str],
    ):
        self._big_query_client = big_query_client
        self._region_code_lower = region_code.lower()
        self._ingest_instance = ingest_instance
        self._dataset_prefix = dataset_prefix
        self._batches_by_view: Dict[str, List[_MockBatchData]] = defaultdict(list)
        self.batch_size = 10

    @property
    def ingest_instance(self) -> DirectIngestInstance:
        return DirectIngestInstance.PRIMARY

    def results_dataset(self) -> str:
        raise ValueError("Unexpected call to results_dataset().")

    @property
    def temp_results_dataset(self) -> str:
        raise ValueError("Unexpected call to temp_results_dataset.")

    def get_max_date_of_data_processed_before_datetime(
        self, datetime_utc: datetime.datetime
    ) -> Dict[str, Optional[datetime.datetime]]:
        raise ValueError(
            "Unexpected call to get_max_date_of_data_processed_before_datetime."
        )

    def save_query_results(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        lower_bound_datetime_exclusive: Optional[datetime.datetime],
        query_str: str,
        order_by_cols_str: str,
        batch_size: int = 100,
    ) -> None:
        raise ValueError("Unexpected call to save_query_results().")

    def get_unprocessed_rows_for_batch(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        batch_number: int,
    ) -> BigQueryResultsContentsHandle:
        if ingest_view_name not in self._batches_by_view:
            raise ValueError(
                f"Unexpected ingest view [{ingest_view_name}] - no batches for this view."
            )
        batch_to_find = ResultsBatchInfo(
            ingest_view_name=ingest_view_name,
            upper_bound_datetime_inclusive=upper_bound_datetime_inclusive,
            batch_number=batch_number,
        )
        for batch_data in self._batches_by_view[ingest_view_name]:
            if batch_data.batch_info != batch_to_find:
                continue
            if not batch_data.data_file:
                raise ValueError(
                    f"Cannot get rows for batch with no associated data path: "
                    f"[{batch_data.batch_info}]"
                )

            query_job = self._fake_query_job_for_batch(batch_data)
            return BigQueryResultsContentsHandle(query_job)

        raise ValueError(f"Did not find data for batch: [{batch_to_find}]")

    @staticmethod
    def _fake_query_job_for_batch(batch_data: _MockBatchData) -> FakeQueryJob:
        def run_query_fn() -> pd.DataFrame:
            return pd.read_csv(batch_data.data_file)

        return FakeQueryJob(run_query_fn=run_query_fn)

    def get_next_unprocessed_batch_info_by_view(
        self,
    ) -> Dict[str, Optional[ResultsBatchInfo]]:
        result = {}
        for ingest_view, batches in self._batches_by_view.items():
            batch_info = None
            for batch_data in batches:
                if not batch_data.is_processed:
                    batch_info = batch_data.batch_info
                    break
            result[ingest_view] = batch_info
        return result

    def mark_rows_as_processed(
        self,
        *,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        batch_number: int,
    ) -> None:
        batch_to_mark_processed = ResultsBatchInfo(
            ingest_view_name=ingest_view_name,
            upper_bound_datetime_inclusive=upper_bound_datetime_inclusive,
            batch_number=batch_number,
        )

        found_batch = False
        for batch_data in self._batches_by_view[ingest_view_name]:
            if batch_to_mark_processed == batch_data.batch_info:
                batch_data.is_processed = True
                found_batch = True
        if not found_batch:
            raise ValueError(f"Could not find batch [{batch_to_mark_processed}]")

    # TEST ONLY FUNCTIONALITY BELOW #
    def test_add_batch(self, batch: ResultsBatchInfo) -> None:
        """Registers a batch with no backing data. Should be used in tests that do not
        try to read / operate on batch data.
        """
        self._add_batch(batch, data_file=None)

    def test_add_batches_for_data(
        self,
        ingest_view_name: str,
        upper_bound_datetime_inclusive: datetime.datetime,
        data_local_path: str,
    ) -> None:
        """Splits up the data in the file at |data_local_path| into the appropriate
        number of batches based on this class' batch size."""
        full_df = pd.read_csv(data_local_path)
        num_data_rows = len(full_df.index)
        if num_data_rows == 0:
            return
        num_batches = ceil(num_data_rows / self.batch_size)
        split_dfs = np.array_split(full_df, num_batches)
        batch_number = 0
        for df in split_dfs:
            output_path = generate_random_temp_path()
            assert_type(df, pd.DataFrame).to_csv(output_path, index=False)
            self._add_batch(
                batch=ResultsBatchInfo(
                    ingest_view_name=ingest_view_name,
                    upper_bound_datetime_inclusive=upper_bound_datetime_inclusive,
                    batch_number=batch_number,
                ),
                data_file=output_path,
            )
            batch_number += 1

    def _add_batch(self, batch: ResultsBatchInfo, data_file: Optional[str]) -> None:
        self._batches_by_view[batch.ingest_view_name].append(
            _MockBatchData(batch, data_file, is_processed=False)
        )
        self._batches_by_view[batch.ingest_view_name].sort(
            key=lambda info: (
                info.batch_info.upper_bound_datetime_inclusive,
                info.batch_info.batch_number,
            )
        )

    def test_clear_data(self) -> None:
        """Clear all batches from the contents cache. Mimics what might happen to
        when data is invalidated.
        """
        self._batches_by_view.clear()

    def test_get_batches(self) -> Iterable[_MockBatchData]:
        """Returns all batch data in the contents cache."""
        return [
            batch_data
            for batch_data_list in self._batches_by_view.values()
            for batch_data in batch_data_list
        ]
