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
"""Tests the recidiviz/tools/deploy/update_raw_date_table_schemas.py file that issues updates to raw regional schemas"""

import logging
import os
from collections import namedtuple
from typing import Any, Dict, List, Tuple

import mock
import pytest

from recidiviz.ingest.direct import raw_data_table_schema_utils
from recidiviz.tools.deploy.update_raw_data_table_schemas import (
    ONE_DAY_MS,
    create_states_raw_data_datasets_if_necessary,
    update_raw_data_table_schemas,
)


@pytest.fixture(name="caplog_info")
def caplog_info_fixture(caplog: Any) -> Any:
    caplog.set_level(logging.INFO)
    return caplog


def test_create_datasets_if_necessary(caplog_info: Any) -> None:
    StateCode = namedtuple("StateCode", ["value"])
    mock_bq_client = mock.MagicMock()
    mock_bq_client.dataset_ref_for_id.side_effect = lambda x: x
    create_states_raw_data_datasets_if_necessary(
        state_codes=[
            StateCode(value="US_XX"),
            StateCode(value="US_YY"),
            StateCode(value="US_ZZ"),
        ],
        bq_client=mock_bq_client,
    )
    assert mock_bq_client.dataset_ref_for_id.mock_calls == [
        mock.call("us_xx_raw_data"),
        mock.call("pruning_us_xx_new_raw_data_primary"),
        mock.call("pruning_us_xx_raw_data_diff_results_primary"),
        mock.call("us_xx_raw_data_secondary"),
        mock.call("pruning_us_xx_new_raw_data_secondary"),
        mock.call("pruning_us_xx_raw_data_diff_results_secondary"),
        mock.call("us_yy_raw_data"),
        mock.call("pruning_us_yy_new_raw_data_primary"),
        mock.call("pruning_us_yy_raw_data_diff_results_primary"),
        mock.call("us_yy_raw_data_secondary"),
        mock.call("pruning_us_yy_new_raw_data_secondary"),
        mock.call("pruning_us_yy_raw_data_diff_results_secondary"),
        mock.call("us_zz_raw_data"),
        mock.call("pruning_us_zz_new_raw_data_primary"),
        mock.call("pruning_us_zz_raw_data_diff_results_primary"),
        mock.call("us_zz_raw_data_secondary"),
        mock.call("pruning_us_zz_new_raw_data_secondary"),
        mock.call("pruning_us_zz_raw_data_diff_results_secondary"),
    ]
    assert mock_bq_client.create_dataset_if_necessary.mock_calls == [
        mock.call("us_xx_raw_data", None),
        mock.call("pruning_us_xx_new_raw_data_primary", ONE_DAY_MS),
        mock.call("pruning_us_xx_raw_data_diff_results_primary", ONE_DAY_MS),
        mock.call("us_xx_raw_data_secondary", None),
        mock.call("pruning_us_xx_new_raw_data_secondary", ONE_DAY_MS),
        mock.call("pruning_us_xx_raw_data_diff_results_secondary", ONE_DAY_MS),
        mock.call("us_yy_raw_data", None),
        mock.call("pruning_us_yy_new_raw_data_primary", ONE_DAY_MS),
        mock.call("pruning_us_yy_raw_data_diff_results_primary", ONE_DAY_MS),
        mock.call("us_yy_raw_data_secondary", None),
        mock.call("pruning_us_yy_new_raw_data_secondary", ONE_DAY_MS),
        mock.call("pruning_us_yy_raw_data_diff_results_secondary", ONE_DAY_MS),
        mock.call("us_zz_raw_data", None),
        mock.call("pruning_us_zz_new_raw_data_primary", ONE_DAY_MS),
        mock.call("pruning_us_zz_raw_data_diff_results_primary", ONE_DAY_MS),
        mock.call("us_zz_raw_data_secondary", None),
        mock.call("pruning_us_zz_new_raw_data_secondary", ONE_DAY_MS),
        mock.call("pruning_us_zz_raw_data_diff_results_secondary", ONE_DAY_MS),
    ]
    assert caplog_info.record_tuples == [
        ("root", logging.INFO, "Creating raw data datasets (if necessary)..."),
    ]


@mock.patch("recidiviz.tools.utils.script_helpers.prompt_for_confirmation")
@mock.patch(
    "recidiviz.tools.deploy.update_raw_data_table_schemas.map_fn_with_progress_bar_results"
)
def test_update_raw_data_table_schemas_parallel_loop_retry_integration(
    mock_map_fn: mock.MagicMock,
    prompt_mock: mock.MagicMock,
    caplog_info: Any,
    tmpdir: Any,
) -> None:
    def mock_map_fn_result(
        *, kwargs_list: List[Dict[str, Any]], **_: str
    ) -> Tuple[
        List[Tuple[Any, Dict[str, Any]]], List[Tuple[Exception, Dict[str, Any]]]
    ]:
        successes = []
        exceptions = []
        for i, kwargs in enumerate(kwargs_list):
            if i % 2 == 0:
                successes.append((None, kwargs))
            else:
                logging.info("Exception!")
                e: Exception = TimeoutError("Time's up")
                exceptions.append((e, kwargs))
        return (successes, exceptions)

    def mock_call(kwargs_list: List[Dict[str, Any]]) -> Any:
        return mock.call(
            fn=raw_data_table_schema_utils.update_raw_data_table_schema,
            kwargs_list=kwargs_list,
            max_workers=64,
            timeout=600,
            progress_bar_message="Updating raw table schemas...",
        )

    mock_map_hard_fail_first = iter(
        (
            lambda *, kwargs_list, **_: ([], []),
            mock_map_fn_result,
            mock_map_fn_result,
            mock_map_fn_result,
        )
    )
    # pylint: disable=unnecessary-lambda
    # pylint is not sophisticated enough to
    # understand the lambda is necessary to wrap
    # next() such that different functions can be returned
    # on different invocations
    mock_map_fn.side_effect = lambda **kwargs: next(mock_map_hard_fail_first)(**kwargs)
    prompt_mock.return_value = True
    kwargs_list = [
        {"state_code": "US_XX", "raw_file_tag": "tagzero"},
        {"state_code": "US_XX", "raw_file_tag": "tagone"},
        {"state_code": "US_YY", "raw_file_tag": "tagtwo"},
        {"state_code": "US_ZZ", "raw_file_tag": "tagthree"},
    ]
    update_raw_data_table_schemas(
        file_kwargs=kwargs_list,
        log_path=os.path.join(str(tmpdir), "test_log.log"),
    )
    assert mock_map_fn.mock_calls == [
        mock_call(
            kwargs_list=mock_kwargs_list,
        )
        for mock_kwargs_list in [
            [
                {"state_code": "US_XX", "raw_file_tag": "tagzero"},
                {"state_code": "US_XX", "raw_file_tag": "tagone"},
                {"state_code": "US_YY", "raw_file_tag": "tagtwo"},
                {"state_code": "US_ZZ", "raw_file_tag": "tagthree"},
            ],
            [
                {"state_code": "US_XX", "raw_file_tag": "tagzero"},
                {"state_code": "US_XX", "raw_file_tag": "tagone"},
                {"state_code": "US_YY", "raw_file_tag": "tagtwo"},
                {"state_code": "US_ZZ", "raw_file_tag": "tagthree"},
            ],
            [
                {"state_code": "US_XX", "raw_file_tag": "tagone"},
                {"state_code": "US_ZZ", "raw_file_tag": "tagthree"},
            ],
            [
                {"state_code": "US_ZZ", "raw_file_tag": "tagthree"},
            ],
        ]
    ]
    assert caplog_info.record_tuples == [
        ("root", logging.INFO, f"Writing logs to {tmpdir}/test_log.log"),
        ("root", logging.ERROR, "Some results are not accounted for"),
        ("root", logging.INFO, "Exception!"),
        ("root", logging.INFO, "Exception!"),
        ("root", logging.WARNING, "These tasks failed with the following exceptions:"),
        (
            "root",
            logging.WARNING,
            ("Time's up    {'state_code': 'US_XX', 'raw_file_tag': 'tagone'}"),
        ),
        (
            "root",
            logging.WARNING,
            ("Time's up    {'state_code': 'US_ZZ', 'raw_file_tag': 'tagthree'}"),
        ),
        ("root", logging.INFO, "Exception!"),
        ("root", logging.WARNING, "These tasks failed with the following exceptions:"),
        (
            "root",
            logging.WARNING,
            ("Time's up    {'state_code': 'US_ZZ', 'raw_file_tag': 'tagthree'}"),
        ),
        ("root", logging.INFO, "All tasks complete"),
    ]
    assert tmpdir.join("test_log.log").readlines() == [
        "Exception!\n",
        "Exception!\n",
        "Exception!\n",
    ]
