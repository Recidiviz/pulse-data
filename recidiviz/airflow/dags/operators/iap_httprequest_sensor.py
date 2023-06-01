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
"""
A subclass of PythonSensor which repeatedly makes an IAP request to an endpoint and succeeds when the response meets the provided criteria
"""
import os
from typing import Any, Callable, Optional

import requests
from airflow.sensors.python import PythonSensor

from recidiviz.cloud_functions.cloud_function_utils import (
    IAP_CLIENT_ID,
    make_iap_request,
)


def request_and_check_condition(
    url: str, response_check: Callable[[requests.Response], bool]
) -> bool:
    client_id = IAP_CLIENT_ID[os.environ["GCP_PROJECT"]]
    # make_iap_request raises an exception if the returned status code is not 200
    response = make_iap_request(url, client_id)

    return response_check(response)


class IAPHTTPRequestSensor(PythonSensor):
    def __init__(
        self,
        task_id: str,
        url: str,
        response_check: Callable[[requests.Response], bool],
        *args: Any,
        url_method: str = "GET",
        data: Optional[bytes] = None,
        **kwargs: Any
    ) -> None:
        super().__init__(
            task_id=task_id,
            python_callable=request_and_check_condition,
            op_kwargs={
                "url": url,
                "response_check": response_check,
                "url_method": url_method,
                "data": data,
            },
            *args,
            **kwargs
        )
