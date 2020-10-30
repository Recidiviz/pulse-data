# type: ignore
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
"""
A subclass of PythonOperator to call the IAP request while managing the return response
"""
import os

from airflow.operators.python_operator import PythonOperator
from airflow.utils.decorators import apply_defaults
try:
    from cloud_function_utils import make_iap_request, IAP_CLIENT_ID
except ImportError:
    from recidiviz.cloud_functions.cloud_function_utils import make_iap_request, IAP_CLIENT_ID


def make_iap_export_request(url: str) -> None:
    client_id = IAP_CLIENT_ID[os.environ.get('GCP_PROJECT_ID')]
    # make_iap_request raises an exception if the returned status code is not 200
    make_iap_request(url, client_id)


class IAPHTTPRequestOperator(PythonOperator):
    @apply_defaults
    def __init__(
            self,
            task_id: str,
            url: str,
            *args, **kwargs) -> None:
        super().__init__(task_id=task_id,
                         python_callable=make_iap_export_request,
                         op_kwargs={'url': url},
                         *args, **kwargs)
