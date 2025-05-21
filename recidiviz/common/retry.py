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
"""Google API Retry classes and builders"""

from typing import Type

from google.api_core import retry
from google.cloud.bigquery.retry import DEFAULT_RETRY


def default_bq_retry_with_additions(
    *additional_exceptions_for_retry: Type[Exception],
) -> retry.Retry:
    """Adds |additional_exceptions_for_retry| to the default BigQuery retry."""
    additional_retry = retry.if_exception_type(*additional_exceptions_for_retry)
    return DEFAULT_RETRY.with_predicate(
        predicate=lambda exc: additional_retry(exc)
        or DEFAULT_RETRY._predicate(exc),  # pylint: disable=protected-access
    )
