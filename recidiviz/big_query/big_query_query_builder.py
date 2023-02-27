# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Class that provides functionality related to formatting and applying address
overrides to BigQuery queries.
"""

import re
from typing import Any, Dict, Optional

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.utils.string import StrictStringFormatter

PROJECT_ID_KEY = "project_id"


REFERENCED_BQ_ADDRESS_REGEX = re.compile(
    r"`(?P<project_id_clause>\{project_id\}|[\w-]*)\.(?P<dataset_id>[\w-]*)\.(?P<table_id>[\w-]*)`"
)


# The project_id is usually, but not always, used in the query text. The
# BigQueryQueryBuilder class does not know whether it will be used so it always
# makes it available to the query. If the query does not use it, this is not
# indicative of a bug.
_QUERY_FORMATTER = StrictStringFormatter(
    allowed_unused_keywords=frozenset({PROJECT_ID_KEY})
)


class BigQueryQueryBuilder:
    """Class that provides functionality related to formatting and applying address
    overrides to BigQuery queries.
    """

    def __init__(
        self, *, address_overrides: Optional[BigQueryAddressOverrides]
    ) -> None:
        self.address_overrides = address_overrides

    def build_query(
        self,
        *,
        project_id: str,
        query_template: str,
        query_format_kwargs: Dict[str, Any],
    ) -> str:
        """Returns a query built by formatting the provided |query_template| with the
        given |query_format_kwargs| and |project_id|. The resulting query may be used
        as the query in a BigQuery view or run directly via the BigQueryClient.
        """
        query_no_overrides = self._format_query_with_project_id(
            project_id,
            query_template,
            query_format_kwargs,
        )
        if not self.address_overrides:
            return query_no_overrides

        return self._apply_overrides_to_query(
            query_no_overrides, self.address_overrides
        )

    def build_no_project_query_template(
        self, *, query_template: str, query_format_kwargs: Dict[str, Any]
    ) -> str:
        """Builds a query *template* from the provided |query|, which has all provided
        format arguments filled in except the project_id.

        Example:
            query_template: "SELECT * FROM `{project_id}.{dataset}.table`;"
            query_format_kwargs: {"dataset": "my_dataset"}

        Output: "SELECT * FROM `{project_id}.my_dataset.table`;"
        """
        query_template_no_overrides = self._format_query_without_project_id(
            query_template, query_format_kwargs
        )

        if not self.address_overrides:
            return query_template_no_overrides

        return self._apply_overrides_to_query(
            query_template_no_overrides, self.address_overrides
        )

    @classmethod
    def _format_query_without_project_id(
        cls,
        query_template: str,
        query_format_kwargs: Dict[str, Any],
    ) -> str:
        """Formats the given |query_template| string with the given arguments,
        without injecting a value for the PROJECT_ID_KEY.
        """
        cls._check_format_args(query_format_kwargs)
        query_format_args = {
            PROJECT_ID_KEY: f"{{{PROJECT_ID_KEY}}}",
            **query_format_kwargs,
        }

        return _QUERY_FORMATTER.format(query_template, **query_format_args)

    @classmethod
    def _format_query_with_project_id(
        cls,
        project_id: str,
        query_template: str,
        query_format_kwargs: Dict[str, Any],
    ) -> str:
        """This builds the query with the given |query_format_kwargs|, injecting the
        provided |project_id|."""
        cls._check_format_args(query_format_kwargs)

        query_format_kwargs = {
            PROJECT_ID_KEY: project_id,
            **{
                # Query format args may themselves have a project_id format arg - format
                # to inject the project_id here as well.
                key: _QUERY_FORMATTER.format(value, project_id=project_id)
                for key, value in query_format_kwargs.items()
            },
        }

        return _QUERY_FORMATTER.format(query_template, **query_format_kwargs)

    @staticmethod
    def _check_format_args(query_format_kwargs: Dict[str, Any]) -> None:
        """Tests that query format args are valid and are not templates with format args
        themselves (other than project_id).
        """
        for key, arg_value in query_format_kwargs.items():
            try:
                # It doesn't matter which project_id we use here - we're just testing
                # that the project_id is the only format arg in this string.
                _QUERY_FORMATTER.format(arg_value, project_id=None)
            except KeyError as e:
                raise ValueError(
                    f"Query format arg [{key}] is a template with arguments other than "
                    f'project_id: "{arg_value}"'
                ) from e

    @staticmethod
    def _apply_overrides_to_query(
        query: str,
        address_overrides: BigQueryAddressOverrides,
    ) -> str:
        """Takes the given query string, parses out the table references, and returns
        the same query string, but with overrides applied to all relevant addresses.
        """
        query_with_overrides = query
        for project_id_str, dataset_id, table_id in re.findall(
            REFERENCED_BQ_ADDRESS_REGEX, query
        ):
            parent_table = BigQueryAddress(dataset_id=dataset_id, table_id=table_id)
            if override := address_overrides.get_sandbox_address(address=parent_table):
                query_with_overrides = query_with_overrides.replace(
                    f"`{project_id_str}.{dataset_id}.{table_id}`",
                    f"`{project_id_str}.{override.dataset_id}.{override.table_id}`",
                )
        return query_with_overrides
