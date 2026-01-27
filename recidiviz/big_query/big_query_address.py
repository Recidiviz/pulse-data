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
"""Defines a type that represents the (dataset_id, table_id) address of a BigQuery view
or table.
"""
import re
from typing import Iterable

import attr
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference, TableReference
from more_itertools import one

from recidiviz.big_query.big_query_job_labels import (
    BigQueryAddressJobLabel,
    BigQueryDatasetIdJobLabel,
    BigQueryTableIdJobLabel,
)
from recidiviz.cloud_resources.platform_resource_labels import (
    StateAgnosticResourceLabel,
    StateCodeResourceLabel,
)
from recidiviz.cloud_resources.resource_label import ResourceLabel
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.common.google_cloud.utils import format_resource_label


@attr.s(frozen=True, kw_only=True, order=True)
class BigQueryAddress:
    """Represents the (dataset_id, table_id) address of a BigQuery view or table."""

    dataset_id: str = attr.ib(validator=attr_validators.is_str)
    table_id: str = attr.ib(validator=attr_validators.is_str)

    @classmethod
    def from_str(cls, address_str: str) -> "BigQueryAddress":
        """Converts a string in the format 'dataset.table' to BigQueryAddress."""
        parts = address_str.split(".")
        if len(parts) != 2 or not all(parts):
            raise ValueError("Input must be in the format 'dataset.table'.")
        return cls(dataset_id=parts[0], table_id=parts[1])

    @classmethod
    def from_table(
        cls,
        table: (
            bigquery.table.Table
            | bigquery.table.TableReference
            | bigquery.table.TableListItem
        ),
    ) -> "BigQueryAddress":
        return cls(dataset_id=table.dataset_id, table_id=table.table_id)

    def to_str(self) -> str:
        return f"{self.dataset_id}.{self.table_id}"

    def select_query_template(self, select_statement: str = "SELECT *") -> str:
        if not select_statement.lstrip().startswith("SELECT"):
            raise ValueError(
                f"Any custom select_statement must start with SELECT. Attempting to "
                f"build a SELECT query for [{self.to_str()}] with statement "
                f"[{select_statement}]"
            )
        return f"{select_statement} FROM `{self.format_address_for_query_template()}`"

    def format_address_for_query_template(self) -> str:
        return f"{{project_id}}.{self.dataset_id}.{self.table_id}"

    def to_project_specific_address(
        self, project_id: str
    ) -> "ProjectSpecificBigQueryAddress":
        return ProjectSpecificBigQueryAddress(
            project_id=project_id,
            dataset_id=self.dataset_id,
            table_id=self.table_id,
        )

    def state_code_for_address(self) -> StateCode | None:
        """Returns the StateCode associated with this address if this is a
        state-specific address, otherwise returns None.
        """
        found_state_codes = set()
        for s in [self.dataset_id, self.table_id]:
            s_lower = s.lower()

            if match := re.match("^(?P<state>us_[a-z]{2})_.*$", s_lower):
                found_state_codes.add(StateCode(match.group("state").upper()))
            if match := re.match("^.*_(?P<state>us_[a-z]{2})$", s_lower):
                found_state_codes.add(StateCode(match.group("state").upper()))

        if not found_state_codes:
            return None

        if len(found_state_codes) > 1:
            raise ValueError(
                f"Found more than one state code referenced by address "
                f"{self.to_str()}: {sorted({s.value for s in found_state_codes})}"
            )
        return one(found_state_codes)

    def is_state_specific_address(self) -> bool:
        """Returns true if either of the dataset_id or table_id starts with a state code
        prefix ('us_xx_') or ends with a state code suffix ('_us_xx').
        """
        return bool(self.state_code_for_address())

    @staticmethod
    def addresses_to_str(
        addresses: Iterable["BigQueryAddress"], *, indent_level: int = 0
    ) -> str:
        """Formats the collection of addresses as a sorted, bulleted, multi-line string.
        If provided, the |indent_level| indicates the number of spaces each line of the
        list will be indented by.
        """
        indent_str = " " * indent_level
        if not addresses:
            return ""
        list_str = "\n".join(
            f"{indent_str}* {a.to_str()}"
            for a in sorted(addresses, key=lambda a: (a.dataset_id, a.table_id))
        )
        return f"\n{list_str}\n"

    @property
    def bq_job_labels(self) -> list[ResourceLabel]:
        state_code = self.state_code_for_address()
        state_code_label = (
            StateCodeResourceLabel(value=state_code.value.lower())
            if state_code
            else StateAgnosticResourceLabel()
        )
        dataset_id_label_safe = format_resource_label(self.dataset_id)
        table_id_label_safe = format_resource_label(self.table_id)
        address_label_safe = format_resource_label(
            f"{dataset_id_label_safe}---{table_id_label_safe}"
        )
        return [
            BigQueryDatasetIdJobLabel(value=dataset_id_label_safe),
            BigQueryTableIdJobLabel(value=table_id_label_safe),
            BigQueryAddressJobLabel(value=address_label_safe),
            state_code_label,
        ]


@attr.s(frozen=True, kw_only=True, order=True)
class ProjectSpecificBigQueryAddress:
    """Represents the (dataset_id, table_id) address of a BigQuery view or table that
    lives in a spefific project.
    """

    project_id: str = attr.ib(validator=attr_validators.is_str)
    dataset_id: str = attr.ib(validator=attr_validators.is_str)
    table_id: str = attr.ib(validator=attr_validators.is_str)

    @classmethod
    def from_list_item(
        cls, table: bigquery.table.TableListItem
    ) -> "ProjectSpecificBigQueryAddress":
        return cls(
            project_id=table.project,
            dataset_id=table.dataset_id,
            table_id=table.table_id,
        )

    @classmethod
    def from_table_reference(
        cls, table_ref: TableReference
    ) -> "ProjectSpecificBigQueryAddress":
        return cls(
            project_id=table_ref.project,
            dataset_id=table_ref.dataset_id,
            table_id=table_ref.table_id,
        )

    def to_project_agnostic_address(self) -> BigQueryAddress:
        return BigQueryAddress(dataset_id=self.dataset_id, table_id=self.table_id)

    def to_str(self) -> str:
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    def format_address_for_query(self) -> str:
        return f"`{self.to_str()}`"

    def select_query(self, select_statement: str = "SELECT *") -> str:
        if not select_statement.startswith("SELECT"):
            raise ValueError(
                f"Any custom select_statement must start with SELECT. Attempting to "
                f"build a SELECT query for [{self.to_str()}] with statement "
                f"[{select_statement}]"
            )
        return f"{select_statement} FROM {self.format_address_for_query()}"

    @property
    def dataset_reference(self) -> DatasetReference:
        return DatasetReference.from_string(
            dataset_id=self.dataset_id, default_project=self.project_id
        )

    @property
    def table_reference(self) -> TableReference:
        return self.dataset_reference.table(self.table_id)
