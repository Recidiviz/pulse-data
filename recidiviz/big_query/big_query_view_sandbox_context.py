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
"""Defines an object that provides a set of address overrides for a view that will be
loaded into a sandbox.
"""
import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_address_formatter import (
    BigQueryAddressFormatterProvider,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode


@attr.define(kw_only=True)
class BigQueryViewSandboxContext:
    """Object that provides a set of address overrides for a *single BigQueryView* that
    will be loaded into a sandbox.
    """

    # The state code that this sandbox should return results for. When set,
    # UnionAllBigQueryViewBuilder views will filter to just parents that are either a)
    # state agnostic views or b) state-specific views for this state.
    state_code_filter: StateCode | None = attr.ib(
        validator=attr_validators.is_opt(StateCode)
    )

    # Address overrides for any parent tables this view may query.
    parent_address_overrides: BigQueryAddressOverrides | None = attr.ib(
        validator=attr_validators.is_opt(BigQueryAddressOverrides)
    )

    # If given, this will give us a formatter that can be used to apply additional
    #  formatting on each parent address in this view.
    parent_address_formatter_provider: BigQueryAddressFormatterProvider | None = (
        attr.ib(validator=attr_validators.is_opt(BigQueryAddressFormatterProvider))
    )

    # The prefix to append to the output dataset this view will be loaded into
    output_sandbox_dataset_prefix: str = attr.ib(validator=attr_validators.is_str)

    def sandbox_view_address(
        self, original_view_address: BigQueryAddress
    ) -> BigQueryAddress:
        """Given a view's original / non-sandbox address, returns the address this view
        should have when loaded into a sandbox.
        """
        return self._sandbox_address(original_view_address)

    def sandbox_materialized_address(
        self, original_materialized_address: BigQueryAddress | None
    ) -> BigQueryAddress | None:
        """Given a view's original / non-sandbox materialized address, returns the
        address this view's materialized table should have when loaded into a sandbox.
        """
        if not original_materialized_address:
            return None
        return self._sandbox_address(original_materialized_address)

    def _sandbox_address(self, original_address: BigQueryAddress) -> BigQueryAddress:
        return BigQueryAddress(
            dataset_id=BigQueryAddressOverrides.format_sandbox_dataset(
                prefix=self.output_sandbox_dataset_prefix,
                dataset_id=original_address.dataset_id,
            ),
            table_id=original_address.table_id,
        )
