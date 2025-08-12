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
"""Defines an object that provides a set of address overrides for a *collection of views* 
that will be loaded into a sandbox by the view update.
"""
import attr

from recidiviz.big_query.address_overrides import BigQueryAddressOverrides
from recidiviz.big_query.big_query_address_formatter import (
    BigQueryAddressFormatterProvider,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode


@attr.define(kw_only=True)
class BigQueryViewUpdateSandboxContext:
    """Object that provides a set of address overrides for a *collection of views* that
    will be loaded into a sandbox.
    """

    # The state code that this sandbox should return results for. When set,
    # UnionAllBigQueryViewBuilder views will filter to just parents that are either a)
    # state agnostic views or b) state-specific views for this state.
    state_code_filter: StateCode | None = attr.ib(
        validator=attr_validators.is_opt(StateCode)
    )

    # Address overrides for any parent source tables views in this view update may
    # query. May be empty (BigQueryAddressOverrides.empty()) if the update should read
    # from only deployed source tables.
    input_source_table_overrides: BigQueryAddressOverrides = attr.ib(
        validator=attr.validators.instance_of(BigQueryAddressOverrides)
    )

    # The prefix to append to the output dataset for any views loaded by this view
    # update.
    output_sandbox_dataset_prefix: str = attr.ib(validator=attr_validators.is_str)

    # If given, this will give us a formatter that can be used to apply additional
    #  formatting on each parent address of views loaded by this view update.
    parent_address_formatter_provider: BigQueryAddressFormatterProvider | None = (
        attr.ib(validator=attr_validators.is_opt(BigQueryAddressFormatterProvider))
    )
