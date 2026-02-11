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
"""Defines interfaces for classes that can be used to format parent address references
in queries. These can be used to apply parent address-specific filters or other sorts of
transformations.
"""

import abc

import attr

from recidiviz.big_query.big_query_address import (
    BigQueryAddress,
    ProjectSpecificBigQueryAddress,
)
from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode


class BigQueryAddressFormatter:
    @abc.abstractmethod
    def format_address(self, address: ProjectSpecificBigQueryAddress) -> str:
        """Given an address to a table or view, returns a formatted string
        that can be used to reference that address in the context of a query.

        For example, in the simplest case, this might return
        "`my-project.my_dataset.my_table`".
        """


class BigQueryAddressFormatterSimple(BigQueryAddressFormatter):
    """The most basic implementation of BigQueryAddressFormatter that just returns an
    address in standard query format: `my-project.my_dataset.my_table`.
    """

    def format_address(self, address: ProjectSpecificBigQueryAddress) -> str:
        return address.format_address_for_query()


@attr.define
class BigQueryAddressFormatterLimitZero(BigQueryAddressFormatter):
    """A BigQueryAddressFormatter that returns a table sub-query that will select zero
    rows from the given address.
    """

    def format_address(self, address: ProjectSpecificBigQueryAddress) -> str:
        return f"(SELECT * FROM {address.format_address_for_query()} LIMIT 0)"


@attr.define
class BigQueryAddressFormatterFilterByStateCode(BigQueryAddressFormatter):
    """A BigQueryAddressFormatter that returns a table sub-query that will select only
    rows with a given state_code value. Should only be used on tables that have a
    state_code column.
    """

    state_code_filter: StateCode = attr.ib(
        validator=attr.validators.instance_of(StateCode)
    )

    def format_address(self, address: ProjectSpecificBigQueryAddress) -> str:
        return (
            f"(SELECT * "
            f"FROM {address.format_address_for_query()} "
            f'WHERE state_code = "{self.state_code_filter.value}")'
        )


class BigQueryAddressFormatterProvider:
    """An interface for getting the appropriate formatter for a given address that will
    be referenced in the context of a query.
    """

    @abc.abstractmethod
    def get_formatter(self, address: BigQueryAddress) -> BigQueryAddressFormatter:
        """Returns the appropriate formatter for the given address. This address should
        be an original address in the standard deployed view graph and not a sandbox
        address.
        """


class SimpleBigQueryAddressFormatterProvider(BigQueryAddressFormatterProvider):
    """The simplest implementation of BigQueryAddressFormatterProvider that will only
    provide a formatter that returns simple table references.
    """

    def get_formatter(self, address: BigQueryAddress) -> BigQueryAddressFormatter:
        return BigQueryAddressFormatterSimple()


class LimitZeroBigQueryAddressFormatterProvider(BigQueryAddressFormatterProvider):
    """An implementation of BigQueryAddressFormatterProvider that will return a
    formatter that selects zero rows from every parent address. Useful for testing
    view compilation without actually reading any data.
    """

    def get_formatter(self, address: BigQueryAddress) -> BigQueryAddressFormatter:
        return BigQueryAddressFormatterLimitZero()


@attr.define
class StateFilteringBigQueryAddressFormatterProvider(BigQueryAddressFormatterProvider):
    """An implementation of BigQueryAddressFormatterProvider that will return a
    formatter that will filter the address down to only rows for the given state code,
    to the best of our ability.
    """

    # Addresses will be formatted to be filtered to only rows for this state at query
    #  time.
    state_code_filter: StateCode = attr.ib(
        validator=attr.validators.instance_of(StateCode)
    )

    # The list of BigQueryAddress that we may encounter which do NOT have a state_code
    # column and cannot be filtered.
    missing_state_code_addresses: set[BigQueryAddress] = attr.ib(
        validator=attr_validators.is_set_of(BigQueryAddress)
    )

    # A map of table address to the pseudocolumns that are available on that table.
    # These are used to ensure that we do not apply a filter on this table because
    # SELECT * would exclude the psuedocolumns which might be referenced elsewhere in
    # the query.
    pseudocolumns_by_address: dict[BigQueryAddress, list[str]] = attr.ib(
        validator=attr_validators.is_dict
    )

    def get_formatter(self, address: BigQueryAddress) -> BigQueryAddressFormatter:
        pseudocolumns = self.pseudocolumns_by_address.get(address, [])
        if pseudocolumns:
            # We cannot do any filtering on this table because a SELECT * query on this
            # table would exclude pseudocolumns like _FILE_NAME which may be referenced
            # elsewhere in the query.
            return BigQueryAddressFormatterSimple()

        address_state_code = address.state_code_for_address()
        if address_state_code:
            if address_state_code != self.state_code_filter:
                # This is a state-specific table for a state other than the one we're
                # filtering to - do not select any rows
                return BigQueryAddressFormatterLimitZero()

            # This table is specific to the state we care about, do no filtering
            return BigQueryAddressFormatterSimple()

        # This is a state-agnostic table but cannot be filtered by state_code - do no
        # filtering.
        if address in self.missing_state_code_addresses:
            return BigQueryAddressFormatterSimple()

        # If this is a state-agnostic table - filter down to rows with the state code
        return BigQueryAddressFormatterFilterByStateCode(
            state_code_filter=self.state_code_filter
        )
