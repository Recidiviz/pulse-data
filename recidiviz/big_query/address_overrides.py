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
"""Class that provides a mapping of table/view addresses to the address they should be
replaced with when operating on any views / view queries.
"""
import uuid
from typing import Dict, Optional
from uuid import UUID

from recidiviz.big_query.big_query_address import BigQueryAddress


def _validate_no_conflicting_overrides(
    full_dataset_overrides: Dict[str, str],
    address_overrides: Dict[BigQueryAddress, BigQueryAddress],
) -> None:
    """Checks that the |full_dataset_overrides| and |address_overrides| contain
    distinct, non-conflicting overrides.
    """
    full_dataset_original_ids = set(full_dataset_overrides.keys())
    address_override_original_dataset_ids = {
        a.dataset_id for a in address_overrides.keys()
    }

    if overlapping_dataset_ids := full_dataset_original_ids.intersection(
        address_override_original_dataset_ids
    ):
        raise ValueError(
            f"Found original addresses with overrides defined in both "
            f"|address_overrides| and |full_dataset_overrides|: "
            f"{overlapping_dataset_ids}"
        )


# Variable private to this file which allows us to enforce that the
# BigQueryAddressOverrides constructor is only called from the
# BigQueryAddressOverrides.Builder. Idea adapted from
# https://stackoverflow.com/a/46459300.
_internal_only_create_key: UUID = uuid.uuid4()


class BigQueryAddressOverrides:
    """Provides a mapping of table/view addresses to the address they should be
    replaced with when operating on any views / view queries.
    """

    def __init__(
        self,
        *,
        full_dataset_overrides: Dict[str, str],
        address_overrides: Dict[BigQueryAddress, BigQueryAddress],
        create_key: UUID,
    ):
        """Constructor for BigQueryAddressOverrides. This should only be called from
        the Builder.build() function.

        Args:
            full_dataset_overrides: A map of dataset_ids and the dataset all views in
                that dataset should be written to instead of the original dataset.
            address_overrides: A map of view addresses and the address that view should
                be written to instead of the original address.
            create_key: A unique identifier that only the Builder has access to -
               allows us to enforce that all BigQueryAddressOverrides are created with
               a Builder.

        The |full_dataset_overrides| and |address_overrides| are checked for
        non-conflicting information. If any of the original datasets or the sandbox
        datasets overlap, this constructor will throw.
        """
        if create_key != _internal_only_create_key:
            raise ValueError(
                f"The create_key [{create_key}] does not match expected "
                f"[{_internal_only_create_key}]. Use BigQueryAddressOverrides.Builder "
                f"instead to create a BigQueryAddressOverrides object."
            )
        _validate_no_conflicting_overrides(full_dataset_overrides, address_overrides)
        self._full_dataset_overrides: Dict[str, str] = full_dataset_overrides
        self._address_overrides: Dict[
            BigQueryAddress, BigQueryAddress
        ] = address_overrides

    def get_sandbox_address(
        self, address: BigQueryAddress
    ) -> Optional[BigQueryAddress]:
        """Returns the overridden sandbox address for a view / table normally written to
        |address|.
        """
        if address.dataset_id in self._full_dataset_overrides:
            return BigQueryAddress(
                dataset_id=self._full_dataset_overrides[address.dataset_id],
                table_id=address.table_id,
            )
        return self._address_overrides.get(address, None)

    def get_dataset(self, dataset: str) -> str:
        """If the dataset has been fully overridden, returns the new dataset id.
        Otherwise, returns the original.
        """
        return self._full_dataset_overrides.get(dataset, dataset)

    def get_full_dataset_overrides_dict(self) -> dict[str, str]:
        return self._full_dataset_overrides

    def get_address_overrides_dict(self) -> dict[BigQueryAddress, BigQueryAddress]:
        return self._address_overrides

    @classmethod
    def merge(
        cls,
        overrides_1: "BigQueryAddressOverrides",
        overrides_2: "BigQueryAddressOverrides",
    ) -> "BigQueryAddressOverrides":
        """Merges two sets of overrides into a single object, throwing if either provide
        conflicting overrides.
        """
        builder = overrides_1.to_builder(sandbox_prefix=None)
        for (
            dataset,
            override_dataset,
        ) in overrides_2.get_full_dataset_overrides_dict().items():
            builder.register_custom_dataset_override(dataset, override_dataset)

        for (
            address,
            override_address,
        ) in overrides_2.get_address_overrides_dict().items():
            builder.register_custom_sandbox_override_for_address(
                address, override_address
            )

        return builder.build()

    def to_builder(
        self, sandbox_prefix: str | None
    ) -> "BigQueryAddressOverrides.Builder":
        return self.Builder(
            sandbox_prefix=sandbox_prefix,
            full_dataset_overrides=self._full_dataset_overrides,
            address_overrides=self._address_overrides,
        )

    class Builder:
        """Builder for BigQueryAddressOverrides objects."""

        def __init__(
            self,
            sandbox_prefix: Optional[str],
            full_dataset_overrides: Optional[Dict[str, str]] = None,
            address_overrides: Optional[Dict[BigQueryAddress, BigQueryAddress]] = None,
        ):
            """Constructor for BigQueryAddressOverrides. This should only be called from
            the Builder.build() function.

            Args:
                sandbox_prefix: The prefix that will be appended to all non-custom
                    overrides registered with this Builder. May be null if this builder
                    will be only used for custom overrides.
                full_dataset_overrides: A map of dataset_ids and the dataset all views in
                    that dataset should be written to instead of the original dataset.
                    This should not be used outside of the
                    BigQueryAddressOverrides.to_builder() function.
                address_overrides: A map of view addresses and the address that view
                    should be written to instead of the original address. This should
                    not be used outside of the BigQueryAddressOverrides.to_builder()
                    function.

            The |full_dataset_overrides| and |address_overrides| are checked for
            non-conflicting information. If any of the original datasets or the sandbox
            datasets overlap, this constructor will throw.
            """

            self._sandbox_prefix_opt = sandbox_prefix
            self._full_dataset_overrides: Dict[str, str] = full_dataset_overrides or {}
            self._address_overrides: Dict[BigQueryAddress, BigQueryAddress] = (
                address_overrides or {}
            )
            _validate_no_conflicting_overrides(
                self._full_dataset_overrides, self._address_overrides
            )

        @property
        def _sandbox_prefix(self) -> str:
            if self._sandbox_prefix_opt is None:
                raise ValueError(
                    "Found null sandbox prefix - this builder can only be "
                    "used for custom overrides that do not require a sandbox prefix."
                )
            return self._sandbox_prefix_opt

        def build(self) -> "BigQueryAddressOverrides":
            return BigQueryAddressOverrides(
                full_dataset_overrides=self._full_dataset_overrides,
                address_overrides=self._address_overrides,
                create_key=_internal_only_create_key,
            )

        def register_sandbox_override_for_address(
            self, address: BigQueryAddress
        ) -> "BigQueryAddressOverrides.Builder":
            """Registers an address override for the view/table at the provided |address|.
            The format of the dataset of the sandbox address is
            '<sandbox_prefix>_<dataset_id>'. If this view / table is referenced by any
            views being deployed, the sandbox address will be referenced instead of the
            original address.
            """
            sandbox_address = BigQueryAddress(
                dataset_id=BigQueryAddressOverrides.format_sandbox_dataset(
                    self._sandbox_prefix, address.dataset_id
                ),
                table_id=address.table_id,
            )
            self.register_custom_sandbox_override_for_address(address, sandbox_address)
            return self

        def register_custom_sandbox_override_for_address(
            self, address: BigQueryAddress, sandbox_address: BigQueryAddress
        ) -> "BigQueryAddressOverrides.Builder":
            """Registers an address override for the view/table at the provided
            |address|. If this view / table is referenced by any
            views being deployed, the |sandbox_address| will be referenced instead of
            the original address.
            """
            if (
                address in self._address_overrides
                and self._address_overrides[address] != sandbox_address
            ):
                raise ValueError(
                    f"Address [{address.to_str()}] already has conflicting override "
                    f"set: [{self._address_overrides[address].to_str()}]"
                )
            if address.dataset_id in self._full_dataset_overrides:
                existing_sandbox_address = BigQueryAddress(
                    dataset_id=self._full_dataset_overrides[address.dataset_id],
                    table_id=address.table_id,
                )
                if sandbox_address == existing_sandbox_address:
                    # This override is already set at the dataset level, do not
                    # explicitly set an override.
                    return self

                raise ValueError(
                    f"Dataset [{address.dataset_id}] for address [{address.to_str()}] "
                    f"already has conflicting full dataset override set: "
                    f"[{self._full_dataset_overrides[address.dataset_id]}]"
                )
            self._address_overrides[address] = sandbox_address
            return self

        def register_sandbox_override_for_entire_dataset(
            self, dataset_id: str
        ) -> "BigQueryAddressOverrides.Builder":
            """Registers an address overrides for all views/tables in |dataset_id|.
            If any of the views / tables in this dataset are referenced by any views being
            deployed, the sandbox address will be used instead of the original
            address. The format of the sandbox address dataset is
            '<sandbox_prefix>_<dataset_id>'.
            """

            sandbox_dataset_id = BigQueryAddressOverrides.format_sandbox_dataset(
                self._sandbox_prefix, dataset_id
            )
            self.register_custom_dataset_override(
                dataset_id, sandbox_dataset_id, force_allow_custom=True
            )
            return self

        def register_custom_dataset_override(
            self,
            original_dataset_id: str,
            new_dataset_id: str,
            force_allow_custom: bool = False,
        ) -> "BigQueryAddressOverrides.Builder":
            """Registers an address overrides for all views/tables in
            |original_dataset_id|. If any of the views / tables in this dataset are
            referenced by any views being deployed, the provided |new_dataset_id| will
            be used instead. This is used when the |new_dataset_id| value is different
            than the standard '<sandbox_prefix>_<dataset_id>' value.

            If |force_allow_custom| is True, the custom override is allowed even if
            |new_dataset_id| is the same as the standard override for this builder.
            Otherwise, if |new_dataset_id| is the same as the standard override, this
            function throws.
            """
            if (
                original_dataset_id in self._full_dataset_overrides
                and self._full_dataset_overrides[original_dataset_id] != new_dataset_id
            ):
                raise ValueError(
                    f"Dataset [{original_dataset_id}] already has override set: "
                    f"[{self._full_dataset_overrides[original_dataset_id]}]"
                )

            if (
                self._sandbox_prefix_opt
                and not force_allow_custom
                and new_dataset_id
                == BigQueryAddressOverrides.format_sandbox_dataset(
                    self._sandbox_prefix, original_dataset_id
                )
            ):
                raise ValueError(
                    f"The new_dataset_id [{new_dataset_id}] matches the standard sandbox "
                    f"override for original_dataset_id [{original_dataset_id}]. Should "
                    f"use register_sandbox_override_for_entire_dataset to set a "
                    f"standard sandbox override for a dataset."
                )

            overlapping_address_overrides = self._get_overlapping_address_overrides(
                original_dataset_id
            )

            # If any of the overlapping address-level overrides have a sandbox address
            # that matches the address that would be derived from this dataset-level
            # override it's ok. However, if the sandbox address is different we have
            # found a conflict and need to throw.
            conflicting_overrides = {
                a: self._address_overrides[a]
                for a in overlapping_address_overrides
                if self._address_overrides[a].dataset_id != new_dataset_id
            }
            if conflicting_overrides:
                raise ValueError(
                    f"Found conflicting address overrides already set for addresses in "
                    f"[{original_dataset_id}]: {conflicting_overrides}."
                )

            # If the overlapping address-level overrides are non-conflicting, we can
            # just remove them in favor of the new dataset-level override.
            for a in overlapping_address_overrides:
                self._address_overrides.pop(a)

            self._full_dataset_overrides[original_dataset_id] = new_dataset_id
            return self

        def _get_overlapping_address_overrides(
            self, dataset_to_override: str
        ) -> set[BigQueryAddress]:
            """Returns any already-registered address-level overrides that share an
            original dataset with the provided dataset that we want to override.
            """
            return {
                a
                for a in self._address_overrides.keys()
                if a.dataset_id == dataset_to_override
            }

    @classmethod
    def empty(cls) -> "BigQueryAddressOverrides":
        return BigQueryAddressOverrides(
            full_dataset_overrides={},
            address_overrides={},
            create_key=_internal_only_create_key,
        )

    @staticmethod
    def format_sandbox_dataset(prefix: str, dataset_id: str) -> str:
        return f"{prefix}_{dataset_id}"
