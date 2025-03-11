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
"""Product config classes for exporting metric views to Google Cloud Storage."""
import os
from typing import List, Optional, Set, TypedDict

import attr

from recidiviz.common import attr_validators
from recidiviz.common.constants.states import StateCode
from recidiviz.metrics.export import products as products_module
from recidiviz.utils import environment, metadata
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION, GCPEnvironment
from recidiviz.utils.types import assert_type
from recidiviz.utils.yaml_dict import YAMLDict

PRODUCTS_CONFIG_PATH = os.path.join(
    os.path.dirname(products_module.__file__),
    "products.yaml",
)

ProductName = str


class BadProductExportSpecificationError(ValueError):
    pass


def in_configured_export_environment(configured_environment: str) -> bool:
    """Returns whether or not the configured_environment matches the current
    environment. If we are in prod, then the configured_environment must be
    production. Everything is configured if we are in staging."""
    configured_project_id = environment.get_project_for_environment(
        GCPEnvironment(configured_environment)
    )
    return (
        not metadata.project_id() == GCP_PROJECT_PRODUCTION
        or configured_project_id == metadata.project_id()
    )


@attr.s
class ProductStateConfig:
    """Stores a product's status information for a given state"""

    # State code
    state_code: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # Product launch environment for this specific state
    environment: str = attr.ib(validator=attr_validators.is_non_empty_str)

    def is_state_launched_in_env(self) -> bool:
        """Returns true if the product can be launched in the given state in the current
         environment.

        If we are in prod, the product config must be explicitly set to specify
        this product can be launched in prod. All exports can be triggered to run in
        staging.
        """
        return in_configured_export_environment(self.environment)


@attr.s
class ProductConfig:
    """Stores information about a product, parsed from the product config file"""

    # Product name
    name: ProductName = attr.ib(validator=attr_validators.is_non_empty_str)
    # Product description
    description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    # List of export names required for this product
    exports: List[str] = attr.ib(validator=attr_validators.is_list)
    # Product launch environment, only for state agnostic products
    environment: Optional[str] = attr.ib(validator=attr_validators.is_opt_str)
    # List of relevant state status information for a product. None if product is
    # state-agnostic.
    states: Optional[List[ProductStateConfig]] = attr.ib(
        validator=attr_validators.is_opt_list
    )
    # Identifies product as state agnostic.
    is_state_agnostic: Optional[bool] = attr.ib(
        validator=attr_validators.is_bool, default=False
    )

    def __attrs_post_init__(self) -> None:
        if self.states and self.environment is not None:
            raise ValueError(
                f"Product with name: [{self.name}] has relevant state statuses."
                f" Environment information should be set at the state level, not the"
                f" product level."
            )
        if self.is_state_agnostic and self.states is not None:
            raise ValueError(
                f"Product with name: [{self.name}] is state agnostic"
                f" but contains enabled states: [{self.states}] in its config."
                f" Check the product config."
            )
        if not self.is_state_agnostic and self.states is None:
            raise ValueError(
                f"Product with name: [{self.name}] is not state agnostic"
                f" but does not contain enabled states in its config."
                f" Check the product config."
            )

    @property
    def launched_states(self) -> Optional[Set[str]]:
        return (
            None
            if self.states is None
            else {
                state.state_code.upper()
                for state in self.states
                if state.is_state_launched_in_env()
            }
        )


class ProductExportConfig(TypedDict):
    export_job_name: str
    state_code: Optional[str]


@attr.s
class ProductConfigs:
    """Loads product configs from file, stores them, and finds a product by job name and state code"""

    products: List[ProductConfig] = attr.ib()

    def get_export_configs_for_job_filter(
        self, export_job_filter: str
    ) -> List[ProductExportConfig]:
        """Returns the export configs for the given export_job_filter,
        which can be either state_code or export job name."""
        filter_uppercase = export_job_filter.upper()
        if StateCode.is_state_code(filter_uppercase):
            return [
                export
                for export in self.get_all_export_configs()
                if export["state_code"] == filter_uppercase
            ]
        return [
            export
            for export in self.get_all_export_configs()
            if export["export_job_name"] == filter_uppercase
        ]

    def get_product_agnostic_export_configs(self) -> List[ProductExportConfig]:
        return [
            export
            for export in self.get_all_export_configs()
            if export["state_code"] is None
        ]

    def get_states_with_export_enabled_in_any_env(
        self, export_job_name: str
    ) -> set[str]:
        """Returns a set of string state codes for states that have the given
        |export_job_name| export job enabled in any environment (staging-only or staging
         and production).
        """
        return {
            assert_type(c["state_code"], str)
            for c in self.get_export_configs_for_job_filter(export_job_name)
            if c["state_code"]
        }

    def is_export_launched_in_env(
        self,
        export_job_name: str,
        state_code: Optional[str] = None,
    ) -> bool:
        """Returns whether the export corresponding to the given export_job_name and,
        if provided, state_code has been launched in the current environment.
        """
        product = self._relevant_product_config_for_export_params(
            export_job_name, state_code
        )

        if product.is_state_agnostic:
            export_env = product.environment

            if not export_env:
                raise ValueError(
                    "State-agnostic products must have a set environment."
                    f" Found: {product}."
                )
        else:
            if not product.states:
                raise ValueError(
                    f"State-specific products must have set states  Found: {product}."
                )

            if not state_code:
                raise BadProductExportSpecificationError(
                    f"Missing required state_code parameter for export_job_name {export_job_name.upper()}",
                )

            relevant_states = [
                state
                for state in product.states
                if state.state_code == state_code.upper()
            ]
            if not relevant_states:
                raise BadProductExportSpecificationError(
                    f"State_code: {state_code} not configured for"
                    f" export_job_name: {export_job_name.upper()}.",
                )

            if len(relevant_states) != 1:
                raise BadProductExportSpecificationError(
                    f"State_code {state_code} represented more than once in product "
                    f"configuration: {product}."
                )

            export_env = relevant_states[0].environment

        if not in_configured_export_environment(export_env):
            # The export is not configured in the current environment
            return False

        return True

    def get_export_config(
        self, export_job_name: str, state_code: Optional[str] = None
    ) -> ProductExportConfig:
        try:
            _ = self._relevant_product_config_for_export_params(
                export_job_name, state_code
            )
        except BadProductExportSpecificationError as e:
            raise e

        return ProductExportConfig(
            export_job_name=export_job_name, state_code=state_code
        )

    def _relevant_product_config_for_export_params(
        self, export_job_name: str, state_code: Optional[str] = None
    ) -> ProductConfig:
        relevant_product_exports = [
            product
            for product in self.products
            if export_job_name.upper() in product.exports
        ]
        if len(relevant_product_exports) != 1:
            raise BadProductExportSpecificationError(
                f"Wrong number of products returned for export for export_job_name {export_job_name.upper()}",
            )
        if state_code is None and relevant_product_exports[0].states is not None:
            raise BadProductExportSpecificationError(
                f"Missing required state_code parameter for export_job_name {export_job_name.upper()}",
            )
        return relevant_product_exports[0]

    def get_all_export_configs(self) -> List["ProductExportConfig"]:
        exports = []
        for product in self.products:
            if product.is_state_agnostic:
                for export in product.exports:
                    exports.append(self.get_export_config(export_job_name=export))
            else:
                for export in product.exports:
                    if product.states is not None:
                        for state in product.states:
                            exports.append(
                                self.get_export_config(
                                    export_job_name=export, state_code=state.state_code
                                )
                            )
        return exports

    @classmethod
    def from_file(cls, path: str = PRODUCTS_CONFIG_PATH) -> "ProductConfigs":
        """Reads a product config file and returns a list of corresponding ProductConfig objects."""

        product_config = YAMLDict.from_path(path).pop("products", list)
        products = [
            ProductConfig(
                name=product["name"],
                description=product["description"],
                exports=product["exports"],
                states=[
                    ProductStateConfig(
                        state_code=state["state_code"], environment=state["environment"]
                    )
                    for state in product["states"]
                ]
                if "states" in product
                else None,
                environment=product.get("environment"),
                is_state_agnostic=product.get("is_state_agnostic", False),
            )
            for product in product_config
        ]
        return cls(products=products)
