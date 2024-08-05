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
"""Responsible for finding state-specific external validation view builders and building
state-agnostic external validation view builders.
"""
from collections import defaultdict
from functools import cached_property
from typing import Dict, List, Set

import attr

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.big_query.big_query_view import (
    BigQueryViewBuilder,
    SimpleBigQueryViewBuilder,
)
from recidiviz.big_query.big_query_view_collector import BigQueryViewCollector
from recidiviz.big_query.union_all_big_query_view_builder import (
    UnionAllBigQueryViewBuilder,
)
from recidiviz.common import attr_validators
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string import StrictStringFormatter
from recidiviz.validation.views import dataset_config
from recidiviz.validation.views.external_data import regions as external_data_regions
from recidiviz.validation.views.metadata.validation_schema_config import (
    TableSchemaInfo,
    get_external_validation_schema,
)

DEFAULT_DESCRIPTION = "Contains external data for {table_name_plain_text} to validate against. See http://go/external-validations for instructions on adding new data."


@attr.define
class StateAgnosticExternalValidationDataViewConfig:
    """Configuration helper used to group and generate a UnionAllBigQueryViewBuilder from
    a collection of state-specific SimpleBigQueryViewBuilder that share the same
    |view_id|.

    Attributes:
        view_id (str): the name of the view
        view_id_description (str): description of the validation view
        select_statement (str): select statement to be used to select from each child
            view builder
    """

    view_id: str = attr.ib(validator=attr_validators.is_str)
    view_id_description: str = attr.ib(validator=attr_validators.is_str)
    select_statement: str = attr.ib(validator=attr_validators.is_str)

    @classmethod
    def from_table_schema_info(
        cls, schema_info: TableSchemaInfo
    ) -> "StateAgnosticExternalValidationDataViewConfig":
        return StateAgnosticExternalValidationDataViewConfig(
            view_id=schema_info.table_name.removesuffix("_materialized"),
            view_id_description=StrictStringFormatter().format(
                DEFAULT_DESCRIPTION,
                table_name_plain_text=schema_info.table_name_plain_text,
            ),
            select_statement=f"SELECT {','.join(schema_info.columns)}",
        )

    def to_union_all_view_builder(
        self, parent_view_builders: List[SimpleBigQueryViewBuilder]
    ) -> UnionAllBigQueryViewBuilder:
        """Uses |parent_view_builders| to build a UnionAllBigQueryViewBuilder for the
        config.
        """
        return UnionAllBigQueryViewBuilder(
            dataset_id=dataset_config.EXTERNAL_ACCURACY_DATASET,
            view_id=self.view_id,
            description=self.view_id_description,
            parent_view_builders=parent_view_builders,
            builder_to_select_statement=lambda _: self.select_statement,
        )


class ExternalValidationDataBigQueryViewCollector(
    BigQueryViewCollector[SimpleBigQueryViewBuilder]
):
    """Class responsible for collecting all state-specific external validation view
    builders and building state-agnostic view builders.

    Class Constants:
        KNOWN_STATE_SPECIFIC_HELPER_VIEWS (Set[BigQueryAddress]): a set of BigQueryAddress
            objects for views that are stored in the the `recidiviz.validation.views.external_data.regions`
            module but which are not directly unioned into one of the state-agnostic
            external validation queries. These views are queried by one of the other
            state-specific external validation queries.

    """

    KNOWN_STATE_SPECIFIC_HELPER_VIEWS: Set[BigQueryAddress] = {
        BigQueryAddress(dataset_id="us_me_validation", table_id="population_releases"),
        BigQueryAddress(
            dataset_id="us_mi_validation", table_id="cb_971_report_supervision_unified"
        ),
        BigQueryAddress(
            dataset_id="us_mi_validation", table_id="cb_971_report_unified"
        ),
    }

    def __init__(self) -> None:
        self._state_specific_helper_view_builders: List[BigQueryViewBuilder] = []
        self._state_agnostic_external_data_validation_configs: List[
            StateAgnosticExternalValidationDataViewConfig
        ] = [
            StateAgnosticExternalValidationDataViewConfig.from_table_schema_info(
                table_schema
            )
            for table_schema in get_external_validation_schema().tables
        ]

    @cached_property
    def external_validation_data_view_ids(self) -> Set[str]:
        return {
            config.view_id
            for config in self._state_agnostic_external_data_validation_configs
        }

    def _validate_all_state_agnostic_configs_have_builders(
        self,
        state_specific_external_data_view_id_to_builders: Dict[
            str, List[SimpleBigQueryViewBuilder]
        ],
    ) -> None:
        for view_id_config in self._state_agnostic_external_data_validation_configs:
            if (
                view_id_config.view_id
                not in state_specific_external_data_view_id_to_builders
            ):
                raise ValueError(
                    f"Expected to find view builders in {external_data_regions.__file__}"
                    f"that had the view id [{view_id_config.view_id}] but found none."
                    f"If you are seeing this and removed the last validation view of "
                    f"that type, please remove the [{view_id_config.view_id}] entry from "
                    f"validation_external_accuracy_schema.yaml"
                )

    def collect_view_builders(self) -> List[SimpleBigQueryViewBuilder]:
        all_view_builders = self.collect_view_builders_in_module(
            builder_type=SimpleBigQueryViewBuilder,
            view_dir_module=external_data_regions,
            recurse=True,
            view_builder_attribute_name_regex=".*_VIEW_BUILDER",
        )

        all_external_validation_data_view_builders: List[SimpleBigQueryViewBuilder] = []

        for vb in all_view_builders:
            if vb.address in self.KNOWN_STATE_SPECIFIC_HELPER_VIEWS:
                self._state_specific_helper_view_builders.append(vb)
                continue

            if vb.view_id not in self.external_validation_data_view_ids:
                raise ValueError(
                    f"Unrecognized for view_id: {vb.address.to_str()}; All view_ids in "
                    f"{external_data_regions.__file__} must have a view_id that is one "
                    f"of :[{self.external_validation_data_view_ids}] or is listed in the "
                    f"list of known state-specific view helpers: "
                    f"[{self.KNOWN_STATE_SPECIFIC_HELPER_VIEWS}]. If you added a new "
                    f"validation type or a parent address to an existing view, please "
                    f"add them to the variables defined in {__file__}"
                )

            all_external_validation_data_view_builders.append(vb)

        return all_external_validation_data_view_builders

    def build_state_agnostic_external_validation_view_builders(
        self,
        state_specific_external_validation_data_view_builders: List[
            SimpleBigQueryViewBuilder
        ],
    ) -> List[UnionAllBigQueryViewBuilder]:
        """Groups |state_specific_external_validation_data_view_builders| by view id and
        builds a state-agnostic view builder that combines the data into a single view.
        """
        external_data_view_id_to_builders = defaultdict(list)

        for vb in state_specific_external_validation_data_view_builders:
            external_data_view_id_to_builders[vb.view_id].append(vb)

        self._validate_all_state_agnostic_configs_have_builders(
            external_data_view_id_to_builders
        )

        return [
            view_id_config.to_union_all_view_builder(
                external_data_view_id_to_builders[view_id_config.view_id]
            )
            for view_id_config in self._state_agnostic_external_data_validation_configs
        ]

    def collect_state_specific_and_build_state_agnostic_external_validation_view_builders(
        self,
    ) -> List[BigQueryViewBuilder]:
        """Collects all state-specific external validation view builders and builds
        state-agnostic view builders for |state_agnostic_external_data_validation_configs|.
        Returns all state-specific external validation view builders and the newly built
        state-agnostic external validation view builders.
        """
        state_specific_external_validation_data_view_builders = (
            self.collect_view_builders()
        )
        return [
            *self._state_specific_helper_view_builders,
            *state_specific_external_validation_data_view_builders,
            *self.build_state_agnostic_external_validation_view_builders(
                state_specific_external_validation_data_view_builders
            ),
        ]


if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        for (
            builder
        ) in ExternalValidationDataBigQueryViewCollector().collect_view_builders():
            print(
                f"=========================={builder.address.to_str()}=========================="
            )
            builder.build_and_print()
