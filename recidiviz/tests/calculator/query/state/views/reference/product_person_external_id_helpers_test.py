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
"""Tests for product_person_external_id_helpers.py"""
import unittest
from collections import defaultdict

from recidiviz.calculator.query.state.views.reference.product_person_external_id_helpers import (
    get_product_display_person_external_id_types_by_state,
    get_product_stable_person_external_id_types_by_state,
)
from recidiviz.common.constants.state.external_id_types import (
    US_CA_DOC,
    US_ND_ELITE,
    US_ND_SID,
    US_NE_ID_NBR,
    US_PA_CONT,
    US_PA_PBPP,
)
from recidiviz.common.constants.state.state_system_type import StateSystemType
from recidiviz.common.constants.states import StateCode
from recidiviz.ingest.direct.external_id_type_helpers import (
    external_id_types_by_state_code,
)
from recidiviz.pipelines.config_paths import PIPELINE_CONFIG_YAML_PATH
from recidiviz.pipelines.ingest.state.validator import (
    person_external_id_types_with_allowed_multiples_per_person,
)
from recidiviz.pipelines.metrics.pipeline_parameters import MetricsPipelineParameters
from recidiviz.pipelines.utils.state_utils.state_calculation_config_manager import (
    get_state_specific_incarceration_metrics_producer_delegate,
    get_state_specific_supervision_metrics_producer_delegate,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.yaml_dict import YAMLDict


def enabled_pipelines_by_state(project_id: str) -> dict[StateCode, list[str]]:
    all_pipelines = YAMLDict.from_path(PIPELINE_CONFIG_YAML_PATH)
    metric_pipelines = all_pipelines.pop_dicts("metric_pipelines")

    pipelines_by_state = defaultdict(list)
    for pipeline_yaml in metric_pipelines:
        parameters = MetricsPipelineParameters(
            project=project_id,
            **pipeline_yaml.get(),  # type: ignore
        )
        pipelines_by_state[StateCode(parameters.state_code)].append(parameters.pipeline)
    return pipelines_by_state


class TestProductPersonExternalIdHelpers(unittest.TestCase):
    """Tests for product_person_external_id_helpers.py"""

    id_types_by_state_code: dict[StateCode, set[str]]

    @classmethod
    def setUpClass(cls) -> None:
        cls.id_types_by_state_code = external_id_types_by_state_code()

    def test_display_person_external_id_is_valid_for_state(self) -> None:
        for system_type in StateSystemType:
            if system_type is StateSystemType.INTERNAL_UNKNOWN:
                continue

            id_types_by_state = get_product_display_person_external_id_types_by_state(
                system_type
            )
            for state_code, id_type in id_types_by_state.items():
                valid_id_types = self.id_types_by_state_code[state_code]

                if id_type not in valid_id_types:
                    raise ValueError(
                        f"Found display person external_id type [{id_type}] defined "
                        f"for state [{state_code}], which is not a valid type for that "
                        f"state."
                    )

    def test_stable_person_external_id_is_valid_for_state(self) -> None:
        for system_type in StateSystemType:
            if system_type is StateSystemType.INTERNAL_UNKNOWN:
                continue

            id_types_by_state = get_product_stable_person_external_id_types_by_state(
                system_type
            )
            for state_code, id_type in id_types_by_state.items():
                valid_id_types = self.id_types_by_state_code[state_code]

                if id_type not in valid_id_types:
                    raise ValueError(
                        f"Found stable person external_id type [{id_type}] defined "
                        f"for state [{state_code}], which is not a valid type for that "
                        f"state."
                    )

    def test_stable_and_display_ids_defined_for_same_states(self) -> None:
        for system_type in StateSystemType:
            if system_type is StateSystemType.INTERNAL_UNKNOWN:
                continue

            display_id_types_by_state = (
                get_product_display_person_external_id_types_by_state(system_type)
            )
            stable_id_types_by_state = (
                get_product_stable_person_external_id_types_by_state(system_type)
            )

            missing_display = set(stable_id_types_by_state) - set(
                display_id_types_by_state
            )
            if missing_display:
                raise ValueError(
                    f"Found state(s) with a defined stable person external_id type but "
                    f"no defined display id type: {missing_display}"
                )
            missing_stable = set(display_id_types_by_state) - set(
                stable_id_types_by_state
            )
            if missing_stable:
                raise ValueError(
                    f"Found state(s) with a defined display person external_id type "
                    f"but no defined stable id type: {missing_stable}"
                )

    def test_supervision_id_types_defined_for_states_with_supervision_pipelines(
        self,
    ) -> None:
        pipeline_names_by_state = enabled_pipelines_by_state(
            project_id=GCP_PROJECT_STAGING
        )

        id_type_by_state = get_product_stable_person_external_id_types_by_state(
            StateSystemType.SUPERVISION
        )

        states_with_supervision_pipeline = {
            state_code
            for state_code, pipeline_names in pipeline_names_by_state.items()
            if any("supervision" in pipeline_name for pipeline_name in pipeline_names)
        }

        missing_id_type = states_with_supervision_pipeline - set(id_type_by_state)
        if missing_id_type:
            raise ValueError(
                f"Found states with enabled supervision Dataflow pipelines, but no"
                f"stable person external_id type chosen in "
                f"product_person_external_id_helpers.py: {missing_id_type}"
            )

    def test_incarceration_id_types_defined_for_states_with_supervision_pipelines(
        self,
    ) -> None:
        pipeline_names_by_state = enabled_pipelines_by_state(
            project_id=GCP_PROJECT_STAGING
        )

        id_type_by_state = get_product_stable_person_external_id_types_by_state(
            StateSystemType.INCARCERATION
        )

        states_with_supervision_pipeline = {
            state_code
            for state_code, pipeline_names in pipeline_names_by_state.items()
            if any("incarceration" in pipeline_name for pipeline_name in pipeline_names)
        }

        missing_id_type = states_with_supervision_pipeline - set(id_type_by_state)
        if missing_id_type:
            raise ValueError(
                f"Found states with enabled incarceration Dataflow pipelines, but no"
                f"stable person external_id type chosen in "
                f"product_person_external_id_helpers.py: {missing_id_type}"
            )

    def test_incarceration_stable_id_types_match_pipeline_delegates(
        self,
    ) -> None:
        for (
            state_code,
            stable_id_type,
        ) in get_product_stable_person_external_id_types_by_state(
            StateSystemType.INCARCERATION
        ).items():

            incarceration_pipeline_delegate = (
                get_state_specific_incarceration_metrics_producer_delegate(
                    state_code.value
                )
            )
            pipeline_primary_id_type = (
                incarceration_pipeline_delegate.primary_person_external_id_to_include()
            )
            if stable_id_type != pipeline_primary_id_type:
                raise ValueError(
                    f"Found state [{state_code.value}] with "
                    f"primary_person_external_id_to_include "
                    f"[{pipeline_primary_id_type}] in "
                    f"{type(incarceration_pipeline_delegate).__name__} that does not "
                    f"match the incarceration stable person external id type defined "
                    f"in product_person_external_id_helpers.py [{stable_id_type}]."
                )

    def test_supervision_stable_id_types_match_pipeline_delegates(
        self,
    ) -> None:
        for (
            state_code,
            stable_id_type,
        ) in get_product_stable_person_external_id_types_by_state(
            StateSystemType.SUPERVISION
        ).items():

            supervision_pipeline_delegate = (
                get_state_specific_supervision_metrics_producer_delegate(
                    state_code.value
                )
            )
            pipeline_primary_id_type = (
                supervision_pipeline_delegate.primary_person_external_id_to_include()
            )
            if stable_id_type != pipeline_primary_id_type:
                raise ValueError(
                    f"Found state [{state_code.value}] with "
                    f"primary_person_external_id_to_include "
                    f"[{pipeline_primary_id_type}] in "
                    f"{type(supervision_pipeline_delegate).__name__} that does not "
                    f"match the supervision stable person external id type defined in "
                    f"product_person_external_id_helpers.py [{stable_id_type}]."
                )

    def test_no_unstable_stable_ids_used(self) -> None:
        """Tests that we don't ever designate an unstable id (changes for a person over
        time) as the "stable" person external_id type.
        """

        # There are a handful of states where there is no single id that remains
        # stable for a JII over time. These are states where the "stable" ID can't be
        # trusted to be fully stable, but we also have no other better option.
        # PLEASE DO NOT ADD TO THIS EXEMPTIONS LIST WITHOUT TALKING TO DOPPLER / ASKING
        # IN #platform-team.
        known_unstable_stable_ids = {
            StateCode.US_CA: {
                StateSystemType.INCARCERATION: US_CA_DOC,
                StateSystemType.SUPERVISION: US_CA_DOC,
            },
            StateCode.US_ND: {
                StateSystemType.INCARCERATION: US_ND_ELITE,
                StateSystemType.SUPERVISION: US_ND_SID,
            },
            StateCode.US_NE: {
                StateSystemType.INCARCERATION: US_NE_ID_NBR,
                StateSystemType.SUPERVISION: US_NE_ID_NBR,
            },
            StateCode.US_PA: {
                StateSystemType.INCARCERATION: US_PA_CONT,
                StateSystemType.SUPERVISION: US_PA_PBPP,
            },
        }

        for system_type in StateSystemType:
            if system_type == StateSystemType.INTERNAL_UNKNOWN:
                continue
            for (
                state_code,
                stable_id_type,
            ) in get_product_stable_person_external_id_types_by_state(
                system_type
            ).items():
                types_allowing_multiples_per_person = (
                    person_external_id_types_with_allowed_multiples_per_person(
                        state_code
                    )
                )

                exempt_type = known_unstable_stable_ids.get(state_code, {}).get(
                    system_type, None
                )
                is_exempt = exempt_type and exempt_type == stable_id_type

                if (
                    not is_exempt
                    and stable_id_type in types_allowing_multiples_per_person
                ):
                    raise ValueError(
                        f"Found state [{state_code.value}] with stable "
                        f"person_external_id type [{stable_id_type}] that allows "
                        f"multiple ids of that type per person. The stable external id "
                        f"type for {system_type.value} should ideally be one that "
                        f"everyone involved in the {system_type.value} system has "
                        f"assigned but which also does not change over time."
                    )
