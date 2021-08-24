# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2020 Recidiviz, Inc.
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
"""Population projection simulation initializer object -- instantiates SuperSimulation"""
from typing import Any, Dict, Tuple

from recidiviz.calculator.modeling.population_projection.super_simulation.exporter import (
    Exporter,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.initializer import (
    Initializer,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.simulator import (
    Simulator,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.super_simulation import (
    SuperSimulation,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.time_converter import (
    TimeConverter,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.validator import (
    Validator,
)
from recidiviz.utils.yaml_dict import YAMLDict


class SuperSimulationFactory:
    """Parse yaml config and initialize either a MacroSuperSimulation or MicroSuperSimulation"""

    # TODO(#5185): incorporate dataclass

    @classmethod
    def build_super_simulation(cls, yaml_file_path: str) -> SuperSimulation:
        """Initialize a SuperSimulation object using the config defined in the YAML file"""
        model_params = cls.get_model_params(yaml_file_path)

        if "big_query_simulation_tag" in model_params["data_inputs_raw"].keys():
            microsim = False
            simulation_tag = model_params["data_inputs_raw"]["big_query_simulation_tag"]

        elif "big_query_inputs" in model_params["data_inputs_raw"].keys():
            microsim = True
            simulation_tag = model_params["data_inputs_raw"]["big_query_inputs"][
                "state_code"
            ]

        else:
            raise RuntimeError(
                f'Unrecognized data input: {model_params["data_inputs_raw"].keys()}'
            )

        time_converter = TimeConverter(
            model_params["reference_year"], model_params["time_step"]
        )
        initializer = Initializer(
            time_converter,
            model_params["user_inputs_raw"],
            model_params["data_inputs_raw"],
            model_params["compartments_architecture"],
            model_params["disaggregation_axes"],
            microsim,
        )

        simulator = Simulator(microsim, time_converter)
        validator = Validator(microsim, time_converter)
        exporter = Exporter(
            microsim, model_params["compartment_costs"], simulation_tag, time_converter
        )

        return SuperSimulation(initializer, simulator, validator, exporter)

    @classmethod
    def get_model_params(cls, yaml_file_path: str) -> Dict[str, Any]:
        """Get the model parameters from the YAMLDict"""
        initialization_params = YAMLDict.from_path(yaml_file_path)

        cls._check_valid_yaml_inputs(initialization_params)

        model_params: Dict[str, Any] = {}

        model_params["reference_year"] = initialization_params.pop(
            "reference_date", float
        )

        model_params["time_step"] = initialization_params.pop("time_step", float)

        model_params["disaggregation_axes"] = initialization_params.pop(
            "disaggregation_axes", list
        )

        model_params["data_inputs_raw"] = cls._get_valid_data_inputs(
            initialization_params
        )

        model_params["user_inputs_raw"] = cls._get_user_inputs(
            initialization_params, model_params
        )

        (
            model_params["compartments_architecture"],
            model_params["compartment_costs"],
        ) = cls._get_valid_compartments(initialization_params)

        return model_params

    @staticmethod
    def _check_valid_yaml_inputs(initialization_params: YAMLDict) -> None:
        # Make sure only one input setting is provided in the yaml file

        required_inputs = {
            "user_inputs",
            "compartments_architecture",
            "reference_date",
            "time_step",
            "data_inputs",
            "disaggregation_axes",
            "per_year_costs",
        }
        given_inputs = set(initialization_params.keys())

        missing_inputs = required_inputs.difference(given_inputs)
        if len(missing_inputs) > 0:
            raise ValueError(f"Missing yaml inputs: {missing_inputs}")

        unexpected_inputs = given_inputs.difference(required_inputs)
        if len(unexpected_inputs) > 0:
            raise ValueError(f"Unexpected yaml inputs: {unexpected_inputs}")

    @staticmethod
    def _get_valid_data_inputs(initialization_params: YAMLDict) -> Dict[str, Any]:
        """Helper to retrieve data_inputs for get_model_params"""

        given_data_inputs = initialization_params.pop_dict("data_inputs")
        if len(given_data_inputs) != 1:
            raise ValueError(
                f"Only one data input can be set in the yaml file, not {len(given_data_inputs)}"
            )

        data_inputs: Dict[str, Any] = {}

        if "big_query_inputs" in given_data_inputs.keys():
            big_query_inputs_yaml_dict = given_data_inputs.pop_dict("big_query_inputs")
            big_query_inputs_keys = big_query_inputs_yaml_dict.keys()

            big_query_inputs_dict: Dict[str, str] = {}
            for k in big_query_inputs_keys:
                big_query_inputs_dict[k] = big_query_inputs_yaml_dict.pop(k, str)

            data_inputs["big_query_inputs"] = big_query_inputs_dict
        elif "big_query_simulation_tag" in given_data_inputs.keys():
            data_inputs["big_query_simulation_tag"] = given_data_inputs.pop(
                "big_query_simulation_tag", str
            )
        else:
            raise ValueError(
                f"Received unexpected key in data_inputs: {given_data_inputs.keys()[0]}"
            )

        return data_inputs

    @staticmethod
    def _get_user_inputs(
        initialization_params: YAMLDict, model_params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Helper to retrieve user_inputs for get_model_params"""

        user_inputs: Dict[str, Any] = {}
        user_inputs_yaml_dict = initialization_params.pop_dict("user_inputs")

        if "big_query_simulation_tag" in model_params["data_inputs_raw"].keys():
            user_inputs = {
                "start_year": user_inputs_yaml_dict.pop("start_year", float),
                "projection_years": user_inputs_yaml_dict.pop(
                    "projection_years", float
                ),
            }

        if "big_query_inputs" in model_params["data_inputs_raw"].keys():
            user_inputs = {
                "start_year": user_inputs_yaml_dict.pop("start_year", float),
                "projection_years": user_inputs_yaml_dict.pop(
                    "projection_years", float
                ),
                "run_date": user_inputs_yaml_dict.pop("run_date", str),
            }

        # Check for optional arguments
        if user_inputs_yaml_dict:
            user_inputs_keys = user_inputs_yaml_dict.keys()
            for k in user_inputs_keys:
                if k not in {"constant_admissions", "speed_run", "cross_flow_function"}:
                    raise ValueError(f"Received unexpected key in user_inputs: {k}")
                user_inputs[k] = user_inputs_yaml_dict.pop(k, bool)

        return user_inputs

    @staticmethod
    def _get_valid_compartments(
        initialization_params: YAMLDict,
    ) -> Tuple[Dict[str, str], Dict[str, float]]:
        """Helper to retrieve model_architecture and compartment costs for get_model_params"""

        compartments_architecture_yaml_key = "compartments_architecture"
        compartments_architecture_raw = initialization_params.pop_dict(
            compartments_architecture_yaml_key
        )
        compartments_architecture_keys = compartments_architecture_raw.keys()

        compartments_architecture_dict: Dict[str, Any] = {}
        for k in compartments_architecture_keys:
            compartments_architecture_dict[
                k
            ] = compartments_architecture_raw.pop_optional(k, str)

        compartment_costs_key = "per_year_costs"
        compartment_costs_raw = initialization_params.pop_dict(compartment_costs_key)
        compartment_costs_keys = compartment_costs_raw.keys()

        compartment_costs_dict: Dict[str, float] = {}
        for k in compartment_costs_keys:
            compartment_costs_dict[k] = compartment_costs_raw.pop(k, float)

        # Ensure there are compartment costs for every compartment in the model architecture
        model_compartments = set(
            c
            for c in compartments_architecture_keys
            if compartments_architecture_dict[c] != "shell"
        )
        compartment_costs = set(compartment_costs_keys)
        if compartment_costs != model_compartments:
            raise ValueError(
                f"Compartments do not match in the YAML '{compartment_costs_key}' "
                f"and '{compartments_architecture_yaml_key}'\n"
                f"Mismatched values: {compartment_costs ^ model_compartments}"
            )

        return compartments_architecture_dict, compartment_costs_dict
