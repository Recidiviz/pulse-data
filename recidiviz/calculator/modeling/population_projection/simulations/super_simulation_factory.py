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

from typing import Dict, Any
import yaml

from recidiviz.calculator.modeling.population_projection.super_simulation_macrosim import MacroSuperSimulation
from recidiviz.calculator.modeling.population_projection.super_simulation_microsim import MicroSuperSimulation
from recidiviz.calculator.modeling.population_projection.super_simulation import SuperSimulation


class SuperSimulationFactory:
    """Parse yaml config and initialize either a MacroSuperSimulation or MicroSuperSimulation"""

    # TODO(#5185): incorporate dataclass

    @classmethod
    def build_super_simulation(cls, yaml_file_path: str) -> SuperSimulation:
        with open(yaml_file_path) as yaml_file:
            initialization_params = yaml.full_load(yaml_file)

            cls._check_valid_yaml_config(initialization_params)

            model_params = dict()
            model_params['reference_year'] = initialization_params['reference_date']
            model_params['time_step'] = initialization_params['time_step']

            model_params['simulation_compartments_architecture'] = initialization_params['model_architecture']

            model_params['disaggregation_axes'] = initialization_params['disaggregation_axes']

            model_params['user_inputs_raw'] = initialization_params['user_inputs']

            model_params['compartment_costs'] = initialization_params['per_year_costs']

            model_params['data_inputs_raw'] = initialization_params['data_inputs']

            if 'big_query_simulation_tag' in initialization_params['data_inputs']:
                return MacroSuperSimulation(model_params)

            if 'big_query_inputs' in initialization_params['data_inputs']:
                return MicroSuperSimulation(model_params)

            raise RuntimeError(f'Unrecognized data input: {initialization_params.keys()[0]}')

    @staticmethod
    def _check_valid_yaml_config(initialization_params: Dict[str, Any]) -> None:
        # Make sure only one input setting is provided in the yaml file

        required_inputs = {'user_inputs', 'model_architecture', 'reference_date', 'time_step', 'data_inputs',
                           'disaggregation_axes', 'per_year_costs'}
        given_inputs = set(initialization_params.keys())

        missing_inputs = required_inputs.difference(given_inputs)
        if len(missing_inputs) > 0:
            raise ValueError(f"Missing yaml inputs: {missing_inputs}")

        unexpected_inputs = given_inputs.difference(required_inputs)
        if len(unexpected_inputs) > 0:
            raise ValueError(f"Unexpected yaml inputs: {unexpected_inputs}")

        if len(initialization_params['data_inputs']) != 1:
            raise ValueError(
                f"Only one data input can be set in the yaml file, not {len(initialization_params['data_inputs'])}"
            )

        model_architecture_yaml_key = 'model_architecture'
        compartment_costs_key = 'per_year_costs'

        # Ensure there are compartment costs for every compartment in the model architecture
        model_compartments = set(c for c in initialization_params[model_architecture_yaml_key]
                                 if initialization_params[model_architecture_yaml_key][c] is not None)
        compartment_costs = set(initialization_params[compartment_costs_key].keys())
        if compartment_costs != model_compartments:
            raise ValueError(
                f"Compartments do not match in the YAML '{compartment_costs_key}' and '{model_architecture_yaml_key}'\n"
                f"Mismatched values: {compartment_costs ^ model_compartments}"
            )
