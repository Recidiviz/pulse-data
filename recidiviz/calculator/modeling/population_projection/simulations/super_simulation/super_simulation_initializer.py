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
"""Initializer object for SuperSimulation."""

from typing import Dict

from recidiviz.calculator.modeling.population_projection.simulations.population_simulation.\
    population_simulation_macro_initializer import PopulationSimulationMacroInitializer
from recidiviz.calculator.modeling.population_projection.simulations.population_simulation.\
    population_simulation_micro_initializer import PopulationSimulationMicroInitializer
from recidiviz.calculator.modeling.population_projection.simulations.population_simulation.population_simulation \
    import PopulationSimulation


class SuperSimulationInitializer:
    """Handles set-up specific logic of SuperSimulation."""

    @classmethod
    def build_population_simulation(cls, simulation_setup: Dict[str, Dict[str, str]]) -> PopulationSimulation:
        # TODO(#5186): refactor PopulationSimulation to match SubSimulation with single initializer
        population_simulation_initializer = {
            'micro': PopulationSimulationMicroInitializer,
            'macro': PopulationSimulationMacroInitializer
        }

        population_simulation_type = simulation_setup['population_simulation']['initializer']

        sub_simulation_type = simulation_setup['sub_simulation']['initializer']
        if sub_simulation_type == 'micro':
            should_initialize_compartment_populations = True
            should_scale_populations_after_step = False
        elif sub_simulation_type == 'macro':
            should_initialize_compartment_populations = False
            should_scale_populations_after_step = True
        else:
            raise ValueError(f'Unexpected sub_simulation_type: {sub_simulation_type}')

        return PopulationSimulation(population_simulation_initializer[population_simulation_type](
            should_initialize_compartment_populations=should_initialize_compartment_populations,
            should_scale_populations_after_step=should_scale_populations_after_step
        ))
