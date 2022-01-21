# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""SuperSimulation composed object for initializing simulations."""
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.population_simulation.population_simulation import (
    PopulationSimulation,
)
from recidiviz.calculator.modeling.population_projection.population_simulation.population_simulation_factory import (
    PopulationSimulationFactory,
)
from recidiviz.calculator.modeling.population_projection.shell_compartment import (
    ShellCompartment,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.super_simulation.initializer import (
    SimulationInputData,
    UserInputs,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.time_converter import (
    TimeConverter,
)


class Simulator:
    """Runs simulations for SuperSimulation."""

    def __init__(self, microsim: bool, time_converter: TimeConverter) -> None:
        self.pop_simulations: Dict[str, PopulationSimulation] = {}
        self.microsim = microsim
        self.time_converter = time_converter

    def get_population_simulations(self) -> Dict[str, PopulationSimulation]:
        if not self.pop_simulations:
            raise ValueError("No population simulations to return")
        return self.pop_simulations

    def simulate_policy(
        self,
        user_inputs: UserInputs,
        data_inputs: SimulationInputData,
        first_relevant_ts: int,
        policy_list: List[SparkPolicy],
        output_compartment: str,
    ) -> pd.DataFrame:
        """
        Run one PopulationSimulation with policy implemented and one baseline, returns cumulative and non-cumulative
            life-years diff, cost diff, total pop diff, all by compartment
        `user_inputs` should be a dict of user inputs
        `policy_list` should be a list of SparkPolicy objects to be applied in the policy scenario
        `output_compartment` should be the primary compartment to be graphed at the end (doesn't affect calculation)
        `cost_multipliers` should be a df with one column per disaggregation axis and a column `multiplier`
        """
        self._reset_pop_simulations()

        self.pop_simulations["policy"] = self._build_population_simulation(
            user_inputs, data_inputs, policy_list, first_relevant_ts
        )
        self.pop_simulations["control"] = self._build_population_simulation(
            user_inputs, data_inputs, [], first_relevant_ts
        )

        self.pop_simulations["policy"].simulate_policies()
        self.pop_simulations["control"].simulate_policies()

        results = {
            scenario: simulation.get_population_projections()
            for scenario, simulation in self.pop_simulations.items()
        }
        results = {
            i: results[i][results[i]["time_step"] >= user_inputs.start_time_step]
            for i in results
        }

        # log warnings from ARIMA model
        self._log_predicted_admissions_warnings()

        self._graph_results(user_inputs, results, output_compartment)
        return self._format_simulation_results(user_inputs, collapse_compartments=False)

    def simulate_baseline(
        self,
        user_inputs: UserInputs,
        data_inputs: SimulationInputData,
        display_compartments: List[str],
        first_relevant_ts: int,
        reset: bool = True,
    ) -> None:
        """
        Calculates a baseline projection, returns transition error for a specific transition
        `display_compartments` are the compartment whose populations you wish to display
        `validation_pairs` should be a dict with key/value pairs corresponding to compartment/outflow_to transitions
            to calculate error for
        """
        if reset:
            self._reset_pop_simulations()

        if first_relevant_ts > user_inputs.start_time_step:
            raise ValueError(
                f"first_relevant_ts ({first_relevant_ts}) must be less than start_time_step "
                f"({user_inputs.start_time_step})"
            )

        # Run one simulation
        self.pop_simulations[
            "baseline_projections"
        ] = self._build_population_simulation(
            user_inputs, data_inputs, [], first_relevant_ts
        )

        self.pop_simulations["baseline_projections"].simulate_policies()

        # log warnings from ARIMA model
        self._log_predicted_admissions_warnings()

        if display_compartments:
            simulation_results = self._format_simulation_results(
                user_inputs, collapse_compartments=True
            )
            display_results = pd.DataFrame(index=simulation_results.year.unique())
            for comp in display_compartments:
                if comp not in simulation_results.compartment.unique():
                    logging.warning(
                        "Display compartment not in simulation architecture: %s",
                        comp,
                    )
                else:
                    relevant_results = simulation_results[
                        (simulation_results.compartment == comp)
                        & (user_inputs.start_time_step <= simulation_results.year)
                    ]
                    display_results[comp] = relevant_results[
                        "baseline_projections_total_population"
                    ]

            display_results.plot(
                title="Baseline Population Projection",
                ylabel="Estimated Total Population",
            )
            plt.legend(loc="lower left")
            plt.ylim([0, None])

    def microsim_baseline_over_time(
        self,
        user_inputs: UserInputs,
        run_date_data_inputs: Dict[datetime, SimulationInputData],
        run_date_first_relevant_ts: Dict[datetime, int],
        projection_time_steps_override: Optional[int],
    ) -> None:
        self._reset_pop_simulations()

        # Change some user_inputs for the validation loop
        if projection_time_steps_override is not None:
            user_inputs.projection_time_steps = projection_time_steps_override

        for start_date, data_inputs in run_date_data_inputs.items():
            print(start_date)
            user_inputs.start_time_step = run_date_first_relevant_ts[start_date]
            simulation_name = f"baseline_{start_date.date()}"
            self.pop_simulations[simulation_name] = self._build_population_simulation(
                user_inputs, data_inputs, [], run_date_first_relevant_ts[start_date]
            )

            self.pop_simulations[simulation_name].simulate_policies()

        # log warnings from ARIMA model
        self._log_predicted_admissions_warnings()

    def get_cohort_hydration_simulations(
        self,
        user_inputs: UserInputs,
        data_inputs: SimulationInputData,
        range_start: int,
        range_end: int,
        step_size: float,
    ) -> Dict[str, PopulationSimulation]:
        """
        Generates population simulations to feed to Validator.calculate_cohort_hydration_error
        """
        self._reset_pop_simulations()

        for ts in np.arange(range_start, range_end, step_size):
            self.pop_simulations[
                f"backfill_period_{ts}_time_steps"
            ] = self._build_population_simulation(
                user_inputs,
                data_inputs,
                [],
                first_relevant_ts=user_inputs.start_time_step - ts,
            )
            self.pop_simulations[f"backfill_period_{ts}_time_steps"].simulate_policies()

        # log warnings from ARIMA model
        self._log_predicted_admissions_warnings()

        return self.pop_simulations

    def get_sub_group_ids_dict(self) -> Dict[str, Dict[str, Any]]:
        return list(self.pop_simulations.values())[0].sub_group_ids_dict

    def _graph_results(
        self,
        user_inputs: UserInputs,
        simulations: Dict[str, pd.DataFrame],
        output_compartment: str,
    ) -> None:
        simulation_results = self._format_simulation_results(
            user_inputs, collapse_compartments=True
        )

        display_results = simulation_results[
            simulation_results["compartment"] == output_compartment
        ]
        y_columns = [
            f"{simulation_name}_total_population"
            for simulation_name in simulations.keys()
        ]
        display_results.plot(
            x="year",
            y=y_columns,
        )
        plt.title(f"Policy Impact on {output_compartment} Population")
        plt.ylabel(f"Estimated\n{output_compartment} Population")
        plt.legend(loc="lower left")

        y_max = 1.1 * display_results[y_columns].max().max()
        print(y_max)
        plt.ylim([0, y_max])

    def _format_simulation_results(
        self,
        user_inputs: UserInputs,
        collapse_compartments: bool = False,
    ) -> pd.DataFrame:
        """Re-format PopulationSimulation results so each simulation is a column"""
        simulation_results = pd.DataFrame()
        for scenario, simulation in self.pop_simulations.items():
            results = simulation.get_population_projections()
            results = results[results.time_step >= user_inputs.start_time_step]
            results = results.rename(
                {
                    "time_step": "year",
                    "total_population": f"{scenario}_total_population",
                },
                axis=1,
            )
            results.year = self.time_converter.convert_time_steps_to_year(results.year)

            if simulation_results.empty:
                simulation_results = results
            else:
                simulation_results = simulation_results.merge(
                    results, on=["compartment", "year", "simulation_group"]
                )

        if collapse_compartments:
            simulation_results = simulation_results.groupby(
                ["compartment", "year"], as_index=False
            ).sum()

        simulation_results.index = simulation_results.year

        return simulation_results

    def _reset_pop_simulations(self) -> None:
        self.pop_simulations = {}

    def _log_predicted_admissions_warnings(self) -> None:
        """
        Checks if PredictedAdmissions objects have any warnings. If so, log them.
        """
        warnings = []

        # collect all compartments
        compartments = []
        for pop_simulation in self.pop_simulations.values():
            for sub_simulation in pop_simulation.sub_simulations.values():
                for compartment in sub_simulation.simulation_compartments.values():
                    compartments.append(compartment)
        # collect all warnings
        for compartment in compartments:
            if isinstance(compartment, ShellCompartment):
                for admissions_predictor in compartment.admissions_predictors.values():
                    while admissions_predictor.warnings:
                        w = admissions_predictor.warnings.pop()
                        if w not in warnings:
                            warnings.append(w)
        # now log unique warnings
        while warnings:
            w = warnings.pop()
            logging.warning(w)

    @staticmethod
    def _build_population_simulation(
        user_inputs: UserInputs,
        data_inputs: SimulationInputData,
        policy_list: List[SparkPolicy],
        first_relevant_ts: int,
    ) -> PopulationSimulation:
        return PopulationSimulationFactory.build_population_simulation(
            user_inputs=user_inputs,
            policy_list=policy_list,
            first_relevant_ts=first_relevant_ts,
            data_inputs=data_inputs,
        )
