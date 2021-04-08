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
"""Highest level simulation object -- runs various comparative scenarios"""

from typing import Dict, List, Optional, Tuple
from datetime import datetime
import matplotlib.pyplot as plt
import pandas as pd

from recidiviz.calculator.modeling.population_projection.super_simulation.initializer import (
    Initializer,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.simulator import (
    Simulator,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.validator import (
    Validator,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.exporter import (
    Exporter,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy


class SuperSimulation:
    """Manage the PopulationSimulations and output data needed to run tests, baselines, and policy scenarios"""

    def __init__(
        self,
        initializer: Initializer,
        simulator: Simulator,
        validator: Validator,
        exporter: Exporter,
    ) -> None:

        self.initializer = initializer
        self.simulator = simulator
        self.validator = validator
        self.exporter = exporter

    def simulate_baseline(
        self, display_compartments: List[str], first_relevant_ts: Optional[int] = None
    ) -> None:
        """
        Calculates a baseline projection.
        `simulation_title` is the desired simulation tag for this baseline
        `first_relevant_ts` is the ts at which to start initialization
        """
        first_relevant_ts = self.initializer.get_first_relevant_ts(first_relevant_ts)
        data_inputs = self.initializer.get_data_inputs()
        user_inputs = self.initializer.get_user_inputs()

        self.simulator.simulate_baseline(
            user_inputs,
            data_inputs,
            display_compartments,
            first_relevant_ts,
        )
        self.validator.reset(self.simulator.get_population_simulations())

    def simulate_policy(
        self, policy_list: List[SparkPolicy], output_compartment: str
    ) -> pd.DataFrame:
        first_relevant_ts = self.initializer.get_first_relevant_ts()
        data_inputs = self.initializer.get_data_inputs()
        user_inputs = self.initializer.get_user_inputs()

        simulation_output = self.simulator.simulate_policy(
            user_inputs,
            data_inputs,
            first_relevant_ts,
            policy_list,
            output_compartment,
        )
        self.validator.reset(
            self.simulator.get_population_simulations(),
            {"policy_simulation": simulation_output},
        )
        return simulation_output

    def microsim_baseline_over_time(self, start_run_dates: List[datetime]) -> None:
        """
        Run a microsim at many different run_dates.
        `start_run_dates` should be a list of datetime at which to run the simulation
        """
        user_inputs = self.initializer.get_user_inputs()
        (
            data_inputs_dict,
            first_relevant_ts_dict,
        ) = self.initializer.get_inputs_for_microsim_baseline_over_time(start_run_dates)

        self.simulator.microsim_baseline_over_time(
            user_inputs, data_inputs_dict, first_relevant_ts_dict
        )
        self.validator.reset(self.simulator.get_population_simulations())

    def upload_baseline_simulation_results_to_bq(
        self,
        simulation_tag: Optional[str] = None,
    ) -> Dict[str, pd.DataFrame]:
        output_data = self.validator.get_output_data_for_upload()
        data_inputs = self.initializer.get_data_inputs()
        excluded_pop_data = self.initializer.get_excluded_pop_data()

        return self.exporter.upload_baseline_simulation_results_to_bq(
            "recidiviz-staging",
            simulation_tag,
            output_data,
            excluded_pop_data,
            data_inputs["total_population_data"],
        )

    def upload_policy_simulation_results_to_bq(
        self,
        simulation_tag: Optional[str] = None,
        cost_multipliers: Optional[pd.DataFrame] = None,
    ) -> Optional[Dict[str, pd.DataFrame]]:
        output_data = self.validator.get_output_data_for_upload()
        sub_group_ids_dict = self.simulator.get_sub_group_ids_dict()
        data_inputs = self.initializer.get_data_inputs()
        disaggregation_axes = data_inputs["disaggregation_axes"]

        return self.exporter.upload_policy_simulation_results_to_bq(
            "recidiviz-staging",
            simulation_tag,
            output_data,
            cost_multipliers if cost_multipliers is not None else pd.DataFrame(),
            sub_group_ids_dict,
            disaggregation_axes,
        )

    def get_arima_output_df(self, simulation_title: str) -> pd.DataFrame:
        return self.validator.gen_arima_output_df(simulation_title)

    def get_arima_output_plots(
        self,
        simulation_title: str,
        fig_size: Tuple[int, int] = (8, 6),
        by_simulation_group: bool = False,
    ) -> List[plt.subplot]:
        return self.validator.gen_arima_output_plots(
            simulation_title, fig_size, by_simulation_group
        )

    def get_outflows_error(self, simulation_title: str) -> pd.DataFrame:
        outflows_data = self.initializer.get_outflows_for_error()
        return self.validator.calculate_outflows_error(simulation_title, outflows_data)

    def get_total_population_error(self, simulation_tag: str) -> pd.DataFrame:
        return self.validator.gen_total_population_error(simulation_tag)

    def get_full_error_output(self, simulation_tag: str) -> pd.DataFrame:
        return self.validator.gen_full_error_output(simulation_tag)

    def calculate_baseline_transition_error(
        self, validation_pairs: Dict[str, str]
    ) -> pd.DataFrame:
        return self.validator.calculate_baseline_transition_error(validation_pairs)

    def calculate_cohort_hydration_error(
        self,
        output_compartment: str,
        outflow_to: str,
        lower_bound: float = 0,
        upper_bound: float = 2,
        step_size: float = 0.1,
        unit: str = "abs",
    ) -> pd.DataFrame:
        """
        `back_fill_range` is a three item tuple giving the lower and upper bounds to test in units of
            subgroup max_sentence and the step size
        """
        data_inputs = self.initializer.get_data_inputs()
        user_inputs = self.initializer.get_user_inputs()
        max_sentence = self.initializer.get_max_sentence()

        self.simulator.get_cohort_hydration_simulations(
            user_inputs,
            data_inputs,
            int(lower_bound * max_sentence),
            int(upper_bound * max_sentence),
            step_size * max_sentence,
        )

        self.validator.reset(self.simulator.get_population_simulations())
        return self.validator.calculate_cohort_hydration_error(
            output_compartment,
            outflow_to,
            int(lower_bound * max_sentence),
            int(upper_bound * max_sentence),
            step_size * max_sentence,
            unit,
        )
