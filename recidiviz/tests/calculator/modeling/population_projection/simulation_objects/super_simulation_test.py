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
"""Test the SuperSimulation object"""

import unittest
from datetime import datetime
import os
from functools import partial
from mock import patch, MagicMock
import pandas as pd
from pandas.testing import assert_frame_equal

from recidiviz.calculator.modeling.population_projection.simulations.super_simulation.super_simulation_factory import (
    SuperSimulationFactory,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.simulations.compartment_transitions import (
    CompartmentTransitions,
)

# pylint: disable=unused-argument


outflows_data_macro = pd.DataFrame(
    {
        "compartment": ["PRETRIAL"] * 12,
        "outflow_to": ["PRISON"] * 12,
        "time_step": list(range(5, 11)) * 2,
        "simulation_tag": ["test_data"] * 12,
        "crime_type": ["NONVIOLENT"] * 6 + ["VIOLENT"] * 6,
        "total_population": [100]
        + [100 + 2 * i for i in range(5)]
        + [10]
        + [10 + i for i in range(5)],
    }
)

transitions_data_macro = pd.DataFrame(
    {
        "compartment": ["PRISON", "PRISON", "RELEASE", "RELEASE"] * 2,
        "outflow_to": ["RELEASE", "RELEASE", "PRISON", "RELEASE"] * 2,
        "compartment_duration": [3, 5, 3, 50] * 2,
        "simulation_tag": ["test_data"] * 8,
        "crime_type": ["NONVIOLENT"] * 4 + ["VIOLENT"] * 4,
        "total_population": [0.6, 0.4, 0.3, 0.7] * 2,
    }
)

total_population_data_macro = pd.DataFrame(
    {
        "compartment": ["PRISON", "RELEASE"] * 2,
        "time_step": [9] * 4,
        "simulation_tag": ["test_data"] * 4,
        "crime_type": ["NONVIOLENT"] * 2 + ["VIOLENT"] * 2,
        "total_population": [300, 500, 30, 50],
    }
)

data_dict_macro = {
    "outflows_data_raw": outflows_data_macro,
    "transitions_data_raw": transitions_data_macro,
    "total_population_data_raw": total_population_data_macro,
}

outflows_data_micro = pd.DataFrame(
    {
        "compartment": ["PRETRIAL"] * 24,
        "outflow_to": ["PRISON"] * 24,
        "time_step": [datetime(2020, i, 1) for i in range(6, 12)] * 2
        + [datetime(2020, i, 1) for i in range(7, 13)] * 2,
        "state_code": ["test_state"] * 24,
        "run_date": [datetime(2020, 12, 1)] * 12 + [datetime(2021, 1, 1)] * 12,
        "gender": ["MALE"] * 6 + ["FEMALE"] * 6 + ["MALE"] * 6 + ["FEMALE"] * 6,
        # total_population generated with np.random.randint(350, 400, 24)
        "total_population": [
            376,
            353,
            375,
            358,
            352,
            355,
            388,
            372,
            375,
            351,
            365,
            363,
            361,
            366,
            382,
            369,
            363,
            371,
            361,
            385,
            373,
            352,
            389,
            392,
        ],
    }
)

transitions_data_micro = pd.DataFrame(
    {
        "compartment": ["PRISON", "PRISON", "RELEASE"] * 4,
        "outflow_to": ["RELEASE", "RELEASE", "RELEASE"] * 4,
        "compartment_duration": [3, 5, 3] * 4,
        "state_code": ["test_state"] * 12,
        "run_date": [datetime(2020, 12, 1)] * 6 + [datetime(2021, 1, 1)] * 6,
        "gender": ["MALE"] * 3 + ["FEMALE"] * 3 + ["MALE"] * 3 + ["FEMALE"] * 3,
        "total_population": [0.6, 0.4, 1] * 4,
    }
)

remaining_sentence_data_micro = pd.DataFrame(
    {
        "compartment": ["PRISON", "PRISON", "RELEASE"] * 4,
        "outflow_to": ["RELEASE", "RELEASE", "RELEASE"] * 4,
        "compartment_duration": [1, 2, 1] * 4,
        "state_code": ["test_state"] * 12,
        "run_date": [datetime(2020, 12, 1)] * 6 + [datetime(2021, 1, 1)] * 6,
        "gender": ["MALE"] * 3 + ["FEMALE"] * 3 + ["MALE"] * 3 + ["FEMALE"] * 3,
        "total_population": [60, 40, 1] * 4,
    }
)

total_population_data_micro = pd.DataFrame(
    {
        "compartment": ["PRISON", "RELEASE"] * 4,
        "time_step": [datetime(2020, 12, 1)] * 4 + [datetime(2021, 1, 1)] * 4,
        "state_code": ["test_state"] * 8,
        "run_date": [datetime(2021, 1, 1)] * 8,
        "gender": ["MALE"] * 2 + ["FEMALE"] * 2 + ["MALE"] * 2 + ["FEMALE"] * 2,
        "total_population": [300, 500, 430, 410, 200, 250, 300, 350],
    }
)

data_dict_micro = {
    "test_outflows": outflows_data_micro,
    "test_transitions": transitions_data_micro,
    "test_total_population": total_population_data_micro,
    "test_remaining_sentences": remaining_sentence_data_micro,
    "test_excluded_population": pd.DataFrame(columns=["state_code"]),
}


def get_inputs_path(file_name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "test_configurations", file_name)


def mock_load_table_from_big_query_macro(
    table_name: str, simulation_tag: str
) -> pd.DataFrame:
    return data_dict_macro[table_name][
        data_dict_macro[table_name].simulation_tag == simulation_tag
    ]


def mock_upload_spark_results(
    project_id: str,
    simulation_tag: str,
    cost_avoidance_df: pd.DataFrame,
    life_years_df: pd.DataFrame,
    population_change_df: pd.DataFrame,
    cost_avoidance_non_cumulative_df: pd.DataFrame,
) -> None:
    pass


def mock_load_table_from_big_query_micro(
    project_id: str, dataset: str, table_name: str, state_code: str
) -> pd.DataFrame:
    return data_dict_micro[table_name][
        data_dict_micro[table_name]["state_code"] == state_code
    ]


def mock_load_table_from_big_query_no_remaining_data(
    project_id: str, dataset: str, table_name: str, state_code: str
) -> pd.DataFrame:
    if table_name == "test_remaining_sentences":
        table_name = "test_transitions"
    return data_dict_micro[table_name][
        data_dict_micro[table_name]["state_code"] == state_code
    ]


class TestSuperSimulation(unittest.TestCase):
    """Test the SuperSimulation object runs correctly"""

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils.load_spark_table_from_big_query",
        mock_load_table_from_big_query_macro,
    )
    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock_load_table_from_big_query_micro,
    )
    def setUp(self) -> None:
        self.macrosim = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_data_ingest.yaml")
        )
        self.microsim = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_microsim_model_inputs.yaml")
        )
        self.microsim_excluded_pop = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_microsim_excluded_pop_model_inputs.yaml")
        )
        for sim in [self.microsim, self.microsim_excluded_pop]:
            sim.simulate_baseline(["PRISON"])

    def test_simulation_architecture_must_match_compartment_costs(self) -> None:
        with self.assertRaises(ValueError):
            SuperSimulationFactory.build_super_simulation(
                get_inputs_path("super_simulation_mismatched_compartments.yaml")
            )

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils.load_spark_table_from_big_query",
        mock_load_table_from_big_query_macro,
    )
    def test_reference_year_must_be_integer_time_steps_from_start_year(self) -> None:
        """Tests macrosimulation enforces compatibility of start year and time step"""
        with self.assertRaises(ValueError):
            SuperSimulationFactory.build_super_simulation(
                get_inputs_path("super_simulation_broken_start_year_model_inputs.yaml")
            )
        with self.assertRaises(ValueError):
            SuperSimulationFactory.build_super_simulation(
                get_inputs_path("super_simulation_broken_time_step_model_inputs.yaml")
            )

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils.load_spark_table_from_big_query",
        mock_load_table_from_big_query_macro,
    )
    def test_macrosim_data_hydrated(self) -> None:
        """Tests macrosimulation are properly ingesting data from BQ"""
        self.assertFalse(self.macrosim.initializer.data_dict["outflows_data"].empty)
        self.assertFalse(self.macrosim.initializer.data_dict["transitions_data"].empty)
        self.assertFalse(
            self.macrosim.initializer.data_dict["total_population_data"].empty
        )

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils.upload_spark_results",
        mock_upload_spark_results,
    )
    def test_cost_multipliers_multiplicative(self) -> None:
        # test doubling multiplier doubles costs
        policy_function = partial(
            CompartmentTransitions.apply_reduction,
            reduction_df=pd.DataFrame(
                {
                    "outflow": ["RELEASE"],
                    "reduction_size": [0.5],
                    "affected_fraction": [0.75],
                }
            ),
            reduction_type="*",
            retroactive=True,
        )
        cost_multipliers = pd.DataFrame(
            {"crime_type": ["NONVIOLENT", "VIOLENT"], "multiplier": [2, 2]}
        )

        policy_list = [
            SparkPolicy(
                policy_fn=policy_function,
                spark_compartment="PRISON",
                sub_population={"crime_type": crime_type},
                apply_retroactive=True,
            )
            for crime_type in ["NONVIOLENT", "VIOLENT"]
        ]
        self.macrosim.simulate_policy(policy_list, "PRISON")

        outputs = self.macrosim.upload_simulation_results_to_bq("test")
        assert outputs
        spending_diff, spending_diff_non_cumulative = (
            outputs["spending_diff"],
            outputs["spending_diff_non_cumulative"],
        )
        outputs_scaled = self.macrosim.upload_simulation_results_to_bq(
            "test", cost_multipliers
        )
        assert outputs_scaled
        spending_diff_scaled, spending_diff_non_cumulative_scaled = (
            outputs_scaled["spending_diff"],
            outputs_scaled["spending_diff_non_cumulative"],
        )

        assert_frame_equal(spending_diff * 2, spending_diff_scaled)
        assert_frame_equal(
            spending_diff_non_cumulative * 2, spending_diff_non_cumulative_scaled
        )

        # same test but for only one subgroup
        partial_cost_multipliers_double = pd.DataFrame(
            {"crime_type": ["NONVIOLENT"], "multiplier": [2]}
        )
        partial_cost_multipliers_triple = pd.DataFrame(
            {"crime_type": ["NONVIOLENT"], "multiplier": [3]}
        )
        outputs_doubled = self.macrosim.upload_simulation_results_to_bq(
            "test", partial_cost_multipliers_double
        )
        assert outputs_doubled
        spending_diff_double, spending_diff_non_cumulative_double = (
            outputs_doubled["spending_diff"],
            outputs_doubled["spending_diff_non_cumulative"],
        )

        outputs_tripled = self.macrosim.upload_simulation_results_to_bq(
            "test", partial_cost_multipliers_triple
        )
        assert outputs_tripled
        spending_diff_triple, spending_diff_non_cumulative_triple = (
            outputs_tripled["spending_diff"],
            outputs_tripled["spending_diff_non_cumulative"],
        )

        assert_frame_equal(
            (spending_diff_triple - spending_diff),
            (spending_diff_double - spending_diff) * 2,
        )
        assert_frame_equal(
            (spending_diff_non_cumulative_triple - spending_diff_non_cumulative),
            (spending_diff_non_cumulative_double - spending_diff_non_cumulative) * 2,
        )

    def test_microsim_data_hydrated(self) -> None:
        """Tests microsimulation are properly ingesting data from BQ"""
        self.assertFalse(self.microsim.initializer.data_dict["outflows_data"].empty)
        self.assertFalse(self.microsim.initializer.data_dict["transitions_data"].empty)
        self.assertFalse(
            self.microsim.initializer.data_dict["total_population_data"].empty
        )
        self.assertFalse(
            self.microsim.initializer.data_dict["remaining_sentence_data"].empty
        )

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock_load_table_from_big_query_no_remaining_data,
    )
    def test_using_remaining_sentences_reduces_prison_population(self) -> None:
        """Tests microsim is using remaining sentence data in the right way"""
        microsim = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_microsim_excluded_pop_model_inputs.yaml")
        )
        microsim.simulate_baseline(["PRISON"])

        # get time before starting cohort filters out of prison
        affected_time_frame = self.microsim.initializer.data_dict["transitions_data"][
            (
                self.microsim.initializer.data_dict["transitions_data"].compartment
                == "PRISON"
            )
            & (
                self.microsim.initializer.data_dict["transitions_data"].total_population
                > 0
            )
        ].compartment_duration.max()

        # get projected prison population from simulation substituting transitions data for remaining sentences
        substitute_outputs = microsim.simulator.pop_simulations[
            "baseline_middle"
        ].population_projections
        substitute_prison_population = (
            substitute_outputs[
                (substitute_outputs.compartment == "PRISON")
                & (
                    substitute_outputs.time_step
                    > microsim.initializer.user_inputs["start_time_step"]
                )
                & (
                    substitute_outputs.time_step
                    - microsim.initializer.user_inputs["start_time_step"]
                    < affected_time_frame
                )
            ]
            .groupby("time_step")
            .sum()
            .total_population
        )

        # get projected prison population from regular simulation
        regular_outputs = self.microsim.validator.pop_simulations[
            "baseline_middle"
        ].population_projections
        regular_prison_population = (
            regular_outputs[
                (regular_outputs.compartment == "PRISON")
                & (
                    regular_outputs.time_step
                    > self.microsim.initializer.user_inputs["start_time_step"]
                )
                & (
                    regular_outputs.time_step
                    - self.microsim.initializer.user_inputs["start_time_step"]
                    < affected_time_frame
                )
            ]
            .groupby("time_step")
            .sum()
            .total_population
        )

        self.assertTrue(
            (substitute_prison_population > regular_prison_population).all()
        )

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock_load_table_from_big_query_no_remaining_data,
    )
    def test_microsim_baseline_over_time_zero_error_for_first_ts(self) -> None:
        """Tests the microsim is initialized with 0 percent error for each initial time step"""

        # Run 2 simulations over different run dates
        run_dates = pd.date_range(
            datetime(2020, 12, 1), datetime(2021, 1, 1), freq="MS"
        ).tolist()
        self.microsim.microsim_baseline_over_time(run_dates)

        # Test each simulation has 0 percent error for the first time step
        for key, _ in self.microsim.simulator.pop_simulations.items():
            total_population_error = self.microsim.get_full_error_output(key)
            first_ts = total_population_error.index.get_level_values(
                level="time_step"
            ).min()
            initial_error = total_population_error.unstack("compartment").loc[
                first_ts, "percent_error"
            ]
            # Error should be 0 for each compartment/simulation group on the first ts
            self.assertTrue((initial_error == 0).all())

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.store_simulation_results"
    )
    def test_microsim_upload(self, mock_store_simulation_results: MagicMock) -> None:
        simulations = {
            "no_excluded_pop": self.microsim,
            "with_excluded_pop": self.microsim_excluded_pop,
        }
        for simulation_name, sim in simulations.items():
            sim.upload_simulation_results_to_bq(
                simulation_tag="baseline", cost_multipliers=pd.DataFrame()
            )
            self.assertTrue(
                mock_store_simulation_results.called,
                f"Simulation '{simulation_name} did not call `store_simulation_results`",
            )
