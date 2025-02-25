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

import os
import unittest
from datetime import datetime
from functools import partial
from typing import Optional

import pandas as pd
from mock import MagicMock, patch
from pandas.testing import assert_frame_equal, assert_index_equal

from recidiviz.calculator.modeling.population_projection.population_simulation.population_simulation import (
    PopulationSimulation,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.super_simulation.super_simulation_factory import (
    SuperSimulation,
    SuperSimulationFactory,
)
from recidiviz.calculator.modeling.population_projection.transition_table import (
    TransitionTable,
)

# pylint: disable=unused-argument

admissions_data_macro = pd.DataFrame(
    {
        "compartment": ["PRETRIAL"] * 12,
        "admission_to": ["PRISON"] * 12,
        "time_step": list(range(5, 11)) * 2,
        "simulation_tag": ["test_data"] * 12,
        "simulation_group": ["NONVIOLENT"] * 6 + ["VIOLENT"] * 6,
        "cohort_population": [100]
        + [100 + 2 * i for i in range(5)]
        + [10]
        + [10 + i for i in range(5)],
    }
)

transitions_data_macro = pd.DataFrame(
    {
        "compartment": ["PRISON", "PRISON", "LIBERTY", "LIBERTY"] * 2,
        "outflow_to": ["LIBERTY", "LIBERTY", "PRISON", "LIBERTY"] * 2,
        "compartment_duration": [3, 5, 3, 50] * 2,
        "simulation_tag": ["test_data"] * 8,
        "simulation_group": ["NONVIOLENT"] * 4 + ["VIOLENT"] * 4,
        "cohort_portion": [0.6, 0.4, 0.3, 0.7] * 2,
    }
)

population_data_macro = pd.DataFrame(
    {
        "compartment": ["PRISON", "LIBERTY"] * 2,
        "time_step": [9] * 4,
        "simulation_tag": ["test_data"] * 4,
        "simulation_group": ["NONVIOLENT"] * 2 + ["VIOLENT"] * 2,
        "compartment_population": [300, 500, 30, 50],
    }
)

data_dict_macro = {
    "admissions_data_raw": admissions_data_macro,
    "transitions_data_raw": transitions_data_macro,
    "population_data_raw": population_data_macro,
}

# population generated with np.random.randint(350, 400, 12)
micro_population = [
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
]
admissions_data_micro = pd.DataFrame(
    {
        "compartment": ["PRETRIAL"] * 24,
        "admission_to": ["PRISON"] * 24,
        "time_step": [datetime(2020, i, 1) for i in range(6, 12)] * 2
        + [datetime(2020, i, 1) for i in range(7, 13)] * 2,
        "state_code": ["test_state"] * 24,
        "run_date": [datetime(2020, 12, 1)] * 12 + [datetime(2021, 1, 1)] * 12,
        "simulation_group": ["MALE"] * 6
        + ["FEMALE"] * 6
        + ["MALE"] * 6
        + ["FEMALE"] * 6,
        "cohort_population": micro_population,
    }
)

admissions_data_micro_missing_time_steps = pd.DataFrame(
    {
        "compartment": ["PRETRIAL"] * 12,
        "admission_to": ["PRISON"] * 12,
        "time_step": [datetime(2020, i, 1) for i in range(6, 12, 2)] * 2
        + [datetime(2020, i, 1) for i in range(7, 13, 2)] * 2,
        "state_code": ["test_state"] * 12,
        "run_date": [datetime(2020, 12, 1)] * 6 + [datetime(2021, 1, 1)] * 6,
        "simulation_group": ["MALE"] * 3
        + ["FEMALE"] * 3
        + ["MALE"] * 3
        + ["FEMALE"] * 3,
        "cohort_population": micro_population[:12],
    }
)

transitions_data_micro = pd.DataFrame(
    {
        "compartment": ["PRISON", "PRISON", "LIBERTY"] * 4,
        "outflow_to": ["LIBERTY", "LIBERTY", "LIBERTY"] * 4,
        "compartment_duration": [3, 5, 3] * 4,
        "state_code": ["test_state"] * 12,
        "run_date": [datetime(2020, 12, 1)] * 6 + [datetime(2021, 1, 1)] * 6,
        "simulation_group": ["MALE"] * 3
        + ["FEMALE"] * 3
        + ["MALE"] * 3
        + ["FEMALE"] * 3,
        "cohort_portion": [0.6, 0.4, 1] * 4,
    }
)

remaining_sentence_data_micro = pd.DataFrame(
    {
        "compartment": ["PRISON", "PRISON", "LIBERTY"] * 4,
        "outflow_to": ["LIBERTY", "LIBERTY", "LIBERTY"] * 4,
        "compartment_duration": [1, 2, 1] * 4,
        "state_code": ["test_state"] * 12,
        "run_date": [datetime(2020, 12, 1)] * 6 + [datetime(2021, 1, 1)] * 6,
        "simulation_group": ["MALE"] * 3
        + ["FEMALE"] * 3
        + ["MALE"] * 3
        + ["FEMALE"] * 3,
        "cohort_portion": [60, 40, 1] * 4,
    }
)

population_data_micro = pd.DataFrame(
    {
        "compartment": ["PRISON", "LIBERTY"] * 4,
        "time_step": [datetime(2020, 12, 1)] * 4 + [datetime(2021, 1, 1)] * 4,
        "state_code": ["test_state"] * 8,
        "run_date": [datetime(2021, 1, 1)] * 8,
        "simulation_group": ["MALE"] * 2
        + ["FEMALE"] * 2
        + ["MALE"] * 2
        + ["FEMALE"] * 2,
        "compartment_population": [300, 500, 430, 410, 200, 250, 300, 350],
    }
)

data_dict_micro = {
    "test_admissions": admissions_data_micro,
    "test_transitions": transitions_data_micro,
    "test_population": population_data_micro,
    "test_remaining_sentences": remaining_sentence_data_micro,
    "test_excluded_population": pd.DataFrame(columns=["state_code", "time_step"]),
    # admissions data with missing values for some time steps
    "test_admissions_missing_time_steps": admissions_data_micro_missing_time_steps,
}


def get_inputs_path(file_name: str) -> str:
    return os.path.join(os.path.dirname(__file__), "test_configurations", file_name)


def mock_load_table_from_big_query_macro(
    table_name: str, simulation_tag: str
) -> pd.DataFrame:
    return data_dict_macro[table_name][
        data_dict_macro[table_name].simulation_tag == simulation_tag
    ]


def mock_upload_policy_results(
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


def mock_load_table_from_big_query_micro_admissions_missing_time_steps(
    project_id: str, dataset: str, table_name: str, state_code: str
) -> pd.DataFrame:
    if table_name == "test_admissions":
        table_name = "test_admissions_missing_time_steps"
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

    macrosim: Optional[SuperSimulation] = None
    microsim: Optional[SuperSimulation] = None
    microsim_excluded_pop: Optional[SuperSimulation] = None

    @classmethod
    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils.load_spark_table_from_big_query",
        mock_load_table_from_big_query_macro,
    )
    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock_load_table_from_big_query_micro,
    )
    def setUpClass(cls) -> None:
        cls.macrosim = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_data_ingest.yaml")
        )
        cls.microsim = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_microsim_model_inputs.yaml")
        )
        cls.microsim_excluded_pop = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_microsim_excluded_pop_model_inputs.yaml")
        )
        for sim in [cls.microsim, cls.microsim_excluded_pop]:
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
        assert isinstance(self.macrosim, SuperSimulation)
        data_inputs = self.macrosim.initializer.get_data_inputs()
        self.assertFalse(data_inputs.admissions_data.empty)
        self.assertFalse(data_inputs.transitions_data.empty)
        self.assertFalse(data_inputs.population_data.empty)

    def test_microsim_data_hydrated(self) -> None:
        """Tests microsimulation are properly ingesting data from BQ"""
        assert isinstance(self.microsim, SuperSimulation)
        data_inputs = self.microsim.initializer.get_data_inputs()
        self.assertFalse(data_inputs.admissions_data.empty)
        self.assertFalse(data_inputs.transitions_data.empty)
        self.assertFalse(data_inputs.population_data.empty)
        self.assertFalse(data_inputs.microsim_data.empty)

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.bq_utils.upload_baseline_simulation_results"
    )
    def test_a_microsim_upload(self, mock_store_simulation_results: MagicMock) -> None:
        assert isinstance(self.microsim, SuperSimulation)
        assert isinstance(self.microsim_excluded_pop, SuperSimulation)
        simulations = {
            "no_excluded_pop": self.microsim,
            "with_excluded_pop": self.microsim_excluded_pop,
        }
        for simulation_name, sim in simulations.items():
            sim.upload_baseline_simulation_results_to_bq(simulation_tag="baseline")
            self.assertTrue(
                mock_store_simulation_results.called,
                f"Simulation '{simulation_name} did not call `store_simulation_results`",
            )

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock_load_table_from_big_query_no_remaining_data,
    )
    def test_b_using_remaining_sentences_reduces_prison_population(self) -> None:
        """Tests microsim is using remaining sentence data in the right way"""
        assert isinstance(self.microsim, SuperSimulation)
        microsim = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_microsim_excluded_pop_model_inputs.yaml")
        )
        microsim.simulate_baseline(["PRISON"])

        # get time before starting cohort filters out of prison
        transitions_data = self.microsim.initializer.get_data_inputs().transitions_data
        affected_time_frame = transitions_data[
            (transitions_data.compartment == "PRISON")
            & (transitions_data.cohort_portion > 0)
        ].compartment_duration.max()

        # get projected prison population from simulation substituting transitions data for remaining sentences
        substitute_outputs = microsim.simulator.pop_simulations[
            "baseline_projections"
        ].population_projections
        substitute_prison_population = (
            substitute_outputs[
                (substitute_outputs.compartment == "PRISON")
                & (
                    substitute_outputs.time_step
                    > microsim.initializer.user_inputs.start_time_step + 1
                )
                & (
                    substitute_outputs.time_step
                    - microsim.initializer.user_inputs.start_time_step
                    < affected_time_frame
                )
            ]
            .groupby("time_step")
            .sum()
            .compartment_population
        )

        # get projected prison population from regular simulation
        regular_outputs = self.microsim.validator.pop_simulations[
            "baseline_projections"
        ].population_projections
        regular_prison_population = (
            regular_outputs[
                (regular_outputs.compartment == "PRISON")
                & (
                    regular_outputs.time_step
                    > self.microsim.initializer.user_inputs.start_time_step + 1
                )
                & (
                    regular_outputs.time_step
                    - self.microsim.initializer.user_inputs.start_time_step
                    < affected_time_frame
                )
            ]
            .groupby("time_step")
            .sum()
            .compartment_population
        )

        self.assertTrue(
            (substitute_prison_population > regular_prison_population).all()
        )

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock_load_table_from_big_query_micro,
    )
    def test_c_user_inputted_cross_flow_equivalent_to_default(self) -> None:
        """
        Tests that the same cross flow function operates the same when accessed through
            the PopulationSimulation object vs through the user override.
        """
        assert isinstance(self.microsim, SuperSimulation)
        regular_outputs = self.microsim.validator.pop_simulations[
            "baseline_projections"
        ].population_projections.copy()

        self.microsim.override_cross_flow_function(
            PopulationSimulation.update_attributes_identity
        )
        self.microsim.simulate_baseline(["PRISON"])

        overridden_outputs = self.microsim.validator.pop_simulations[
            "baseline_projections"
        ].population_projections

        assert_frame_equal(regular_outputs, overridden_outputs)

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.bq_utils.upload_policy_simulation_results",
        mock_upload_policy_results,
    )
    def test_d_cost_multipliers_multiplicative(self) -> None:
        # test doubling multiplier doubles costs
        assert isinstance(self.macrosim, SuperSimulation)
        policy_function = partial(
            TransitionTable.apply_reductions,
            reduction_df=pd.DataFrame(
                {
                    "outflow": ["LIBERTY"],
                    "reduction_size": [0.5],
                    "affected_fraction": [0.75],
                }
            ),
            reduction_type="*",
            retroactive=True,
        )
        cost_multipliers = pd.DataFrame(
            {"simulation_group": ["NONVIOLENT", "VIOLENT"], "multiplier": [2, 2]}
        )

        policy_list = [
            SparkPolicy(
                policy_fn=policy_function,
                spark_compartment="PRISON",
                simulation_group=crime_type,
                policy_time_step=self.macrosim.initializer.get_user_inputs().start_time_step
                + 5,
                apply_retroactive=True,
            )
            for crime_type in ["NONVIOLENT", "VIOLENT"]
        ]
        self.macrosim.simulate_policy(policy_list, "PRISON")

        outputs = self.macrosim.upload_policy_simulation_results_to_bq("test")
        assert outputs
        spending_diff, spending_diff_non_cumulative = (
            outputs["spending_diff"],
            outputs["spending_diff_non_cumulative"],
        )
        outputs_scaled = self.macrosim.upload_policy_simulation_results_to_bq(
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
            {"simulation_group": ["NONVIOLENT"], "multiplier": [2]}
        )
        partial_cost_multipliers_triple = pd.DataFrame(
            {"simulation_group": ["NONVIOLENT"], "multiplier": [3]}
        )
        outputs_doubled = self.macrosim.upload_policy_simulation_results_to_bq(
            "test", partial_cost_multipliers_double
        )
        assert outputs_doubled
        spending_diff_double, spending_diff_non_cumulative_double = (
            outputs_doubled["spending_diff"],
            outputs_doubled["spending_diff_non_cumulative"],
        )

        outputs_tripled = self.macrosim.upload_policy_simulation_results_to_bq(
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

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock_load_table_from_big_query_micro,
    )
    def test_e_microsim_baseline_over_time_zero_error_for_first_time_step(self) -> None:
        """Tests the microsim is initialized with 0 percent error for each initial time step"""
        assert isinstance(self.microsim, SuperSimulation)
        # Run 2 simulations over different run dates
        run_dates = pd.date_range(
            datetime(2020, 12, 1), datetime(2021, 1, 1), freq="MS"
        ).tolist()
        self.microsim.microsim_baseline_over_time(run_dates)

        # Test each simulation has 0 percent error for the first time step
        for key, _ in self.microsim.simulator.pop_simulations.items():
            population_error = self.microsim.get_full_error_output(key)
            first_time_step = population_error.index.get_level_values(
                level="time_step"
            ).min()
            initial_error = population_error.unstack("compartment").loc[
                first_time_step, "percent_error"
            ]
            # Error should be 0 for each compartment/simulation group on the first ts
            self.assertTrue((initial_error == 0).all())

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock_load_table_from_big_query_micro_admissions_missing_time_steps,
    )
    def test_initializer_fills_missing_time_steps_in_admissions(self) -> None:
        """Tests the SuperSimulation initialization fills in missing admissions with 0s"""

        microsim = SuperSimulationFactory.build_super_simulation(
            get_inputs_path("super_simulation_microsim_model_inputs.yaml")
        )
        data_inputs = microsim.initializer.get_data_inputs()
        comparison_columns = [
            "time_step",
            "simulation_group",
            "compartment",
            "admission_to",
            "run_date",
            "cohort_population",
        ]
        expected_admissions_data = (
            pd.concat(
                [
                    admissions_data_micro_missing_time_steps[
                        admissions_data_micro_missing_time_steps.run_date
                        == datetime(2021, 1, 1)
                    ][comparison_columns],
                    pd.DataFrame(
                        {
                            "time_step": [datetime(2020, 8, 1), datetime(2020, 10, 1)]
                            * 2,
                            "simulation_group": ["FEMALE"] * 2 + ["MALE"] * 2,
                            "compartment": ["PRETRIAL"] * 4,
                            "admission_to": ["PRISON"] * 4,
                            "run_date": [datetime(2021, 1, 1)] * 4,
                            "cohort_population": [0.0] * 4,
                        }
                    ),
                ]
            )
            .sort_values(by=["simulation_group", "time_step"])
            .reset_index(drop=True)
        )
        expected_admissions_data["time_step"] = expected_admissions_data[
            "time_step"
        ].apply(microsim.initializer.time_converter.convert_timestamp_to_time_step)
        # Test the two dfs are the same but ignore the index
        assert_frame_equal(
            data_inputs.admissions_data.reset_index(drop=True), expected_admissions_data
        )

    @patch(
        "recidiviz.calculator.modeling.population_projection.utils.ignite_bq_utils.load_ignite_table_from_big_query",
        mock_load_table_from_big_query_micro,
    )
    def test_g_microsim_access_inputs(self) -> None:
        """Tests SuperSimulation's get_transitions_data_input fn returns correct value, compartments case-insensitive"""
        assert isinstance(self.microsim, SuperSimulation)
        print(self.microsim.initializer.get_data_inputs().transitions_data)
        expected_transitions_data = (
            self.microsim.initializer.get_data_inputs().transitions_data[
                [
                    "compartment",
                    "outflow_to",
                    "simulation_group",
                    "compartment_duration",
                    "cohort_portion",
                ]
            ]
        )
        transitions_returned = self.microsim.get_transitions_data_input()
        assert_frame_equal(transitions_returned, expected_transitions_data)

        prison_transitions_returned = self.microsim.get_transitions_data_input("prison")
        assert_frame_equal(
            prison_transitions_returned,
            expected_transitions_data[
                expected_transitions_data.compartment == "PRISON"
            ],
        )

        prison_and_liberty_transitions_returned = (
            self.microsim.get_transitions_data_input("prIsOn", "LiBErty")
        )
        assert_frame_equal(
            prison_and_liberty_transitions_returned,
            expected_transitions_data[
                expected_transitions_data.compartment.isin(["PRISON", "LIBERTY"])
            ],
        )

    def test_simulate_baseline_basic_functionality(self) -> None:
        """Ensure baseline simulation runs without breaking and can return all expected outputs."""
        self.macrosim.simulate_baseline([])  # type: ignore[union-attr]

        # check you can extract pop projections and they are as expected
        pop_projections = self.macrosim.get_population_projections(  # type: ignore[union-attr]
            "baseline_projections"
        )
        assert_index_equal(
            pop_projections.columns,
            pd.Index(
                [
                    "compartment",
                    "compartment_population",
                    "time_step",
                    "simulation_group",
                ]
            ),
        )
        assert set(pop_projections.compartment) == {"LIBERTY", "PRISON"}
        assert set(pop_projections.simulation_group) == {"NONVIOLENT", "VIOLENT"}

        # check that arima outputs can be extracted and are as expected
        arimas = self.macrosim.get_arima_output_df("baseline_projections")  # type: ignore[union-attr]
        assert_index_equal(arimas.columns, pd.Index(["actuals", "predictions"]))
        assert set(arimas.index.get_level_values("compartment")) == {"PRETRIAL"}
        assert set(arimas.index.get_level_values("simulation_group")) == {
            "NONVIOLENT",
            "VIOLENT",
        }
        assert set(arimas.index.get_level_values("admission_to")) == {"PRISON"}

        # run just to make sure it isn't broken
        self.macrosim.get_arima_output_plots("baseline_projections")  # type: ignore[union-attr]

        # check that outflows output can be extracted and is as expected
        outflows = self.macrosim.get_all_outflows_tables()  # type: ignore[union-attr]
        assert_index_equal(
            outflows.columns,
            pd.Index(
                [
                    "policy_sim",
                    "simulation_group",
                    "compartment",
                    "time_step",
                    "outflow_to",
                    "outflows",
                ]
            ),
        )
        assert set(outflows.compartment) == {"LIBERTY", "PRISON", "PRETRIAL"}
        assert set(outflows.outflow_to) == {"LIBERTY", "PRISON"}
        assert set(outflows.simulation_group) == {"NONVIOLENT", "VIOLENT"}

        # check that admissions error output can be extracted and is as expected
        admissions_error = self.macrosim.get_admissions_error("baseline_projections")  # type: ignore[union-attr]
        assert_index_equal(admissions_error.columns, pd.Index(["model", "actual"]))
        assert set(admissions_error.index.get_level_values("compartment")) == {
            "PRETRIAL"
        }
        assert set(admissions_error.index.get_level_values("admission_to")) == {
            "PRISON"
        }

        # check that population error output can be extracted and is as expected
        population_error = self.macrosim.get_population_error("baseline_projections")  # type: ignore[union-attr]
        assert_index_equal(population_error.columns, pd.Index(["PRISON", "LIBERTY"]))

        # check that full error output can be extracted and is as expected
        full_error = self.macrosim.get_full_error_output("baseline_projections")  # type: ignore[union-attr]
        assert_index_equal(
            full_error.columns,
            pd.Index(
                [
                    "simulation_population",
                    "historical_population",
                    "percent_error",
                    "compartment_type",
                ]
            ),
        )
        assert set(full_error.index.get_level_values("compartment")) == {
            "LIBERTY",
            "PRISON",
        }

        # TODO(#23946): No check for cohort hydration error because appears to be currently broken
        #   --> fix that then add test

    def test_input_data_access(self) -> None:
        """Test that data inputs are accessible through a SuperSimulation object"""
        transitions_input = transitions_data_macro.drop("simulation_tag", axis=1)
        assert_frame_equal(
            transitions_input,
            self.macrosim.get_transitions_data_input()[transitions_input.columns],  # type: ignore[union-attr]
        )
        admissions_input = admissions_data_macro.drop("simulation_tag", axis=1)
        assert_frame_equal(
            admissions_input,
            self.macrosim.get_admissions_data_input()[admissions_input.columns],  # type: ignore[union-attr]
        )
