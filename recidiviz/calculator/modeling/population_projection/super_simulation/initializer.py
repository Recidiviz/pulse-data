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
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.super_simulation.time_converter import (
    TimeConverter,
)
from recidiviz.calculator.modeling.population_projection.utils import (
    ignite_bq_utils,
    spark_bq_utils,
)


class Initializer:
    """Manage model inputs to SuperSimulation."""

    MISSING_EVENT_THRESHOLD = 0.25

    def __init__(
        self,
        time_converter: TimeConverter,
        yaml_user_inputs: Dict[str, Any],
        data_inputs_params: Dict[str, Any],
        compartments_architecture: Dict[str, str],
        disaggregation_axes: List[str],
        microsim: bool,
    ) -> None:
        self.time_converter = time_converter
        self.microsim = microsim
        self.data_dict: Dict[str, Any] = {}
        self.override_cross_flow_function: Optional[Callable] = None

        self._set_user_inputs(yaml_user_inputs)
        self._initialize_data(
            data_inputs_params, compartments_architecture, disaggregation_axes
        )

    def get_first_relevant_ts(
        self, first_relevant_ts_override: Optional[int] = None
    ) -> int:
        """calculate ts to start model initialization at."""

        if first_relevant_ts_override is not None:
            if self.microsim:
                raise ValueError(
                    "Cannot pass first_relevant_ts_override to micro-simulation"
                )

            return first_relevant_ts_override

        if self.microsim:
            return self.user_inputs["start_time_step"]

        if self.user_inputs["speed_run"]:
            max_sentence = self.user_inputs["projection_time_steps"] + 1
        else:
            max_sentence = max(self.data_dict["transitions_data"].compartment_duration)
        return self.user_inputs["start_time_step"] - int(max_sentence)

    def get_outflows_for_error(self) -> pd.DataFrame:
        """Pull historical outflows data for Validator.calculate_outflows_error"""
        if self.microsim:
            return self.data_dict["outflows_data"][
                self.data_dict["outflows_data"].run_date
                == self.data_dict["outflows_data"].run_date.max()
            ]
        return self.data_dict["outflows_data"]

    def get_data_inputs(
        self, run_date_override: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """Get data inputs to build a PopulationSimulation"""
        if not self.microsim and run_date_override is not None:
            raise ValueError("Cannot override run_date for Macrosimulation.")

        if self.microsim:
            run_date = run_date_override or self.user_inputs["run_date"]
            return {
                "outflows_data": self.data_dict["outflows_data"][
                    self.data_dict["outflows_data"].run_date == run_date
                ],
                "transitions_data": self.data_dict["remaining_sentence_data"][
                    self.data_dict["remaining_sentence_data"].run_date == run_date
                ],
                # Take the most recent total population data
                "total_population_data": self.data_dict["total_population_data"][
                    self.data_dict["total_population_data"].run_date
                    == self.data_dict["total_population_data"].run_date.max()
                ],
                "compartments_architecture": self.data_dict[
                    "compartments_architecture"
                ],
                "disaggregation_axes": self.data_dict["disaggregation_axes"],
                "microsim_data": self.data_dict["transitions_data"][
                    self.data_dict["transitions_data"].run_date == run_date
                ],
                "should_initialize_compartment_populations": True,
                "should_scale_populations_after_step": False,
                "override_cross_flow_function": self.override_cross_flow_function,
            }

        return {
            "outflows_data": self.data_dict["outflows_data"],
            "transitions_data": self.data_dict["transitions_data"],
            "total_population_data": self.data_dict["total_population_data"],
            "compartments_architecture": self.data_dict["compartments_architecture"],
            "disaggregation_axes": self.data_dict["disaggregation_axes"],
            "microsim_data": pd.DataFrame(),
            "should_initialize_compartment_populations": False,
            "should_scale_populations_after_step": True,
            "override_cross_flow_function": self.override_cross_flow_function,
        }

    def get_user_inputs(self) -> Dict[str, Any]:
        return self.user_inputs

    def get_max_sentence(self) -> int:
        return self.user_inputs["start_time_step"] - self.get_first_relevant_ts(None)

    def get_excluded_pop_data(self) -> pd.DataFrame:
        return self.data_dict["excluded_population_data"]

    def get_inputs_for_calculate_prep_scale_factor(
        self,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        excluded_pop = self.data_dict["excluded_population_data"]
        excluded_pop = excluded_pop[
            (excluded_pop.time_step == self.user_inputs["start_time_step"])
            & (excluded_pop.run_date == self.user_inputs["run_date"])
        ]
        total_pop = self.data_dict["total_population_data"]
        total_pop = total_pop[
            (total_pop.time_step == self.user_inputs["start_time_step"])
            & (total_pop.run_date == self.user_inputs["run_date"])
        ]
        return excluded_pop, total_pop

    def get_inputs_for_microsim_baseline_over_time(
        self, start_run_dates: List[datetime]
    ) -> Tuple[Dict[datetime, Dict[str, Any]], Dict[datetime, int]]:
        data_input_dict = {}
        first_relevant_ts_dict = {}
        for start_date in start_run_dates:
            data_input_dict[start_date] = self.get_data_inputs(start_date)
            first_relevant_ts_dict[
                start_date
            ] = self.time_converter.convert_timestamp_to_time_step(start_date)
        return data_input_dict, first_relevant_ts_dict

    def set_override_cross_flow_function(self, cross_flow_function: Callable) -> None:
        self.override_cross_flow_function = cross_flow_function

    def _fully_connect_outflows(self, outflows_data: pd.DataFrame) -> pd.DataFrame:
        """Helper function for get_data_inputs that ensures outflows_data is fully connected."""
        # Handle sparse outflow events where a disaggregation is missing data for some time steps
        if self.microsim:
            if len(outflows_data.run_date.unique()) != 1:
                raise ValueError(
                    f"outflows_data must have unique run date. "
                    f"Given run dates: {outflows_data.run_date.unique()}"
                )
            run_date = outflows_data.iloc[0].run_date

        fully_connected_columns = self.data_dict["disaggregation_axes"] + [
            "compartment",
            "outflow_to",
            "time_step",
        ]

        # For each compartment that has outflows data, complete data so that every time step between
        # MIN(time_step) and MAX(time_step) has data (i.e. fill in 0s)
        fully_connected_data = pd.DataFrame()
        for compartment in outflows_data.compartment.unique():
            # pick out min and max time_step and re-index on everything in between
            compartment_data = outflows_data[outflows_data.compartment == compartment]
            connected_time_steps = range(
                compartment_data.time_step.min(), compartment_data.time_step.max() + 1
            )
            connected_compartment_data = (
                compartment_data.groupby(fully_connected_columns)
                .total_population.sum()
                .unstack(level="time_step")
                .transpose()
                .reindex(connected_time_steps)
            )

            # Raise a warning if there are disaggregations without outflow records for more than 25% of the time steps
            number_of_missing_events = connected_compartment_data.isnull().sum()
            sparse_disaggregations = number_of_missing_events[
                number_of_missing_events / len(connected_compartment_data)
                > self.MISSING_EVENT_THRESHOLD
            ] / len(connected_compartment_data)
            if not sparse_disaggregations.empty:
                logging.warning(
                    "Outflows data is missing for more than %s%% for some disaggregations:"
                    "\n%s%%",
                    100 * self.MISSING_EVENT_THRESHOLD,
                    100 * sparse_disaggregations,
                )

            # Fill the total population with 0 and remove the multiindex for the population simulation
            fully_connected_data = fully_connected_data.append(
                connected_compartment_data.fillna(0)
                .transpose()
                .stack("time_step")
                .reset_index(name="total_population")
            )

        # add run_date back in after losing it in the groupby
        if self.microsim:
            fully_connected_data["run_date"] = run_date

        return fully_connected_data

    def _set_user_inputs(self, yaml_user_inputs: Dict[str, Any]) -> None:
        """Populate the simulation user inputs"""
        self.user_inputs = {}
        self.user_inputs[
            "start_time_step"
        ] = self.time_converter.convert_year_to_time_step(
            yaml_user_inputs["start_year"]
        )
        self.user_inputs[
            "projection_time_steps"
        ] = self.time_converter.get_num_time_steps(yaml_user_inputs["projection_years"])
        if not np.isclose(
            self.user_inputs["projection_time_steps"],
            round(self.user_inputs["projection_time_steps"]),
        ):
            raise ValueError(
                f"Projection years {yaml_user_inputs['projection_years']} input cannot be evenly divided "
                f"by time step {self.time_converter.get_time_step()}"
            )
        self.user_inputs["projection_time_steps"] = round(
            self.user_inputs["projection_time_steps"]
        )

        # Load all optional arguments, set them to the default value if not provided in the initialization params
        self.user_inputs["constant_admissions"] = yaml_user_inputs.get(
            "constant_admissions", False
        )

        if self.microsim:
            self.user_inputs["run_date"] = yaml_user_inputs["run_date"]
        else:
            self.user_inputs["speed_run"] = yaml_user_inputs.get("speed_run", False)

    def _initialize_data(
        self,
        data_inputs_params: Dict[str, Any],
        compartments_architecture: Dict[str, str],
        disaggregation_axes: List[str],
    ) -> None:
        """Hydrate data_dict from Big Query"""
        # TODO(#5185): replace data_dict with data class
        self.data_dict["compartments_architecture"] = compartments_architecture
        self.data_dict["disaggregation_axes"] = disaggregation_axes
        if self.microsim:
            self._initialize_data_micro(data_inputs_params)
            sparse_outflows = self.data_dict["outflows_data"]
            outflows = pd.DataFrame()
            for run_date in sparse_outflows.run_date.unique():
                outflows = outflows.append(
                    self._fully_connect_outflows(
                        sparse_outflows[sparse_outflows.run_date == run_date]
                    )
                )
        else:
            self._initialize_data_macro(data_inputs_params)
            outflows = self._fully_connect_outflows(self.data_dict["outflows_data"])

        self.data_dict["outflows_data"] = outflows

    def _initialize_data_micro(self, data_inputs_params: Dict[str, Any]) -> None:
        """Helper function for _initialize_data()"""
        big_query_params = data_inputs_params["big_query_inputs"]
        project_id = big_query_params["project_id"]
        dataset = big_query_params["input_dataset"]
        state_code = big_query_params["state_code"]

        input_data_tables = [
            "outflows_data",
            "transitions_data",
            "total_population_data",
            "remaining_sentence_data",
        ]

        if "excluded_population_data" in big_query_params:
            input_data_tables.append("excluded_population_data")
        else:
            self.data_dict["excluded_population_data"] = pd.DataFrame()

        for table_key in input_data_tables:
            table_name = big_query_params[table_key]
            table_data = ignite_bq_utils.load_ignite_table_from_big_query(
                project_id, dataset, table_name, state_code
            )
            if "time_step" in table_data.columns:
                # Convert the time_step from a timestamp to a relative int value
                table_data["time_step"] = table_data["time_step"].apply(
                    self.time_converter.convert_timestamp_to_time_step
                )

            print(f"{table_key} for {table_name} returned {len(table_data)} results")
            self.data_dict[table_key] = table_data

        # add extra transitions from the RELEASE compartment
        self.data_dict["transitions_data"] = ignite_bq_utils.add_transition_rows(
            self.data_dict["transitions_data"]
        )

        self.data_dict[
            "remaining_sentence_data"
        ] = ignite_bq_utils.add_remaining_sentence_rows(
            self.data_dict["remaining_sentence_data"]
        )

    def _initialize_data_macro(self, data_inputs_params: Dict[str, Any]) -> None:
        """Helper function for _initialize_data()"""
        simulation_tag = data_inputs_params["big_query_simulation_tag"]
        input_data_tables = {
            "outflows_data": spark_bq_utils.OUTFLOWS_DATA_TABLE_NAME,
            "transitions_data": spark_bq_utils.TRANSITIONS_DATA_TABLE_NAME,
            "total_population_data": spark_bq_utils.TOTAL_POPULATION_DATA_TABLE_NAME,
        }

        for table_tag, table_bq_name in input_data_tables.items():
            table_data = spark_bq_utils.load_spark_table_from_big_query(
                table_bq_name, simulation_tag
            )
            print(f"{table_tag} returned {len(table_data)} results")
            self.data_dict[table_tag] = table_data

        # add this so function inputs are consistent with micro-simulation
        self.data_dict["excluded_population_data"] = pd.DataFrame()
