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
"""SuperSimulation composed object for initializing simulations."""
from datetime import datetime
from warnings import warn
from typing import Dict, Any, Union, List, Tuple, Optional
import numpy as np
import pandas as pd
from recidiviz.calculator.modeling.population_projection import ignite_bq_utils
from recidiviz.calculator.modeling.population_projection import spark_bq_utils


class Initializer:
    """Manage model inputs to SuperSimulation."""

    def __init__(
        self,
        reference_year: float,
        time_step: float,
        yaml_user_inputs: Dict[str, Any],
        data_inputs_params: Dict[str, Any],
        compartments_architecture: Dict[str, str],
        disaggregation_axes: List[str],
        microsim: bool,
    ) -> None:
        self.reference_year = reference_year
        self.time_step = time_step
        self.microsim = microsim
        self.data_dict: Dict[str, Any] = dict()

        self._set_user_inputs(yaml_user_inputs)
        self._initialize_data(
            data_inputs_params, compartments_architecture, disaggregation_axes
        )

    def get_first_relevant_ts(self, first_relevant_ts_override: Optional[int]) -> int:
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
            outflows = self._fully_connect_outflows(
                self.data_dict["outflows_data"][
                    self.data_dict["outflows_data"].run_date == run_date
                ]
            )
            return {
                "outflows_data": outflows,
                "transitions_data": self.data_dict["remaining_sentence_data"][
                    self.data_dict["remaining_sentence_data"].run_date == run_date
                ],
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
            }

        outflows = self._fully_connect_outflows(self.data_dict["outflows_data"])
        return {
            "outflows_data": outflows,
            "transitions_data": self.data_dict["transitions_data"],
            "total_population_data": self.data_dict["total_population_data"],
            "compartments_architecture": self.data_dict["compartments_architecture"],
            "disaggregation_axes": self.data_dict["disaggregation_axes"],
            "microsim_data": pd.DataFrame(),
            "should_initialize_compartment_populations": False,
            "should_scale_populations_after_step": True,
        }

    def get_user_inputs(self) -> Dict[str, Any]:
        return self.user_inputs

    def get_time_step(self) -> float:
        return self.time_step

    def get_reference_year(self) -> float:
        return self.reference_year

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
        data_input_dict = dict()
        first_relevant_ts_dict = dict()
        for start_date in start_run_dates:
            data_input_dict[start_date] = self.get_data_inputs(start_date)
            first_relevant_ts_dict[
                start_date
            ] = self._convert_to_relative_date_from_timestamp(start_date)
        return data_input_dict, first_relevant_ts_dict

    def _fully_connect_outflows(self, outflows_data: pd.DataFrame) -> pd.DataFrame:
        """Helper function for get_data_inputs that ensures outflows_data is fully connected."""
        # Handle sparse outflow events where a disaggregation is missing data for some time steps
        fully_connected_columns = self.data_dict["disaggregation_axes"] + [
            "compartment",
            "outflow_to",
            "time_step",
        ]
        outflows_data = (
            outflows_data.groupby(fully_connected_columns)["total_population"]
            .sum()
            .unstack(level=["time_step"])
        )

        # Raise a warning if there are any disaggregations without outflow records for more than 25% of the time steps
        missing_event_threshold = 0.25
        number_of_missing_events = outflows_data.isnull().sum(axis=1)
        sparse_disaggregations = number_of_missing_events[
            number_of_missing_events / len(outflows_data.columns)
            > missing_event_threshold
        ]
        if not sparse_disaggregations.empty:
            warn(
                f"Outflows data is missing for more than {missing_event_threshold * 100}% for some disaggregations:\n"
                f"{100 * sparse_disaggregations / len(outflows_data.columns)}"
            )

        # Fill the total population with 0 and remove the multiindex for the population simulation
        return (
            outflows_data.fillna(0)
            .stack("time_step")
            .reset_index(name="total_population")
        )

    def _set_user_inputs(self, yaml_user_inputs: Dict[str, Any]) -> None:
        """Populate the simulation user inputs"""
        self.user_inputs = dict()
        self.user_inputs["start_time_step"] = self._convert_to_relative_date(
            yaml_user_inputs["start_year"]
        )
        self.user_inputs["projection_time_steps"] = (
            yaml_user_inputs["projection_years"] / self.time_step
        )
        if not np.isclose(
            self.user_inputs["projection_time_steps"],
            round(self.user_inputs["projection_time_steps"]),
        ):
            raise ValueError(
                f"Projection years {yaml_user_inputs['projection_years']} input cannot be evenly divided "
                f"by time step {self.time_step}"
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
            self.user_inputs["policy_time_step"] = (
                self.user_inputs["start_time_step"] + 1
            )
        else:
            self.user_inputs["speed_run"] = yaml_user_inputs.get("speed_run", False)
            self.user_inputs["policy_time_step"] = self._convert_to_relative_date(
                yaml_user_inputs["policy_year"]
            )

    def _convert_to_relative_date(self, years: Union[float, int]) -> int:
        """converts units of years to units of time steps"""
        ts = (years - self.reference_year) / self.time_step
        if not np.isclose(ts, round(ts)):
            raise ValueError(f"Cannot convert years {years} to integers {ts}")
        return round(ts)

    @staticmethod
    def convert_to_absolute_year(
        time_step: float, reference_year: float, time_steps: pd.Series
    ) -> pd.Series:
        """converts a number of time steps relative to reference date into absolute dates"""
        return time_steps.apply(lambda x: np.round(x * time_step + reference_year, 8))

    def _convert_to_absolute_year(self, time_steps: pd.Series) -> float:
        """versison of convert_to_absolute_year for use inside class"""
        return self.convert_to_absolute_year(
            self.time_step, self.reference_year, time_steps
        )

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
        else:
            self._initialize_data_macro(data_inputs_params)

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
            "excluded_population_data",
        ]
        for table_key in input_data_tables:
            table_name = big_query_params[table_key]
            table_data = ignite_bq_utils.load_ignite_table_from_big_query(
                project_id, dataset, table_name, state_code
            )
            if "time_step" in table_data.columns:
                # Convert the time_step from a timestamp to a relative int value
                table_data["time_step"] = table_data["time_step"].apply(
                    self._convert_to_relative_date_from_timestamp
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

    def _convert_to_relative_date_from_timestamp(self, timestamp: datetime) -> int:
        """Converts units of years to units of time steps"""
        if not self.microsim:
            raise ValueError(
                "Trying to use microsim method _convert_to_relative_date_from_timestamp() in macrosim"
            )
        reference_date_year = np.floor(self.reference_year)
        reference_date_month = 12 * (self.reference_year % 1) + 1
        ts = (
            12 * (timestamp.year - reference_date_year)
            + timestamp.month
            - reference_date_month
        )
        if not np.isclose(ts, round(ts)):
            raise ValueError(f"Cannot convert date {timestamp} to integer {ts}")
        return round(ts)

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
