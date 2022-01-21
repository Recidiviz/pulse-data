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
import dataclasses
import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional, Tuple, Union

import pandas as pd

from recidiviz.calculator.modeling.population_projection.super_simulation.time_converter import (
    TimeConverter,
)
from recidiviz.calculator.modeling.population_projection.utils import (
    ignite_bq_utils,
    spark_bq_utils,
)


@dataclasses.dataclass
class UserInputs:
    """Settings for the population projection simulation defined in YAML configs"""

    # Time step when the projection begins
    start_time_step: int
    # Number of time steps to project into the future
    projection_time_steps: int
    # True if the predicted admissions should assume constant admissions,
    # useful when there is limited historical admissions data
    constant_admissions: Optional[bool] = None
    # Date the projection is run: used to pick the data from a snapshot in time
    run_date: Optional[str] = None
    # True if the initialization (filling the compartments) should be done
    # only `projection_time_steps` backwards from the `start_time_step`.
    # Used when there is not data for the compartment population at the `start_time_step`
    speed_run: Optional[bool] = None
    # Optional alternative function to handle cross-flows between SubSimulations
    cross_flow_function: Optional[str] = None


@dataclasses.dataclass
class SimulationInputData:
    """Data to create and run the population projection simulation"""

    # Compartment names and types (full, shell) that make up this simulated system
    compartments_architecture: Dict[str, str]
    # Columns that define the groupings for separate SubSimulations
    disaggregation_axes: List[str]
    # True if the simulation inputs have microsim data:
    # case-level, current population, remaining length of stay for each FullCompartment
    microsim: bool
    # Historical transition data from Shell -> Full compartments
    outflows_data: pd.DataFrame
    # Historical population data for each FullCompartment
    total_population_data: pd.DataFrame
    # Transition probabilities for each FullCompartment -> outflow
    # For the microsim this is the remaining length of stay table for the current population
    transitions_data: pd.DataFrame
    # Transition data for new admissions, empty dataframe for the macrosim
    microsim_data: pd.DataFrame
    # True if the input data includes current population & remaining length of stay info
    # so that the simulation can be initialized at the start time step (microsim)
    should_initialize_compartment_populations: bool
    # True if the simulation initialization prior to the start time step should scale
    # the FullCompartment total population each time step in order to help the
    # macrosim cold-start
    should_scale_populations_after_step: bool
    # Optional population data used for the microsim for groups that should not be
    # included in the simulation output in BigQuery, but are too entwined in the
    # population to remove from the simulation inputs.
    excluded_population_data: pd.DataFrame = dataclasses.field(
        default_factory=pd.DataFrame
    )
    # Method to override the default cross-flow function
    # PopulationSimulation.update_attributes_identity()
    override_cross_flow_function: Optional[
        Callable[[pd.DataFrame, int], pd.DataFrame]
    ] = None


@dataclasses.dataclass
class MicroSimulationDataInputs:
    """BigQuery parameters for loading the MicroSimulation data"""

    # BigQuery project id
    project_id: str
    # BigQuery dataset id
    input_dataset: str
    # Simulation state code used for downloading and uploading the data
    state_code: str
    # Table name for historical transition data from Shell -> Full compartments
    outflows_data: str
    # Table name for historical population data for each Full compartment
    total_population_data: str
    # Table name for remaining length of stay transition probabilities
    remaining_sentence_data: str
    # Table name for transition probabilities for new admissions to the system
    transitions_data: str
    # Optional table name for population totals that should be excluded from the
    # predicted population output for certain compartments
    excluded_population_data: Optional[str] = None


@dataclasses.dataclass
class MacroSimulationDataInputs:
    """BigQuery parameters for loading the MacroSimulation data"""

    # State/policy specific string used for downloading and uploading the data
    big_query_simulation_tag: str


class Initializer:
    """Manage model inputs to SuperSimulation."""

    # Percent threshold used to determine when to warn the user about missing outflows data
    MISSING_EVENT_THRESHOLD = 0.25

    def __init__(
        self,
        time_converter: TimeConverter,
        user_inputs: UserInputs,
        data_inputs_params: Union[MacroSimulationDataInputs, MicroSimulationDataInputs],
        compartments_architecture: Dict[str, str],
        disaggregation_axes: List[str],
        microsim: bool,
    ) -> None:
        self.time_converter = time_converter

        self.user_inputs = user_inputs
        self.data_inputs = self._raw_data_inputs_from_data_inputs(
            data_inputs_params, compartments_architecture, disaggregation_axes, microsim
        )

    def get_first_relevant_ts(
        self, first_relevant_ts_override: Optional[int] = None
    ) -> int:
        """calculate ts to start model initialization at."""

        if first_relevant_ts_override is not None:
            if self.data_inputs.microsim:
                raise ValueError(
                    "Cannot pass first_relevant_ts_override to micro-simulation"
                )

            return first_relevant_ts_override

        if self.data_inputs.microsim:
            return self.user_inputs.start_time_step
        if self.user_inputs.speed_run:
            max_sentence = self.user_inputs.projection_time_steps + 1
        else:
            max_sentence = max(self.data_inputs.transitions_data.compartment_duration)
        return self.user_inputs.start_time_step - int(max_sentence)

    def get_outflows_for_error(self) -> pd.DataFrame:
        """Pull historical outflows data for Validator.calculate_outflows_error"""
        if self.data_inputs.microsim:
            # Use the most up-to-date outflows to compute the simulation error
            return self.data_inputs.outflows_data[
                self.data_inputs.outflows_data.run_date
                == self.data_inputs.outflows_data.run_date.max()
            ]
        return self.data_inputs.outflows_data

    def get_data_inputs(
        self, run_date_override: Optional[datetime] = None
    ) -> SimulationInputData:
        """Get data inputs to build a PopulationSimulation"""
        if not self.data_inputs.microsim and run_date_override is not None:
            raise ValueError("Cannot override run_date for Macrosimulation.")

        if self.data_inputs.microsim:
            run_date = run_date_override or self.user_inputs.run_date
            return SimulationInputData(
                compartments_architecture=self.data_inputs.compartments_architecture,
                disaggregation_axes=self.data_inputs.disaggregation_axes,
                microsim=self.data_inputs.microsim,
                outflows_data=self.data_inputs.outflows_data[
                    self.data_inputs.outflows_data.run_date == run_date
                ],
                # Use the most recent total population data
                total_population_data=self.data_inputs.total_population_data[
                    self.data_inputs.total_population_data.run_date
                    == self.data_inputs.total_population_data.run_date.max()
                ],
                transitions_data=self.data_inputs.transitions_data[
                    self.data_inputs.transitions_data.run_date == run_date
                ],
                microsim_data=self.data_inputs.microsim_data[
                    self.data_inputs.microsim_data.run_date == run_date
                ],
                excluded_population_data=self.get_excluded_pop_data(),
                should_initialize_compartment_populations=self.data_inputs.should_initialize_compartment_populations,
                should_scale_populations_after_step=self.data_inputs.should_scale_populations_after_step,
                override_cross_flow_function=self.data_inputs.override_cross_flow_function,
            )

        return self.data_inputs

    def get_user_inputs(self) -> UserInputs:
        return self.user_inputs

    def get_max_sentence(self) -> int:
        return self.user_inputs.start_time_step - self.get_first_relevant_ts(None)

    def get_excluded_pop_data(self) -> pd.DataFrame:
        """
        Return the size of the population that should be excluded per compartment at
        the first time step of the simulation
        """
        excluded_pop = self.data_inputs.excluded_population_data
        if self.data_inputs.microsim & (not excluded_pop.empty):
            # Only return the excluded pop for the model run date and starting time step
            excluded_pop = excluded_pop[
                (excluded_pop.time_step == self.user_inputs.start_time_step)
                & (excluded_pop.run_date == self.user_inputs.run_date)
            ]
        return excluded_pop

    def get_inputs_for_calculate_prep_scale_factor(
        self,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        excluded_pop = self.get_excluded_pop_data()
        total_pop = self.data_inputs.total_population_data
        total_pop = total_pop[
            (total_pop.time_step == self.user_inputs.start_time_step)
            & (total_pop.run_date == self.user_inputs.run_date)
        ]
        return excluded_pop, total_pop

    def get_inputs_for_microsim_baseline_over_time(
        self, start_run_dates: List[datetime]
    ) -> Tuple[Dict[datetime, SimulationInputData], Dict[datetime, int]]:
        data_input_dict: Dict[datetime, SimulationInputData] = {}
        first_relevant_ts_dict: Dict[datetime, int] = {}
        for start_date in start_run_dates:
            data_input_dict[start_date] = self.get_data_inputs(start_date)
            first_relevant_ts_dict[
                start_date
            ] = self.time_converter.convert_timestamp_to_time_step(start_date)
        return data_input_dict, first_relevant_ts_dict

    def set_override_cross_flow_function(
        self, cross_flow_function: Callable[[pd.DataFrame, int], pd.DataFrame]
    ) -> None:
        self.data_inputs.override_cross_flow_function = cross_flow_function

    @staticmethod
    def fully_hydrate_outflows(
        outflows_data: pd.DataFrame,
        disaggregation_axes: List[str],
        microsim: bool,
    ) -> pd.DataFrame:
        """Return the outflows_data dataframe with 0s filled in for missing time steps."""
        # Handle sparse outflow events where a disaggregation is missing data for some time steps
        fully_hydrated_columns = disaggregation_axes + ["compartment", "outflow_to"]

        if microsim:
            fully_hydrated_columns.append("run_date")

        # For each compartment that has outflows data, complete data so that every time step between
        # MIN(time_step) and MAX(time_step) has data (i.e. fill in 0s)
        time_range_per_compartment = outflows_data.groupby(
            fully_hydrated_columns, as_index=False
        )["time_step"].agg(["min", "max"])
        time_range_per_compartment["time_step"] = time_range_per_compartment.apply(
            lambda row: range(row["min"], row["max"] + 1), axis=1
        )
        time_range_per_compartment = time_range_per_compartment.explode("time_step")[
            ["time_step"]
        ]

        fully_hydrated_data = time_range_per_compartment.merge(
            outflows_data[fully_hydrated_columns + ["time_step", "total_population"]],
            on=["time_step"] + fully_hydrated_columns,
            how="left",
        )

        # Raise a warning if there are disaggregations without outflow records for more
        # than the MISSING_EVENT_THRESHOLD percent of outflows per compartment
        fully_hydrated_data[
            "missing_outflow"
        ] = fully_hydrated_data.total_population.isnull()
        number_of_missing_events = fully_hydrated_data.groupby(
            fully_hydrated_columns
        ).agg({"missing_outflow": ["sum", "size"]})
        number_of_missing_events["percent_missing"] = (
            number_of_missing_events["missing_outflow", "sum"]
            / number_of_missing_events["missing_outflow", "size"]
        )
        sparse_disaggregations = number_of_missing_events[
            number_of_missing_events["percent_missing"]
            > Initializer.MISSING_EVENT_THRESHOLD
        ]["percent_missing"]
        if microsim:
            sparse_disaggregations = sparse_disaggregations.unstack("run_date")
        if not sparse_disaggregations.empty:
            logging.warning(
                "Outflows data is missing for more than %s%% for some disaggregations:"
                "\n%s",
                100 * Initializer.MISSING_EVENT_THRESHOLD,
                100 * sparse_disaggregations,
            )

        # Fill the total population with 0 and remove the `missing_outflow` column
        fully_hydrated_data.drop("missing_outflow", axis=1, inplace=True)
        fully_hydrated_data.fillna(0, inplace=True)
        fully_hydrated_data.reset_index(drop=True, inplace=True)

        return fully_hydrated_data

    def _raw_data_inputs_from_data_inputs(
        self,
        data_inputs_params: Union[MacroSimulationDataInputs, MicroSimulationDataInputs],
        compartments_architecture: Dict[str, str],
        disaggregation_axes: List[str],
        microsim: bool,
    ) -> SimulationInputData:
        """Load the simulation data from BigQuery into RawDataInputs"""
        if microsim and isinstance(data_inputs_params, MicroSimulationDataInputs):
            return self._microsim_data_inputs_to_raw_data_inputs(
                data_inputs_params, compartments_architecture, disaggregation_axes
            )
        if not microsim and isinstance(data_inputs_params, MacroSimulationDataInputs):
            return self._macrosim_data_inputs_to_raw_data_inputs(
                data_inputs_params, compartments_architecture, disaggregation_axes
            )
        raise TypeError(
            f"Cannot initialize {microsim=} with {type(data_inputs_params)=}!"
        )

    def _microsim_data_inputs_to_raw_data_inputs(
        self,
        big_query_params: MicroSimulationDataInputs,
        compartments_architecture: Dict[str, str],
        disaggregation_axes: List[str],
    ) -> SimulationInputData:
        """Helper function for _raw_data_inputs_from_data_inputs()"""

        def read_table_data(table_name: str) -> pd.DataFrame:
            table_data = ignite_bq_utils.load_ignite_table_from_big_query(
                big_query_params.project_id,
                big_query_params.input_dataset,
                table_name,
                big_query_params.state_code,
            )
            if "time_step" in table_data.columns:
                # Convert the time_step from a timestamp to a relative int value
                table_data["time_step"] = table_data["time_step"].apply(
                    self.time_converter.convert_timestamp_to_time_step
                )
            return table_data

        if big_query_params.excluded_population_data is None:
            excluded_population_data = pd.DataFrame()
        else:
            excluded_population_data = read_table_data(
                big_query_params.excluded_population_data
            )

        sparse_outflows = read_table_data(big_query_params.outflows_data)
        outflows_data = self.fully_hydrate_outflows(
            sparse_outflows,
            disaggregation_axes,
            True,
        )

        return SimulationInputData(
            compartments_architecture=compartments_architecture,
            disaggregation_axes=disaggregation_axes,
            microsim=True,
            outflows_data=outflows_data,
            total_population_data=read_table_data(
                big_query_params.total_population_data
            ),
            # add extra transitions from the RELEASE compartment
            transitions_data=ignite_bq_utils.add_remaining_sentence_rows(
                read_table_data(big_query_params.remaining_sentence_data)
            ),
            microsim_data=ignite_bq_utils.add_transition_rows(
                read_table_data(big_query_params.transitions_data)
            ),
            excluded_population_data=excluded_population_data,
            should_initialize_compartment_populations=True,
            should_scale_populations_after_step=False,
        )

    def _macrosim_data_inputs_to_raw_data_inputs(
        self,
        data_inputs_params: MacroSimulationDataInputs,
        compartments_architecture: Dict[str, str],
        disaggregation_axes: List[str],
    ) -> SimulationInputData:
        """Helper function for _raw_data_inputs_from_data_inputs()"""

        def read_table_data(table_bq_name: str) -> pd.DataFrame:
            table_data = spark_bq_utils.load_spark_table_from_big_query(
                table_bq_name, data_inputs_params.big_query_simulation_tag
            )
            return table_data

        return SimulationInputData(
            compartments_architecture=compartments_architecture,
            disaggregation_axes=disaggregation_axes,
            microsim=False,
            outflows_data=self.fully_hydrate_outflows(
                read_table_data(spark_bq_utils.OUTFLOWS_DATA_TABLE_NAME),
                disaggregation_axes,
                False,
            ),
            total_population_data=read_table_data(
                spark_bq_utils.TOTAL_POPULATION_DATA_TABLE_NAME
            ),
            transitions_data=read_table_data(
                spark_bq_utils.TRANSITIONS_DATA_TABLE_NAME
            ),
            microsim_data=pd.DataFrame(),
            should_initialize_compartment_populations=False,
            should_scale_populations_after_step=True,
        )
