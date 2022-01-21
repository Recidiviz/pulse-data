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
"""Composition object for SubSimulation to initialize compartments for a macro-simulation and scale populations."""
import logging
from typing import Dict, List, Tuple

import pandas as pd

from recidiviz.calculator.modeling.population_projection.compartment_transitions import (
    CompartmentTransitions,
)
from recidiviz.calculator.modeling.population_projection.full_compartment import (
    FullCompartment,
)
from recidiviz.calculator.modeling.population_projection.shell_compartment import (
    ShellCompartment,
)
from recidiviz.calculator.modeling.population_projection.spark_compartment import (
    SparkCompartment,
)
from recidiviz.calculator.modeling.population_projection.spark_policy import SparkPolicy
from recidiviz.calculator.modeling.population_projection.sub_simulation.sub_simulation import (
    SubSimulation,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.initializer import (
    UserInputs,
)


class SubSimulationFactory:
    """Handles set-up specific logic of SubSimulation."""

    @classmethod
    def build_sub_simulation(
        cls,
        outflows_data: pd.DataFrame,
        transitions_data: pd.DataFrame,
        compartments_architecture: Dict[str, str],
        user_inputs: UserInputs,
        policy_list: List[SparkPolicy],
        first_relevant_ts: int,
        should_single_cohort_initialize_compartments: bool,
        starting_cohort_sizes: pd.DataFrame,
    ) -> SubSimulation:
        """Build a sub_simulation."""

        transitions_per_compartment, shell_policies = cls._initialize_transition_tables(
            transitions_data, compartments_architecture, policy_list
        )

        # Preprocess the historical data into separate pieces per compartment
        historical_outflows = cls._load_data(compartments_architecture, outflows_data)

        # Initialize the compartment classes
        simulation_compartments = cls._build_compartments(
            historical_outflows,
            transitions_per_compartment,
            shell_policies,
            user_inputs,
            compartments_architecture,
            first_relevant_ts,
            starting_cohort_sizes,
            should_single_cohort_initialize_compartments,
        )

        return SubSimulation(
            simulation_compartments,
        )

    @classmethod
    def _initialize_transition_tables(
        cls,
        transitions_data: pd.DataFrame,
        compartments_architecture: Dict[str, str],
        policy_list: List[SparkPolicy],
    ) -> Tuple[Dict[str, CompartmentTransitions], Dict[str, List[SparkPolicy]]]:
        """Create and initialize all transition tables and store shell policies."""
        # Initialize a default transition class for each compartment to represent the no-policy scenario
        transitions_per_compartment = {}
        unused_transitions_data = transitions_data
        for compartment in compartments_architecture:
            compartment_type = compartments_architecture[compartment]
            compartment_duration_data = transitions_data[
                transitions_data["compartment"] == compartment
            ]
            unused_transitions_data = unused_transitions_data.drop(
                compartment_duration_data.index
            )

            if compartment_duration_data.empty:
                # Do not throw an error if this compartment has no current transitions
                # but does have an applied policy
                if compartment_type != "shell":
                    raise ValueError(
                        f"Transition data missing for compartment {compartment}. Data is required for all "
                        "disaggregation axes. Even the 'release' compartment needs transition data even if "
                        "it's just outflow to 'release'."
                    )
            else:
                if compartment_type == "full":
                    transition_class = CompartmentTransitions(compartment_duration_data)
                elif compartment_type == "shell":
                    raise ValueError(
                        f"Cannot provide transitions data for shell compartment \n "
                        f"{compartment_duration_data}"
                    )
                else:
                    raise ValueError(
                        f"unrecognized transition table type {compartment_type}"
                    )

                transitions_per_compartment[compartment] = transition_class

        if len(unused_transitions_data) > 0:
            logging.warning(
                "Some transitions data not fed to a compartment:\n%s",
                unused_transitions_data.groupby("compartment").sum()[
                    "total_population"
                ],
            )

        # Create a transition object for each compartment and year with policies applied and store shell policies
        shell_policies = {}
        for compartment in compartments_architecture:
            # Select any policies that are applicable for this compartment
            compartment_policies = SparkPolicy.get_compartment_policies(
                policy_list, compartment
            )

            # add to the dict compartment -> transition class with policies applied
            if compartment in transitions_per_compartment:
                transitions_per_compartment[compartment].initialize_transition_tables(
                    compartment_policies
                )

            # add shell policies to dict that gets passed to initialization
            else:
                shell_policies[compartment] = compartment_policies

        return transitions_per_compartment, shell_policies

    @classmethod
    def _load_data(
        cls, compartments_architecture: Dict[str, str], outflows_data: pd.DataFrame
    ) -> pd.DataFrame:
        """pre-process historical outflows data to produce a dictionary of formatted DataFrames containing
        outflow data for each compartment"""

        # Check that the outflows data has the compartments needed
        simulation_architecture = compartments_architecture.keys()
        outflow_compartments = outflows_data["compartment"].unique()
        missing_compartment_data = [
            compartment
            for compartment in outflow_compartments
            if compartment not in simulation_architecture
        ]
        if len(missing_compartment_data) != 0:
            raise ValueError(
                f"Simulation architecture is missing compartments for the outflows: "
                f"{missing_compartment_data}"
            )

        # get counts of population from historical data aggregated by compartment, outflow, and year
        preprocessed_data = outflows_data.groupby(
            ["compartment", "outflow_to", "time_step"]
        )["total_population"].sum()

        preprocessed_data = preprocessed_data.unstack(level=["outflow_to", "time_step"])
        preprocessed_data = preprocessed_data.reindex(simulation_architecture).stack(
            level="outflow_to", dropna=False
        )
        return preprocessed_data

    @classmethod
    def _build_compartments(
        cls,
        preprocessed_data: pd.DataFrame,
        transition_tables_by_compartment: Dict[str, CompartmentTransitions],
        shell_policies: Dict[str, List[SparkPolicy]],
        user_inputs: UserInputs,
        simulation_architecture: Dict[str, str],
        first_relevant_ts: int,
        starting_cohort_sizes: pd.DataFrame,
        should_initialize_compartment_populations: bool,
    ) -> Dict[str, SparkCompartment]:
        """Initialize all the SparkCompartments for the subpopulation simulation"""

        simulation_compartments: Dict[str, SparkCompartment] = {}
        for compartment, compartment_type in simulation_architecture.items():
            outflows_data = (
                preprocessed_data.loc[compartment]
                .dropna(axis=1, how="all")
                .dropna(axis=0, how="all")
            )

            # if no transition table, initialize as shell compartment
            if compartment_type == "shell":
                if outflows_data.empty:
                    raise ValueError(
                        f"outflows_data for shell compartment {compartment} cannot be empty"
                    )
                simulation_compartments[compartment] = ShellCompartment(
                    outflows_data=outflows_data,
                    starting_ts=first_relevant_ts,
                    # Default `constant_admissions` to False if not set
                    constant_admissions=user_inputs.constant_admissions
                    if user_inputs.constant_admissions is not None
                    else False,
                    tag=compartment,
                    policy_list=shell_policies[compartment],
                )
            elif compartment in transition_tables_by_compartment:
                simulation_compartments[compartment] = FullCompartment(
                    outflow_data=outflows_data,
                    compartment_transitions=transition_tables_by_compartment[
                        compartment
                    ],
                    starting_ts=first_relevant_ts,
                    tag=compartment,
                )
            else:
                logging.warning("Not initializing a compartment for %s", compartment)

        cls._initialize_edges_and_cohorts(
            simulation_compartments,
            should_initialize_compartment_populations,
            starting_cohort_sizes,
        )

        return simulation_compartments

    @classmethod
    def _initialize_edges_and_cohorts(
        cls,
        simulation_compartments: Dict[str, SparkCompartment],
        should_initialize_compartment_populations: bool,
        starting_cohort_sizes: pd.DataFrame,
    ) -> None:
        """
        Initializes cohorts and edges
        starting_cohort_sizes should be empty if should_initialize_compartment_populations is false
        """
        for compartment_tag, compartment_obj in simulation_compartments.items():
            compartment_obj.initialize_edges(list(simulation_compartments.values()))

            # Initialize for the microsim
            if should_initialize_compartment_populations and isinstance(
                compartment_obj, FullCompartment
            ):
                compartment_population = starting_cohort_sizes[
                    starting_cohort_sizes.compartment == compartment_tag
                ]
                if compartment_population.empty:
                    compartment_population = 0
                elif len(compartment_population) > 1:
                    raise ValueError(
                        f"Multiple total populations for the same compartment and time_step: \n"
                        f"{compartment_population} "
                    )
                else:
                    compartment_population = compartment_population.iloc[
                        0
                    ].total_population
                compartment_obj.single_cohort_intitialize(compartment_population)

            elif (
                not should_initialize_compartment_populations
                and not starting_cohort_sizes.empty
            ):
                raise ValueError(
                    "starting cohort populations data passed to simulation that isn't"
                    " initializing compartment populations"
                )
