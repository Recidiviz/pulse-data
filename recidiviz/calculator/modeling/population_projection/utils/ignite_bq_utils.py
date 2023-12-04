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
"""BigQuery Methods for running the Ignite population projection simulation"""

import numpy as np
import pandas as pd
import pandas_gbq


def load_ignite_table_from_big_query(
    project_id: str, dataset: str, table_name: str, state_code: str
) -> pd.DataFrame:
    """Pull all data from a table for a specific state and run date"""

    query = f"""SELECT * FROM {dataset}.{table_name} WHERE state_code = '{state_code}'
        """

    table_results = pandas_gbq.read_gbq(query, project_id=project_id)
    return table_results


def add_transition_rows(transition_data: pd.DataFrame) -> pd.DataFrame:
    """Add rows for the terminal compartment transitions (LIBERTY and DEATH)"""
    complete_transitions = transition_data.copy()

    # In order to handle any disaggreation axes for the microsim this function would need to take that as an argument
    for run_date, run_date_transitions in transition_data.groupby("run_date"):
        terminal_compartment_transitions = []
        for terminal_compartment in [
            "LIBERTY - LIBERTY_REPEAT_IN_SYSTEM",
            "DEATH - DEATH",
        ]:
            for gender in ["FEMALE", "MALE"]:
                terminal_compartment_transitions.append(
                    {
                        "compartment": terminal_compartment,
                        "outflow_to": terminal_compartment,
                        "simulation_group": gender,
                        "cohort_portion": 1,
                        "compartment_duration": 1,
                        "run_date": run_date,
                    }
                )
        terminal_compartment_transitions = pd.DataFrame(
            terminal_compartment_transitions
        )
        long_sentences = 1 - np.round(
            run_date_transitions.groupby(["compartment", "simulation_group"])[
                "cohort_portion"
            ].sum(),
            6,
        )
        broken_data = long_sentences[long_sentences < 0]
        if len(broken_data) > 0:
            raise RuntimeError(
                f"broken transitions data for run_date {run_date}:\n" f"{broken_data}"
            )

        complete_transitions = pd.concat(
            [
                complete_transitions,
                terminal_compartment_transitions,
            ]
        )
    return complete_transitions


def add_remaining_sentence_rows(remaining_sentence_data: pd.DataFrame) -> pd.DataFrame:
    """
    Append remaining sentence rows so there is at least 1 row per compartment
    and simulation group (gender).
    """
    complete_remaining = remaining_sentence_data.copy()
    for run_date in remaining_sentence_data.run_date.unique():
        extra_rows = []
        for terminal_compartment in [
            "LIBERTY - LIBERTY_REPEAT_IN_SYSTEM",
            "DEATH - DEATH",
        ]:
            for gender in ["FEMALE", "MALE"]:
                extra_rows.append(
                    {
                        "compartment": terminal_compartment,
                        "outflow_to": terminal_compartment,
                        "simulation_group": gender,
                        "cohort_portion": 1,
                        "compartment_duration": 1,
                        "run_date": run_date,
                    }
                )

        complete_remaining = pd.concat([complete_remaining, pd.DataFrame(extra_rows)])

        # Add a row to the `remaining_sentence_data` if it does not exist for the
        # infrequent compartment so that the sub-sim initialization does not fail.
        # If there are no remaining transitions then there are no open sessions for this
        # run_date, and so the remaining sentence length does actually not matter.
        for infrequent_compartment in [
            "PENDING_CUSTODY - PENDING_CUSTODY",
            # "SUPERVISION_OUT_OF_STATE - INFORMAL_PROBATION",
            "INCARCERATION - TEMPORARY_CUSTODY",
            "SUPERVISION_OUT_OF_STATE - PROBATION",
            "SUPERVISION_OUT_OF_STATE - INTERNAL_UNKNOWN",
            "ERRONEOUS_RELEASE - ERRONEOUS_RELEASE",
            "INCARCERATION - WEEKEND_CONFINEMENT",
        ]:
            infrequent_sentences = complete_remaining[
                (complete_remaining["run_date"] == run_date)
                & (complete_remaining["compartment"] == infrequent_compartment)
            ]
            # Only add rows for the unrepresented simulation groups
            if infrequent_sentences["simulation_group"].nunique() < 2:
                missing_gender = [
                    gender
                    for gender in complete_remaining["simulation_group"].unique()
                    if gender not in infrequent_sentences["simulation_group"].unique()
                ]
                num_rows = len(missing_gender)
                infrequent_sentences_rows = pd.DataFrame(
                    {
                        "compartment": [infrequent_compartment] * num_rows,
                        "outflow_to": [infrequent_compartment] * num_rows,
                        "simulation_group": missing_gender,
                        "cohort_portion": [1] * num_rows,
                        "compartment_duration": [1] * num_rows,
                        "run_date": [run_date] * num_rows,
                    }
                )
                complete_remaining = pd.concat(
                    [complete_remaining, infrequent_sentences_rows]
                )

    return complete_remaining
