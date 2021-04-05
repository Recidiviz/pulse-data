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

import pandas as pd
import numpy as np
import pandas_gbq


def load_ignite_table_from_big_query(
    project_id: str, dataset: str, table_name: str, state_code: str
) -> pd.DataFrame:
    """Pull all data from a table for a specific state and run date"""

    query = (
        f"""SELECT * FROM {dataset}.{table_name} WHERE state_code = '{state_code}'"""
    )

    table_results = pandas_gbq.read_gbq(query, project_id=project_id)
    return table_results


def add_transition_rows(transition_data: pd.DataFrame) -> pd.DataFrame:
    """Add rows for the RELEASE compartment transitions"""
    complete_transitions = transition_data.copy()
    for run_date in transition_data.run_date.unique():
        extra_rows_release = pd.DataFrame(
            {
                "compartment": ["RELEASE - RELEASE"] * 2,
                "outflow_to": ["RELEASE - RELEASE"] * 2,
                "gender": ["FEMALE", "MALE"],
                "total_population": [1] * 2,
                "compartment_duration": [1] * 2,
                "run_date": [run_date] * 2,
            }
        )
        long_sentences = 1 - np.round(
            transition_data[transition_data.run_date == run_date]
            .groupby(["compartment", "gender"])
            .sum()
            .total_population,
            6,
        )
        broken_data = long_sentences[long_sentences < 0]
        if len(broken_data) > 0:
            raise RuntimeError(f"broken transitions data: {broken_data}")

        extra_rows_long_sentence = pd.DataFrame(
            {
                "compartment": long_sentences.index.get_level_values(
                    level="compartment"
                ),
                "outflow_to": ["RELEASE - RELEASE"] * len(long_sentences),
                "gender": long_sentences.index.get_level_values(level="gender"),
                "total_population": long_sentences,
                "run_date": [run_date] * len(long_sentences),
                "compartment_duration": [48]
                * len(long_sentences),  # hard coded for now
            }
        )
        complete_transitions = pd.concat(
            [
                complete_transitions,
                extra_rows_release,
                extra_rows_long_sentence.reset_index(drop=True),
            ]
        )
    return complete_transitions


def add_remaining_sentence_rows(remaining_sentence_data: pd.DataFrame) -> pd.DataFrame:
    """Add rows for the RELEASE compartment sentences and set the remaining_duration column to True"""
    complete_remaining = remaining_sentence_data.copy()
    for run_date in remaining_sentence_data.run_date.unique():
        extra_rows = pd.DataFrame(
            {
                "compartment": ["RELEASE - RELEASE"] * 2,
                "outflow_to": ["RELEASE - RELEASE"] * 2,
                "gender": ["FEMALE", "MALE"],
                "total_population": [1] * 2,
                "compartment_duration": [1] * 2,
                "run_date": [run_date] * 2,
                "remaining_duration": True,
            }
        )

        remaining_sentence_data["remaining_duration"] = True
        complete_remaining = pd.concat([complete_remaining, extra_rows])
    return complete_remaining
