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
"""Methods to pull data from BigQuery for running the population projection simulation"""

import pandas as pd
import pandas_gbq


def load_table_from_big_query(project_id: str, dataset: str, table_name: str, state_code: str, run_date: str):
    """Pull all data from a table for a specific state and run date"""

    query = f"""SELECT * FROM {dataset}.{table_name} WHERE state_code = '{state_code}' AND run_date = '{run_date}'"""

    table_results = pandas_gbq.read_gbq(query, project_id=project_id)
    return table_results


def add_transition_rows(transition_data):
    """Add rows for the RELEASE compartment transitions"""
    extra_rows = pd.DataFrame({'compartment': ['RELEASE - FULL'] * 2,
                               'outflow_to': ['RELEASE - FULL'] * 2,
                               'gender': ['FEMALE', 'MALE'],
                               'total_population': [1] * 2,
                               'compartment_duration': [1] * 2
                               })
    return pd.concat([transition_data, extra_rows])


def add_remaining_sentence_rows(remaining_sentence_data):
    """Add rows for the RELEASE compartment sentences and set the remaining_duration column to True"""
    extra_rows = pd.DataFrame({
        'compartment': ['RELEASE - FULL'] * 2,
        'outflow_to': ['RELEASE - FULL'] * 2,
        'gender': ['FEMALE', 'MALE'],
        'total_population': [1] * 2,
        'compartment_duration': [1] * 2,
        'remaining_duration': True
    })

    remaining_sentence_data['remaining_duration'] = True
    return pd.concat([remaining_sentence_data, extra_rows])
