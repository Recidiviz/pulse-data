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
# pylint: skip-file
"""
STATE: MS
POLICY: The proposed policy would replace life sentences for nonviolent habitual offenders with determinate habitual sentencing.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1Lq-2lBmZ3s19nx1YfMmxCl9R4OQdyAoJ?usp=sharing
DATA QUALITY: excellent
HIGHEST PRIORITY MISSING DATA: N/A
REFERENCE_DATE: 2018
TIME_STEP: 1 year
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import upload_spark_model_inputs


outflows_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'age', 'time_step', 'total_population'])
total_population_data = pd.DataFrame(columns=['compartment', 'age', 'time_step', 'total_population'])


# OUTFLOWS TABLE
outflows_data = pd.concat([outflows_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/MS/habitual_sentencing/MS_data/Outflows Data (Policy A Baseline)-Table 1.csv')])
old_outflows = outflows_data[outflows_data.age != '50_and_under']
old_outflows = old_outflows.groupby(['compartment', 'time_step', 'outflow_to']).sum().total_population.reset_index()
old_outflows['age'] = '51_and_up'
outflows_data = pd.concat([outflows_data[outflows_data.age == '50_and_under'], old_outflows])

# TOTAL POPULATION TABLE
total_population_data = pd.concat([total_population_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/MS/habitual_sentencing/MS_data/Total Population Data (Policy A)-Table 1.csv')])
old_population = total_population_data[total_population_data.age != '50_and_under']
old_population = old_population.groupby(['compartment', 'time_step']).sum().total_population.reset_index()
old_population['age'] = '51_and_up'
total_population_data = pd.concat([total_population_data[total_population_data.age == '50_and_under'], old_population])


# TRANSITIONS TABLE
transitions_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/MS/habitual_sentencing/MS_data/Transitions Data (Policy A Enacted)-Table 1.csv')
old_transitions = transitions_data[transitions_data.age != '50_and_under']
old_transitions = old_transitions.groupby(['compartment', 'outflow_to', 'compartment_duration']).sum().total_population.reset_index()
old_transitions['age'] = '51_and_up'
parole_transitions = pd.concat([transitions_data[transitions_data.age == '50_and_under'], old_transitions])

prison_transitions = pd.DataFrame({'compartment': ['prison'] * 2,
                                   'outflow_to': ['prison'] * 2,
                                   'compartment_duration': [50] * 2,
                                   'age': ['50_and_under', '51_and_up'],
                                   'total_population': [1, 1]})

transitions_data = pd.concat([prison_transitions, parole_transitions])

outflows_data.loc[(outflows_data.time_step == 0) & (outflows_data.age == '50_and_under'), 'total_population'] = \
    total_population_data.loc[total_population_data.age == '50_and_under', 'total_population'].iloc[0]
outflows_data.loc[(outflows_data.time_step == 0) & (outflows_data.age == '51_and_up'), 'total_population'] = \
    total_population_data.loc[total_population_data.age == '51_and_up', 'total_population'].iloc[0]
outflows_data = outflows_data.append(pd.DataFrame(
    {'total_population': [0] * 20, 'age': ['50_and_under'] * 10 + ['51_and_up'] * 10,
     'time_step': list(range(1, 11)) * 2})).ffill()


# STORE DATA
fake_total_population_data = pd.DataFrame({'compartment': ['prison'] * 2, 'time_step': [-1] * 2,
                                           'total_population': [0] * 2, 'age': ['50_and_under', '51_and_up']})
upload_spark_model_inputs('recidiviz-staging', 'MS_habitual_offenders_A', outflows_data, transitions_data,
                          fake_total_population_data)
