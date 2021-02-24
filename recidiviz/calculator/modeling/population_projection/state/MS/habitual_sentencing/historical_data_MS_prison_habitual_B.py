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
POLICY: The proposed policy would eliminate habitual sentencing for nonviolent offenders.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1Lq-2lBmZ3s19nx1YfMmxCl9R4OQdyAoJ?usp=sharing
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA: Admissions and transitions data for 20- sentence year population 
REFERENCE_DATE: 2013
TIME_STEP: 1 year
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import upload_spark_model_inputs

transitions_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'age', 'compartment_duration', 'total_population'])
outflows_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'age', 'time_step', 'total_population'])
total_population_data = pd.DataFrame(columns=['compartment', 'age', 'time_step', 'total_population'])

# TRANSITIONS TABLE
transitions_data = pd.concat([transitions_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/MS/habitual_sentencing/MS_data/Transitions Data (Policy B Baseline)-Table 1.csv')])
old_transitions = transitions_data[transitions_data.age != '50_and_under']
old_transitions = old_transitions.groupby(['compartment', 'outflow_to', 'compartment_duration']).sum().total_population.reset_index()
old_transitions['age'] = '51_and_up'
transitions_data = pd.concat([transitions_data[transitions_data.age == '50_and_under'], old_transitions])


# OUTFLOWS TABLE
outflows_data = pd.concat([outflows_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/MS/habitual_sentencing/MS_data/Outflows Data (Policy B)-Table 1.csv')])
old_outflows = outflows_data[outflows_data.age != '50_and_under']
old_outflows = old_outflows.groupby(['compartment', 'time_step', 'outflow_to']).sum().total_population.reset_index()
old_outflows['age'] = '51_and_up'
outflows_data = pd.concat([outflows_data[outflows_data.age == '50_and_under'], old_outflows])

# TOTAL POPULATION TABLE
total_population_data = pd.concat([total_population_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/MS/habitual_sentencing/MS_data/Total Population Data (Policy B)-Table 1.csv')])
old_population = total_population_data[total_population_data.age != '50_and_under']
old_population = old_population.groupby(['compartment', 'time_step']).sum().total_population.reset_index()
old_population['age'] = '51_and_up'
total_population_data = pd.concat([total_population_data[total_population_data.age == '50_and_under'], old_population])


# STORE DATA
upload_spark_model_inputs('recidiviz-staging', 'MS_habitual_offenders_B', outflows_data, transitions_data,
                          total_population_data)
