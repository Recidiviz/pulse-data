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
STATE: OK
POLICY: The proposed policy would remove Oklahoma's repeat offender sentencing enhancements
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1t9TfHxC7B5kxPjfmkBvpwkX1VUCeycy0?usp=sharing
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA: Total population data
REFERENCE_DATE: January 2010
TIME_STEP: 1 month
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import upload_spark_model_inputs

reference_year = 2010

transitions_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'crime_type', 'compartment_duration', 'total_population'])
outflows_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'crime_type', 'time_step', 'total_population'])
total_population_data = pd.DataFrame(columns=['compartment', 'crime_type', 'time_step', 'total_population'])

# TRANSITIONS TABLE
transitions_data = pd.concat([transitions_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/OK/OK_habitual_sentencing/OK_data/Habitual Transitions Data-Table 1.csv')])
subgroups = [f'max_sentence_{i}_yr' for i in [1, 2, 3, 4, 5, 7, 8, 10, 15, 20]]
transitions_data.loc[(transitions_data.compartment == 'release') &
                     (transitions_data.outflow_to == 'release'), 'outflow_to'] = 'release_full'
transitions_data.loc[(transitions_data.compartment == 'release') &
                     (transitions_data.outflow_to == 'release'), 'compartment_duration'] = 36
transitions_data = transitions_data.append(pd.DataFrame({
    'compartment': ['release_full'] * len(subgroups),
    'outflow_to': ['release_full'] * len(subgroups),
    'crime_type': subgroups,
    'compartment_duration': [36] * len(subgroups),
    'total_population': [1] * len(subgroups)
}))


# OUTFLOWS TABLE
yearly_outflows_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/OK/OK_habitual_sentencing/OK_data/Habitual Outflows Data (Yearly)-Table 1.csv')

for year in range(2010, 2020):
    temp_monthly_outflows_data = pd.DataFrame({
        'time_step': [i for i in range((year - reference_year) * 12, (year - reference_year + 1) * 12)] * 10,
        'compartment': ['pretrial'] * 120,
        'outflow_to': ['prison'] * 120,
        'crime_type': ['max_sentence_1_yr'] * 12 + ['max_sentence_2_yr'] * 12 + ['max_sentence_3_yr'] * 12 + ['max_sentence_4_yr'] * 12 + ['max_sentence_5_yr'] * 12 + ['max_sentence_7_yr'] * 12 + ['max_sentence_8_yr'] * 12 + ['max_sentence_10_yr'] * 12 + ['max_sentence_15_yr'] * 12 + ['max_sentence_20_yr'] * 12,
        'total_population': [yearly_outflows_data.iloc[(year - reference_year) * 10, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 10 + 1, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 10 + 2, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 10 + 3, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 10 + 4, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 10 + 5, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 10 + 6, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 10 + 7, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 10 + 8, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 10 + 9, 4] / 12 for month in range(12)]
    })
    outflows_data = pd.concat([outflows_data, temp_monthly_outflows_data])


# STORE DATA
simulation_tag = "OK_prison"
upload_spark_model_inputs('recidiviz-staging', simulation_tag, outflows_data, transitions_data,
                          total_population_data)



