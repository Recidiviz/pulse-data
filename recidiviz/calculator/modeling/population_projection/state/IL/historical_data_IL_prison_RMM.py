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
"""
STATE: IL
POLICY: reducing mandatory minimums (RMM)
VERSION: V1
DATA_SOURCE: upload files in Illinois Prison Data Formatted folder (see Spark tracker/IL folder)
DATA QUALITY: excellent
TIME STEP: 1 month
POLICY DESCRIPTION: reduces the mandatory minimums for felony classes as follows (with maximum sentences remaining unchanged):
Murder: 20 to 15 years
Class X: 6 to 4 years
Class 1: 4 to 2 years
Class 2: 3 to 1 years
Class 3: 2 to 1 years
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import upload_spark_model_inputs
pd.set_option("display.max_rows", None, "display.max_columns", None)
# pylint: skip-file


reference_year = 2011

# DISAGGREGATION AXES
race = ['white', 'non-white']
crime_classification = ['murder', 'class_x', 'class_1', 'class_2', 'class_3']

# TRANSITIONS DATA
transitions_data = pd.DataFrame()

# change phrase within parentheses in file names below from "LOS" to "Sentence Length"
# if would like to see transition distributions by sentence length instead

prison_transitions_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/RMM Prison Transitions Data (LOS)-Table 1.csv')

probation_transitions_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/RMM Probation Transitions Data-Table 1.csv')

release_transitions_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/RMM Release Transitions Data-Table 1.csv')

transitions_data = pd.concat([transitions_data, prison_transitions_data, probation_transitions_data, release_transitions_data])

# OUTFLOWS DATA

outflows_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/RMM Prison Admissions Data (LOS)-Table 1.csv')

monthly_outflows_data = pd.DataFrame()

for year in range(2011, 2020):
    temp_monthly_outflows_data = pd.DataFrame({
        'time_step': [i for i in range((year - reference_year) * 12, (year - reference_year + 1) * 12)] * 10,
        'compartment': ['pretrial'] * 120,
        'outflow_to': ['prison'] * 120,
        'crime_classification': ['murder'] * 24 + ['class_x'] * 24 + ['class_1'] * 24 + ['class_2'] * 24 + ['class_3'] * 24,
        'race': ['white'] * 12 + ['non-white'] * 12 + ['white'] * 12 + ['non-white'] * 12 + ['white'] * 12 + ['non-white'] * 12 + ['white'] * 12 + ['non-white'] * 12 + ['white'] * 12 + ['non-white'] * 12,
        'total_population': [outflows_data.iloc[(year - reference_year) * 10, 5] / 12 for month in range(12)] +
                            [outflows_data.iloc[(year - reference_year) * 10 + 1, 5] / 12 for month in range(12)] +
                            [outflows_data.iloc[(year - reference_year) * 10 + 2, 5] / 12 for month in range(12)] +
                            [outflows_data.iloc[(year - reference_year) * 10 + 3, 5] / 12 for month in range(12)] +
                            [outflows_data.iloc[(year - reference_year) * 10 + 4, 5] / 12 for month in range(12)] +
                            [outflows_data.iloc[(year - reference_year) * 10 + 5, 5] / 12 for month in range(12)] +
                            [outflows_data.iloc[(year - reference_year) * 10 + 6, 5] / 12 for month in range(12)] +
                            [outflows_data.iloc[(year - reference_year) * 10 + 7, 5] / 12 for month in range(12)] +
                            [outflows_data.iloc[(year - reference_year) * 10 + 8, 5] / 12 for month in range(12)] +
                            [outflows_data.iloc[(year - reference_year) * 10 + 9, 5] / 12 for month in range(12)]
    })
    monthly_outflows_data = pd.concat([monthly_outflows_data, temp_monthly_outflows_data])

# TOTAL POPULATION DATA

total_population_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/RMM Total Prison Population Data-Table 1.csv')

monthly_total_population_data = pd.DataFrame()

for year in range(2011, 2020):
    temp_monthly_total_population_data = pd.DataFrame({
        'time_step': [i for i in range((year - reference_year) * 12, (year - reference_year + 1) * 12)] * 10,
        'compartment': ['prison'] * 120,
        'crime_classification': ['murder'] * 24 + ['class_x'] * 24 + ['class_1'] * 24 + ['class_2'] * 24 + ['class_3'] * 24,
        'race': ['white'] * 12 + ['non-white'] * 12 + ['white'] * 12 + ['non-white'] * 12 + ['white'] * 12 + ['non-white'] * 12 + ['white'] * 12 + ['non-white'] * 12 + ['white'] * 12 + ['non-white'] * 12,
        'total_population': [total_population_data.iloc[(year - reference_year) * 10, 4] for month in range(12)] +
                            [total_population_data.iloc[(year - reference_year) * 10 + 1, 4] for month in range(12)] +
                            [total_population_data.iloc[(year - reference_year) * 10 + 2, 4] for month in range(12)] +
                            [total_population_data.iloc[(year - reference_year) * 10 + 3, 4] for month in range(12)] +
                            [total_population_data.iloc[(year - reference_year) * 10 + 4, 4] for month in range(12)] +
                            [total_population_data.iloc[(year - reference_year) * 10 + 5, 4] for month in range(12)] +
                            [total_population_data.iloc[(year - reference_year) * 10 + 6, 4] for month in range(12)] +
                            [total_population_data.iloc[(year - reference_year) * 10 + 7, 4] for month in range(12)] +
                            [total_population_data.iloc[(year - reference_year) * 10 + 8, 4] for month in range(12)] +
                            [total_population_data.iloc[(year - reference_year) * 10 + 9, 4] for month in range(12)]
    })
    monthly_total_population_data = pd.concat([monthly_total_population_data, temp_monthly_total_population_data])

# STORE DATA
monthly_outflows_data = monthly_outflows_data.rename({'crime_classification': 'crime_type'}, axis=1)
transitions_data = transitions_data.rename({'crime_classification': 'crime_type'}, axis=1)
monthly_total_population_data = monthly_total_population_data.rename({'crime_classification': 'crime_type'}, axis=1)

upload_spark_model_inputs('recidiviz-staging', 'IL_prison_RMM', monthly_outflows_data, transitions_data,
                          monthly_total_population_data)
