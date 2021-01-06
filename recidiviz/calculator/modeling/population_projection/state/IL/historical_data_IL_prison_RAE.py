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
POLICY: reducing/removing automatic firearm enhancements (RAE)
VERSION: V1
DATA_SOURCE: upload files from Illinois Prison Data Monthly folder (see Spark tracker/IL folder)
DATA QUALITY: excellent
TIME STEP: 1 month
POLICY DESCRIPTION: Currently, felony offenders who are convicted of certain crimes get sentenced 15, 20, or 25
additional years depending on whether a firearm has been possessed, discharged, or discharged resulting an injury.
The policy would reduce the sentence enhancement by an unspecified number of years (coordinate with policy group).
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import upload_spark_model_inputs
pd.set_option("display.max_rows", None, "display.max_columns", None)
# pylint: skip-file


reference_year = 2013

# DISAGGREGATION AXES
tis_percentage = ['100%', '85%', '50%']

# TRANSITIONS DATA
transitions_data = pd.DataFrame()

prison_transitions_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/RAE Prison Transitions Data-Table 1.csv')

probation_transitions_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/RAE Probation Transitions Data-Table 1.csv')

release_transitions_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/RAE Release Transitions Data-Table 1.csv')

transitions_data = pd.concat([transitions_data, prison_transitions_data, probation_transitions_data, release_transitions_data])

# OUTFLOWS DATA

yearly_outflows_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/RAE Prison Admissions Data-Table 1.csv')

monthly_outflows_data = pd.DataFrame()

for year in range(2013, 2020):
    temp_monthly_outflows_data = pd.DataFrame({
        'time_step': [i for i in range((year - reference_year) * 12, (year - reference_year + 1) * 12)] * 3,
        'compartment': ['pretrial'] * 36,
        'outflow_to': ['prison'] * 36,
        'tis_percentage': ['50%'] * 12 + ['85%'] * 12 + ['100%'] * 12,
        'total_population': [yearly_outflows_data.iloc[(year - reference_year) * 3, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 3 + 1, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 3 + 2, 4] / 12 for month in range(12)]
        })
    monthly_outflows_data = pd.concat([monthly_outflows_data, temp_monthly_outflows_data])

# TOTAL POPULATION DATA

yearly_total_population_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/RAE Total Prison Population Data-Table 1.csv')
monthly_total_population_data = pd.DataFrame()

for year in range(2013, 2020):
    temp_monthly_total_population_data = pd.DataFrame({
        'time_step': [i for i in range((year - reference_year) * 12, (year - reference_year + 1) * 12)] * 3,
        'compartment': ['prison'] * 36,
        'tis_percentage': ['50%'] * 12 + ['85%'] * 12 + ['100%'] * 12,
        'total_population': [yearly_total_population_data.iloc[(year - reference_year) * 3, 3] for month in range(12)] +
                            [yearly_total_population_data.iloc[(year - reference_year) * 3 + 1, 3] for month in range(12)] +
                            [yearly_total_population_data.iloc[(year - reference_year) * 3 + 2, 3] for month in range(12)]
    })
    monthly_total_population_data = pd.concat([monthly_total_population_data, temp_monthly_total_population_data])

# STORE DATA
monthly_outflows_data = monthly_outflows_data.rename({'tis_percentage': 'crime_type'})
transitions_data = transitions_data.rename({'tis_percentage': 'crime_type'})
monthly_total_population_data = monthly_total_population_data.rename({'tis_percentage': 'crime_type'})

upload_spark_model_inputs('recidiviz-staging', 'IL_prison_RAE', monthly_outflows_data, transitions_data,
                          monthly_total_population_data)
