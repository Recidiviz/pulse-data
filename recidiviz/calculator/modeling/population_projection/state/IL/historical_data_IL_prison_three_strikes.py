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
POLICY: three strikes
VERSION: V1
DATA SOURCE: upload files from Illinois Prison Data (Monthly) folder (see Spark tracker/IL folder)
DATA QUALITY: reasonable
HIGHEST PRIORITY MISSING DATA: total population data for individuals sentenced under policy
REFERENCE_DATE: January 2011
TIME_STEP: 1 month
POLICY NOTES: Currently, any offender whose third Class 1 or 2 offense occurs after the age
of 18 automatically has their third Class 1 or 2 offense upgraded to a Class X sentence. The proposed
policy would limit this sentence enhancement to individuals where all three convictions are forcible felonies.
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import upload_spark_model_inputs
pd.set_option("display.max_rows", None, "display.max_columns", None)
# pylint: skip-file


reference_year = 2011

transitions_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'race', 'compartment_duration', 'total_population'])
outflows_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'race', 'time_step', 'total_population'])

# TRANSITIONS TABLE
prison_transitions_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/Three Strikes Prison Transitions Data (Baseline)-Table 1.csv')

probation_transitions_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/Three Strikes Probation Transitions Data (Baseline)-Table 1.csv')

release_transitions_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/Three Strikes Release Transitions Data (Baseline)-Table 1.csv')

transitions_data = pd.concat([transitions_data, prison_transitions_data, probation_transitions_data, release_transitions_data])

# OUTFLOWS TABLE
yearly_outflows_data = pd.read_csv('recidiviz/calculator/modeling/population_projection/state/IL/IL_data/Three Strikes Prison Admissions Data-Table 1.csv')

for year in range(2011, 2020):
    temp_monthly_outflows_data = pd.DataFrame({
        'time_step': [i for i in range((year - reference_year) * 12, (year - reference_year + 1) * 12)] * 2,
        'compartment': ['pretrial'] * 24,
        'outflow_to': ['prison'] * 24,
        'race': ['white'] * 12 + ['non-white'] * 12,
        'total_population': [yearly_outflows_data.iloc[(year - reference_year) * 2, 4] / 12 for month in range(12)] +
                            [yearly_outflows_data.iloc[(year - reference_year) * 2 + 1, 4] / 12 for month in range(12)]
        })
    outflows_data = pd.concat([outflows_data, temp_monthly_outflows_data])

#TOTAL POPULATION TABLE
# none

# STORE DATA
upload_spark_model_inputs('recidiviz-staging', 'IL_prison_three_strikes', outflows_data, transitions_data,
                          pd.DataFrame())
