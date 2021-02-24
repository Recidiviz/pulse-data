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
STATE: TX
POLICY: The proposed policy would provide financial incentives for probation and parole offices to reduce 
revocations to prison through better supervision practices. The incentives would be funded through money 
saved from a reduced prison population.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1yBHqFbNeeg1Vs5xzhOvyc5gMXHVnFkwZ?usp=sharing
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA: LOS distribution data
REFERENCE_DATE: January 2010
TIME_STEP: 1 month
"""
import pandas as pd

from recidiviz.calculator.modeling.population_projection.spark_bq_utils import upload_spark_model_inputs

transitions_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'placeholder_axis', 'compartment_duration', 'total_population'])
outflows_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'placeholder_axis', 'time_step', 'total_population'])
total_population_data = pd.DataFrame(columns=['compartment', 'placeholder_axis', 'time_step', 'total_population'])

# TRANSITIONS TABLE
transitions_data = pd.concat([transitions_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/TX/TX_data/Transitions Data-Table 1.csv')])
transitions_data = transitions_data.rename({'placeholder_axis': 'crime_type'}, axis=1)

# OUTFLOWS TABLE
outflows_data = pd.concat([outflows_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/TX/TX_data/Outflows Data-Table 1.csv')])
outflows_data = outflows_data.rename({'placeholder_axis': 'crime_type'}, axis=1)

# TOTAL POPULATION TABLE
total_population_data = pd.concat([total_population_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/TX/TX_data/Total Population Data-Table 1.csv')])
total_population_data = total_population_data.rename({'placeholder_axis': 'crime_type'}, axis=1)

# STORE DATA
upload_spark_model_inputs('recidiviz-staging', 'TX_PO_incentives', outflows_data, transitions_data,
                          total_population_data)
