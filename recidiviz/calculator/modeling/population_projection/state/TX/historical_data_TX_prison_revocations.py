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
# pylint: disable-all
transitions_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'placeholder_axis', 'compartment_duration', 'total_population'])
outflows_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'placeholder_axis', 'time_step', 'total_population'])
total_population_data = pd.DataFrame(columns=['compartment', 'placeholder_axis', 'time_step', 'total_population'])

# TRANSITIONS TABLE
transitions_data = pd.concat([transitions_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/TX/TX_data/Transitions Data-Table 1.csv')])

# OUTFLOWS TABLE
outflows_data = pd.concat([outflows_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/TX/TX_data/Outflows Data-Table 1.csv')])

#TOTAL POPULATION TABLE
total_population_data = pd.concat([total_population_data, pd.read_csv('recidiviz/calculator/modeling/population_projection/state/TX/TX_data/Total Population Data-Table 1.csv')])

# STORE DATA
pd.concat([transitions_data, outflows_data, total_population_data], sort=False).to_csv(
    'recidiviz/calculator/modeling/population_projection/state/TX/preprocessed_data_TX_prison_revocations.csv')

