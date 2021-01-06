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
STATE: MI
POLICY: probation cap reduction
VERSION: V2
DATA SOURCE: https://docs.google.com/spreadsheets/d/1zDQ0AFRz_bcXvjhSnuBj7g3uWKkqldo4S5V5orD99_M/edit#gid=0
DATA QUALITY: not ideal, but making the most of it...
HIGHEST PRIORITY MISSING DATA: state-specific LOS distribution for felony probation, discharge statistics
ADDITIONAL NOTES: model built to support policy memo for MI SB1050
"""

import pandas as pd
from numpy import mean
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import upload_spark_model_inputs
# pylint: skip-file


# compartments needed: probation, prison, prison_technicals, release
# relevant policy facts: probation cap reduced from five years to three years for felony charges

# INPUT DATA (with some extrapolated estimates; see spreadsheet)

# total number of individuals on probation for felonies
total_population = pd.DataFrame({
    'year': [i for i in range(2011, 2019)],
    'felony': [53375, 49458, 47526, 49643, 45135, 44931, 43482, 43048],
})

# number of individuals placed on probation, either from prison or directly from sentencing
admissions = pd.DataFrame({
    'year': [i for i in range(2011, 2019)],
    'felony': [30446, 29432, 28282, 29542, 26859, 26738, 25876, 25617],
})

# number of individuals with probation for felony revoked
revocations = pd.DataFrame({
    'year': [i for i in range(2011, 2019)],
    'totals': [8393, 7777, 7474, 7807, 7098, 7066, 6838, 6769],
    'technicals': [4627, 4287, 4120, 4303, 3912, 3895, 3769, 3732],
    'new_offenses': [3767, 3490, 3354, 3503, 3185, 3171, 3069, 3038],
})

reference_date = 2016

total_population_months = pd.DataFrame()
admissions_months = pd.DataFrame()
revocations_months = pd.DataFrame()

for year in total_population.year:
    monthly_total_population = pd.DataFrame({
        'month': [i for i in range((year - reference_date) * 12, (year - reference_date + 1) * 12)],
        'felony': [total_population[total_population.year == year]['felony'].iloc[0] for month in range(12)]
    })
    total_population_months = pd.concat([total_population_months, monthly_total_population])

    monthly_admissions = pd.DataFrame({
        'month': [i for i in range((year - reference_date) * 12, (year - reference_date + 1) * 12)],
        'felony': [admissions[admissions.year == year]['felony'].iloc[0] / 12 for month in range(12)]
    })
    admissions_months = pd.concat([admissions_months, monthly_admissions])

    monthly_revocations = pd.DataFrame({
        'month': [i for i in range((year - reference_date) * 12, (year - reference_date + 1) * 12)],
        'totals': [revocations[total_population.year == year]['totals'].iloc[0] / 12 for month in range(12)],
        'technicals': [revocations[total_population.year == year]['technicals'].iloc[0] / 12 for month in range(12)],
        'new_offenses': [revocations[total_population.year == year]['new_offenses'].iloc[0] / 12 for month in range(12)]
    })
    revocations_months = pd.concat([revocations_months, monthly_revocations])

# TRANSITIONS TABLE
mean_completion_duration = mean(total_population['felony']) / mean(admissions['felony'])

# out of total probation population
mean_completion_fraction = 1 - mean(revocations['totals']) / mean(admissions['felony'])
mean_fraction_of_technicals = \
    (1 - mean_completion_fraction) * mean(revocations['technicals']) / mean(revocations['totals'])
mean_fraction_of_new_offenses = \
    (1 - mean_completion_fraction) * mean(revocations['new_offenses']) / mean(revocations['totals'])

assert round(mean_fraction_of_new_offenses + mean_fraction_of_technicals + mean_completion_fraction) == 1

mean_completion_duration_monthly = mean_completion_duration * 12


# populate transition table (with probabilities, rather than absolute numbers); aggregated jail and prison outflows
probation_transition_table = pd.DataFrame({
    'compartment': ['probation'] * 3,
    'compartment_duration': [mean_completion_duration] * 3,
    'outflow_to': ['release', 'prison', 'prison_technicals'],  # prison refers to new offenses in this context
    'total_population': [mean_completion_fraction, mean_fraction_of_new_offenses, mean_fraction_of_technicals]
})
probation_transition_table_monthly = pd.DataFrame({
    'compartment': ['probation'] * 3,
    'compartment_duration': [mean_completion_duration_monthly] * 3,
    'outflow_to': ['release', 'prison', 'prison_technicals'],  # prison refers to new offenses in this context
    'total_population': [mean_completion_fraction, mean_fraction_of_new_offenses, mean_fraction_of_technicals]
})

secondary_transition_table = pd.DataFrame({
    'compartment': ['prison_technicals', 'release', 'prison'],
    'compartment_duration': [0.08, 1, 1],  # average length of stay capped at 30 days for prison_technicals
    'outflow_to': ['probation', 'release', 'prison'],
    'total_population': [1, 1, 1]
})

secondary_transition_table_monthly = pd.DataFrame({
    'compartment': ['prison_technicals', 'release', 'prison'],
    'compartment_duration': [1, 12, 12],  # average length of stay capped at 30 days for prison_technicals
    'outflow_to': ['probation', 'release', 'prison'],
    'total_population': [1, 1, 1]
})

# yearly version
# transitions_data = pd.concat([probation_transition_table, secondary_transition_table])

# monthly version
transitions_data = pd.concat([probation_transition_table_monthly, secondary_transition_table_monthly])

transitions_data['crime_type'] = 'felony'

# OUTFLOWS TABLE
outflows_data = pd.DataFrame()
admissions_outflows_data = pd.DataFrame({
    'compartment': ['prison_shell'] * 8,
    'time_step': admissions['year'],
    'outflow_to': ['probation'] * 8,
    'total_population': admissions['felony']
})

admissions_outflows_data_monthly = pd.DataFrame({
    'compartment': ['prison_shell'] * 8 * 12,
    'time_step': admissions_months['month'],
    'outflow_to': ['probation'] * 8 * 12,
    'total_population': admissions_months['felony']
})


technical_revocations_outflows_data = pd.DataFrame({
    'compartment': ['probation'] * 8,
    'time_step': revocations['year'],
    'outflow_to': ['prison_technicals'] * 8,
    'total_population': revocations['technicals']
})

technical_revocations_outflows_data_monthly = pd.DataFrame({
    'compartment': ['probation'] * 8 * 12,
    'time_step': revocations_months['month'],
    'outflow_to': ['prison_technicals'] * 8 * 12,
    'total_population': revocations_months['technicals']
})

new_offense_revocations_outflows_data = pd.DataFrame({
    'compartment': ['probation'] * 8,
    'time_step': revocations['year'],
    'outflow_to': ['prison'] * 8,
    'total_population': revocations['new_offenses']
})

new_offense_revocations_outflows_data_monthly = pd.DataFrame({
    'compartment': ['probation'] * 8 * 12,
    'time_step': revocations_months['month'],
    'outflow_to': ['prison'] * 8 * 12,
    'total_population': revocations_months['new_offenses']
})

# yearly version
# outflows_data = pd.concat([outflows_data, admissions_outflows_data, technical_revocations_outflows_data,
#                            new_offense_revocations_outflows_data])

# monthly version
outflows_data = pd.concat([outflows_data, admissions_outflows_data_monthly,
                           technical_revocations_outflows_data_monthly, new_offense_revocations_outflows_data_monthly])
outflows_data['crime_type'] = 'felony'

#TOTAL POPULATION TABLE
total_population_data = total_population.copy().rename({'year': 'time_step', 'felony': 'total_population'}, axis=1)
# monthly version
total_population_data = \
    total_population_months.copy().rename({'month': 'time_step', 'felony': 'total_population'}, axis=1)

total_population_data['compartment'] = 'probation'
total_population_data['crime_type'] = 'felony'

# STORE DATA
upload_spark_model_inputs('recidiviz-staging', 'MI_probation', outflows_data, transitions_data,
                          total_population_data)
