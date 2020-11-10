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
STATE: ex_state
POLICY: max prison sentence reduction
VERSION: V2
DATA SOURCE: NA
DATA QUALITY: reasonable
HIGHEST PRIORITY MISSING DATA: LOS distribution by crime
ADDITIONAL NOTES: this dummy example data
"""

import pandas as pd
from numpy import mean
# pylint: skip-file


# RAW DATA
reference_date = 2015
crimes = ['drug_possession', 'drug_distribution', 'theft']

population_data = pd.DataFrame({
    'year': [-2, -1, 0, 1],
    'drug_possession': [100, 105, 105, 110],
    'drug_distribution': [20, 22, 23, 22],
    'theft': [40, 44, 49, 48]
})

admissions_data = pd.DataFrame({
    'year': [-2, -1, 0, 1],
    'drug_possession': [55, 60, 60, 62],
    'drug_distribution': [4, 5, 5, 5],
    'theft': [21, 22, 23, 24]
})

# for simplicity, i'm just coding up a single 1-year-out recidivism rate, even though this doesn't match the proposal
recidivism_rate_data = pd.DataFrame({
    'year': [-1, 0, 1],
    'drug_possession': [0.10, 0.11, 0.11],
    'drug_distribution': [0.20, 0.25, 0.25],
    'theft': [0.05, 0.06, 0.05]
})

transitions_data = \
    pd.DataFrame(columns=['compartment', 'outflow_to', 'total_population', 'compartment_duration', 'crime_type'])
outflows_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'total_population', 'time_step', 'crime_type'])
total_population_data = pd.DataFrame(columns=['compartment', 'total_population', 'time_step', 'crime_type'])

# TRANSITIONS TABLE
average_LOS = {crime: mean(population_data[crime]) / mean(admissions_data[crime]) for crime in crimes}

for crime in crimes:
    crime_transitions_data = pd.DataFrame({
        'compartment': ['prison', 'release', 'release'],
        'outflow_to': ['release', 'prison', 'release'],
        'total_population': [1, mean(recidivism_rate_data[crime]), 1 - mean(recidivism_rate_data[crime])],
        'compartment_duration': [average_LOS[crime], 1, 80],  # 80 years is a place-holder for "until death"
        'crime_type': [crime, crime, crime]
    })
    transitions_data = pd.concat([transitions_data, crime_transitions_data])


# OUTFLOWS TABLE
for crime in crimes:
    crime_outflows_data = pd.DataFrame({
        'compartment': ['pre-trial' for year in admissions_data.year],
        'outflow_to': ['prison' for year in admissions_data.year],
        'total_population': admissions_data[crime].to_list(),
        'time_step': admissions_data.year.to_list(),
        'crime_type': [crime for year in admissions_data.year]
    })
    outflows_data = pd.concat([outflows_data, crime_outflows_data])

# TOTAL POPULATION DATA
for crime in crimes:
    crime_total_population_data = pd.DataFrame({
        'compartment': ['prison' for year in admissions_data.year],
        'total_population': population_data[crime].to_list(),
        'time_step': admissions_data.year.to_list(),
        'crime_type': [crime for year in admissions_data.year]
    })
    total_population_data = pd.concat([total_population_data, crime_total_population_data])


# STORE DATA
state = 'ex_state'
primary_compartment = 'prison'
pd.concat([transitions_data, outflows_data, total_population_data]).to_csv(
    f'spark/state/{state}/preprocessed_data_{state}_{primary_compartment}.csv')
