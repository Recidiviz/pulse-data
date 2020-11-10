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
Historical data to be ingested for a particular state x policy combination
file name should be `historical_data_{state_code}_{primary_compartment}.py`
    where state_code is of form NJ and primary compartment is the tag of the main compartment relevant to the policy

STATE: NJ
POLICY: early parole policy that shortens prison sentences for non-violent crimes
VERSION: v1
DATA SOURCE: IDK
DATA QUALITY: MVP
HIGHEST PRIORITY MISSING DATA: prison LOS data disaggregated by crime-type
ADDITIONAL NOTES: N/A
"""

import pandas as pd
import numpy as np
# pylint: skip-file


#RAW DATA
reference_year = 2016

total_population_data = pd.DataFrame({
    'time_step': [i - reference_year for i in range(2011, 2021)] * 2,
    'total_population': [1836, 1706, 1665, 1557, 1419, 1276, 1137, 1048, 904, 817] +
                        [3741, 3128, 2825, 2411, 2286, 2010, 1749, 1639, 1541, 1525],
    'compartment': ['prison'] * 20,
    'offense_group': ['PROPERTY'] * 10 + ['DRUG'] * 10
})

# NJ Historical counts of adult offenders in the prison system at the beginning of the year
# 5595 admissions 2018
historical_offender_counts = pd.DataFrame({
    'time_step': [i - reference_year for i in range(2020,2010,-1)],
    'drug_sentences': [1525, 1541, 1639, 1749, 2010, 2286, 2411, 2825, 3128, 3741],
    'property_sentences': [817, 904, 1048, 1137, 1276, 1419, 1557, 1665, 1706, 1836],
    'property_median_sentence_length': [3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3.5, 3],
    'drug_median_sentence_length': [2.5, 2, 2, 2, 2, 2, 2, 2, 1.75, 1.5]
}).sort_values(by='time_step')
historical_offender_counts['drug_p_y'] = 1 - (1/historical_offender_counts['drug_median_sentence_length'])
historical_offender_counts['property_p_y'] = 1 - (1/historical_offender_counts['property_median_sentence_length'])
historical_offender_counts['drug_releases'] = \
    np.round(historical_offender_counts['drug_sentences']
             - (historical_offender_counts['drug_sentences'] * historical_offender_counts['drug_p_y']), 0).astype(int)
historical_offender_counts['property_releases'] = \
    np.round(historical_offender_counts['property_sentences']
             - (historical_offender_counts['property_sentences'] * historical_offender_counts['property_p_y']),
             0).astype(int)
historical_offender_counts['drug_admissions'] = historical_offender_counts['drug_releases'] \
    - historical_offender_counts['drug_sentences'].diff(periods=-1)
historical_offender_counts['property_admissions'] = historical_offender_counts['property_releases'] \
    - historical_offender_counts['property_sentences'].diff(periods=-1)


transitions_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'total_population', 'compartment_duration',
                                         'offense_group'])
outflows_data = pd.DataFrame(columns=['compartment', 'outflow_to', 'total_population', 'time_step', 'offense_group'])


#TRANSITIONS TABLE
# Convert offender counts DF to 1 row per offense & sentence length
drug_sentences = historical_offender_counts[['drug_median_sentence_length', 'drug_admissions']]
drug_sentences = drug_sentences.rename({'drug_median_sentence_length': 'compartment_duration',
                                        'drug_admissions': 'total_population'}, axis=1)
drug_sentences['offense_group'] = 'DRUG'

property_sentences = historical_offender_counts[['property_median_sentence_length', 'property_admissions']].copy()
property_sentences = property_sentences.rename({'property_median_sentence_length': 'compartment_duration',
                                                'property_admissions': 'total_population'}, axis=1)
property_sentences['offense_group'] = 'PROPERTY'

historical_sentences = pd.concat([drug_sentences, property_sentences])
historical_sentences['compartment'] = 'prison'
historical_sentences['outflow_to'] = 'release'
historical_sentences = historical_sentences[historical_sentences['total_population'].notnull()]

# Sum all rows with the same offense group and compartment duration
historical_sentences = historical_sentences.groupby(['compartment_duration', 'offense_group', 'compartment',
                                                     'outflow_to'], as_index=False).sum()


historical_sentences = historical_sentences.append(
    pd.DataFrame({'compartment_duration': [100, 100],
                  'offense_group': ['DRUG', 'PROPERTY'],
                  'compartment': ['release', 'release'],
                  'outflow_to': ['prison', 'prison'],
                  'total_population': [1, 1]}).reset_index(drop=True)
)

transitions_data = historical_sentences

#OUTFLOWS TABLE
drug_admissions = historical_offender_counts[['time_step', 'drug_admissions']]
drug_admissions = drug_admissions.rename({'drug_admissions': 'total_population'}, axis=1)
drug_admissions['offense_group'] = 'DRUG'

property_admissions = historical_offender_counts[['time_step', 'property_admissions']].copy()
property_admissions = property_admissions.rename({'property_admissions': 'total_population'}, axis=1)
property_admissions['offense_group'] = 'PROPERTY'

historical_admissions = pd.concat([drug_admissions, property_admissions])
historical_admissions['compartment'] = 'pretrial'
historical_admissions['outflow_to'] = 'prison'
historical_admissions = historical_admissions[historical_admissions['total_population'].notnull()]

outflows_data = historical_admissions

#STORE DATA
state = 'NJ'
primary_compartment = 'prison'
pd.concat([transitions_data, outflows_data, total_population_data], sort=False).to_csv(
    f'spark/state/{state}/preprocessed_data_{state}_{primary_compartment}.csv')
