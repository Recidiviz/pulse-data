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

STATE: VA
POLICY: mandatory minimum reduction and earned time
VERSION: v1
DATA SOURCE: not all but #https://www.vadoc.virginia.gov/media/1484/vadoc-state-recidivism-report-2020-02.pdf
    and #https://vadoc.virginia.gov/media/1166/vadoc-offender-population-forecasts-2019-2024.pdf
    and #http://www.vcsc.virginia.gov/Man_Min.pdf
DATA QUALITY: great
HIGHEST PRIORITY MISSING DATA: more years of data
ADDITIONAL NOTES: NA
"""
import pandas as pd
# pylint: skip-file


#RAW DATA
release_to_incarceration = pd.DataFrame({
    2011: [0.047, 0.145, 0.23],
    2012: [0.044, 0.15, 0.234],
    2013: [0.043, 0.15, 0.224],
    2014: [0.05, 0.153, 0.234],
    2015: [0.049, 0.15, 0.231],
    2016: [0.05, None, None],
    2017: [0.049, None, None]
}, index=[i+1 for i in range(3)]).ffill(axis=1)
release_to_incarceration.loc[3] -= release_to_incarceration.loc[2]
release_to_incarceration.loc[2] -= release_to_incarceration.loc[1]
cohort_sizes = {2011: 12263, 2012: 11496, 2013: 11575, 2014: 12021, 2015: 12385, 2016: 12554, 2017: 12415, 2018: 12502}

total_state_responsible_adult_population = pd.Series({
    2007: 38369,
    2008: 39158,
    2009: 38809,
    2010: 38178,
    2011: 37983,
    2012: 37849,
    2013: 38337,
    2014: 38871,
    2015: 38761,
    2016: 38264,
    2017: 37762,
    2018: 37254,
    2019: 37177,
    2020: 37254,
    2021: 37382,
    2022: 37525,
    2023: 37656,
    2024: 37837
})

mandatory_minimums = {
    'NAR3085': 5,
    'NAR3119': 20,
    'NAR3124': 40,
    'NAR3114': 20,
    'NAR3067': 3,
    'NAR3086': 10,
    'NAR3126': 2,
    'NAR3127': 5,
    'NAR3128': 5,
    'NAR3063': 2,
    'NAR3098': 5,
    'NAR3097': 5,
    'NAR3090': 20,
    'NAR3092': 40,
    'NAR3118': 20,
    'NAR3123': 40,
    'NAR3117': 20,
    'NAR3122': 40,
    'NAR3116': 20,
    'NAR3121': 40,
    'NAR3120': 20,
    'NAR3125': 40,
    'NAR3091': 20,
    'NAR3093': 40,
    'NAR3038': 3,
    'NAR3087': 10,
    'NAR3146': 5,
    'NAR3113': 20,
    'NAR3145': 5,
    'NAR3112': 20,
    'NAR3144': 5,
    'NAR3111': 40,
    'NAR3147': 5,
    'NAR3149': 3,
    'NAR3115': 20,
    'NAR3151': 5,
    'NAR3133': 3,
    'NAR3065': 3,
    'NAR3088': 10,
    'NAR3066': 3,
    'NAR3089': 10,
    'NAR3099': 1,
    'NAR3041': 1,
    'NAR3094': 20,
    'WPN5296': 5,
    'WPN5297': 2,
}

VA_validation_data = pd.DataFrame(columns=['time_step', 'compartment', 'outflow_to', 'count'])
VA_validation_data = VA_validation_data.append({
    'time_step': 2019,
    'compartment': ['prison', 'jail'],
    'outflow_to': 'release',
    'count': 12819
}, ignore_index=True)
VA_validation_data = VA_validation_data.append({
    'time_step': 2016,
    'compartment': ['prison', 'jail'],
    'outflow_to': 'release',
    'count': 12650
}, ignore_index=True)


def add_recidivism_data(historical_data):
    for year in set(historical_data['time_step']):
        year_data = historical_data[historical_data['time_step'] == year]
        total_admissions = len(year_data)
        recidivism_admissions = get_recidivism_admissions(year)
        recidivism_fraction = recidivism_admissions / total_admissions
        for category in set(year_data['simulation_group_name']):
            r_indices = year_data[year_data['simulation_group_name'] == category].sample(frac=recidivism_fraction).index
            for index in r_indices:
                historical_data.loc[index, 'outflow_from'] = 'release'

    return historical_data


def get_recidivism_admissions(year):
    revocations = 0
    for yr in range(1, 4):
        cohort = year - yr
        if cohort < min(cohort_sizes):
            cohort_size = cohort_sizes[min(cohort_sizes)]
        elif cohort > max(cohort_sizes):
            cohort_size = cohort_sizes[max(cohort_sizes)]
        else:
            cohort_size = cohort_sizes[cohort]

        if cohort < min(release_to_incarceration.columns):
            recidivism_rate = release_to_incarceration[min(release_to_incarceration.columns)][yr]
        elif cohort > max(release_to_incarceration.columns):
            recidivism_rate = release_to_incarceration[max(release_to_incarceration.columns)][yr]
        else:
            recidivism_rate = release_to_incarceration[cohort][yr]

        revocations += recidivism_rate * cohort_size

    return revocations

#you may have to change this path to point to wherever you have the file on your computer
historical_sentences = pd.read_csv('spark/state/VA/VA_data/processed_va_historical_sentences_v2.csv')

# Filter to the supported sentence types
supported_sentence_types = ['jail', 'prison']
historical_sentences = historical_sentences[historical_sentences['sentence_type'].isin(supported_sentence_types)]

# Remove sentences with 0 month sentence lengths
historical_sentences = historical_sentences[historical_sentences['effective_sentence_months'] > 0].copy()


#TRANSITIONS TABLE
jail_prison_sentences = historical_sentences[['offense_group', 'offense_code', 'sentence_type',
                                              'effective_sentence_years']]\
    .groupby(['offense_code', 'sentence_type', 'effective_sentence_years'], as_index=False).count()
jail_prison_sentences = jail_prison_sentences.rename({'offense_group': 'total_population',
                                                          'effective_sentence_years': 'compartment_duration',
                                                          'sentence_type': 'inflow_to'}, axis=1)
jail_prison_sentences = jail_prison_sentences.rename({'inflow_to': 'compartment'}, axis=1)
jail_prison_sentences['outflow_to'] = 'release'

for offense_code in set(jail_prison_sentences['offense_code']):
    jail_prison_sentences = jail_prison_sentences.append({'offense_code': offense_code, 'compartment': 'release',
                                                          'compartment_duration': 100, 'total_population': 1,
                                                          'outflow_to': 'prison'}, ignore_index=True)
transitions_data = jail_prison_sentences

#OUTFLOWS TABLE
jail_prison_admissions = historical_sentences[['offense_group', 'off1_vcc', 'offense_code', 'time_step', 'sentence_type', 'compartment']]\
    .groupby(['offense_group', 'offense_code', 'compartment', 'sentence_type', 'time_step'], as_index=False).count()
jail_prison_admissions = jail_prison_admissions.rename({'off1_vcc': 'total_population',
                                                        'sentence_type': 'outflow_to'}, axis=1)
outflows_data = jail_prison_admissions

#STORE DATA
state = 'VA'
primary_compartment = 'parole'
pd.concat([transitions_data, outflows_data], sort=False).to_csv(
    f'spark/state/{state}/preprocessed_data_{state}_{primary_compartment}.csv')
