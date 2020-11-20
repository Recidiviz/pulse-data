"""
STATE: AZ
POLICY: Increase amount of earned time by 15% for non-violent offenses without violent history
VERSION: [version of model at time of implementation]
DATA SOURCE: https://docs.google.com/spreadsheets/d/1PtOgwn20B0x3pGdVKwIZyYzw4_b04hZrbbWiuagNHT0/edit#gid=0&range=Q15
DATA QUALITY: reasonable
HIGHEST PRIORITY MISSING DATA: violent history
REFERENCE_DATE: 2020
TIME_STEP: year
"""
import pandas as pd
import numpy as np

historical_data_2019 = pd.read_csv(
    'recidiviz/calculator/modeling/population_projection/state/AZ/AZ_data/2019.csv', sep=';', thousands=','
)
historical_data_2018 = pd.read_csv(
    'recidiviz/calculator/modeling/population_projection/state/AZ/AZ_data/2018.csv', sep=';', thousands=','
)
historical_data_2017 = pd.read_csv(
    'recidiviz/calculator/modeling/population_projection/state/AZ/AZ_data/2017.csv', sep=';', thousands=','
)

crimes = historical_data_2019.crime.to_list()

is_violent_map = {
    'arson': True,
    'assault': True,
    'auto_theft': False,
    'burglary_or_criminal_trespass': True, # only certain kinds of burglary
    'child_or_adult_abuse': True, # only child abuse would be violent?
    'child_molestation': True,
    'criminal_damage': False,
    'domestic_violence': False,
    'drug_possession': False,
    'drug_trafficking': False,
    'dui': False,
    'escape': False,
    'forgery': False,
    'fraud': False,
    'identity_theft': False,
    'kidnapping': True,
    'manslaughter_or_neg_homicide': True,
    'murder': True,
    'other': False,
    'rape_or_sexual_assault': True,
    'robbery': True,
    'sex_offense': False,
    'theft': False,
    'trafficking_in_stolen_property': False,
    'weapons_offense': True,
}

def get_field_for_crime(df, field, crime_type):
    return df.loc[df['crime'] == crime_type][field].to_list()[0]

def get_yearly_fields_for_crime(field, crime_type):
    return [
        get_field_for_crime(historical_data_2017, field, crime_type),
        get_field_for_crime(historical_data_2018, field, crime_type),
        get_field_for_crime(historical_data_2019, field, crime_type)
    ]

population_by_crime = {crime: get_yearly_fields_for_crime('total', crime) for crime in crimes}
population_data = pd.DataFrame({
    'year': [-3, -2, -1],
    **population_by_crime
})

admissions_by_crime = {crime: get_yearly_fields_for_crime('admissions', crime) for crime in crimes}
admissions_data = pd.DataFrame({
    'year': [-3, -2, -1],
    **admissions_by_crime
})

releases_by_crime = {crime: get_yearly_fields_for_crime('releases', crime) for crime in crimes}
releases_data = pd.DataFrame({
    'year': [-3, -2, -1],
    **releases_by_crime
})

transitions_data = pd.DataFrame(
    columns=['compartment', 'outflow_to', 'total_population', 'compartment_duration', 'crime_type', 'is_violent'])
outflows_data = pd.DataFrame(
    columns=['compartment', 'outflow_to', 'total_population', 'time_step', 'crime_type', 'is_violent'])
total_population_data = pd.DataFrame(
    columns=['compartment', 'total_population', 'time_step', 'crime_type', 'is_violent'])

# TRANSITIONS TABLE
recidivism_3_year_rate = 0.391
average_LOS = {crime: np.mean(population_data[crime]) / np.mean(admissions_data[crime]) for crime in crimes}

for crime in crimes:
    crime_transitions_data = pd.DataFrame({
        'compartment': ['prison', 'release', 'release', 'release', 'release'],
        'outflow_to': ['release', 'prison', 'prison', 'prison', 'release'],
        'total_population': [
            1,
            recidivism_3_year_rate / 3,
            recidivism_3_year_rate / 3,
            recidivism_3_year_rate / 3,
            1 - recidivism_3_year_rate
        ],
        'compartment_duration': [average_LOS[crime], 1, 2, 3, 80],
        'crime_type': [crime] * 5,
        'is_violent': [is_violent_map[crime]] * 5
    })
    transitions_data = pd.concat([transitions_data, crime_transitions_data])

# OUTFLOWS TABLE
for crime in crimes:
    admissions_outflows_data = pd.DataFrame({
        'compartment': ['pre-trial' for year in admissions_data.year],
        'outflow_to': ['prison' for year in admissions_data.year],
        'total_population': admissions_data[crime].to_list(),
        'time_step':admissions_data.year.to_list(),
        'crime_type': [crime for year in admissions_data.year],
        'is_violent': [is_violent_map[crime] for year in admissions_data.year],
    })
    prison_outflows_data = pd.DataFrame({
        'compartment': ['prison' for year in releases_data.year],
        'outflow_to': ['release' for year in releases_data.year],
        'total_population': releases_data[crime].to_list(),
        'time_step': releases_data.year.to_list(),
        'crime_type': [crime for year in releases_data.year],
        'is_violent': [is_violent_map[crime] for year in releases_data.year],
    })
    outflows_data = pd.concat([outflows_data, admissions_outflows_data, prison_outflows_data])

# TOTAL POPULATION TABLE
for crime in crimes:
    crime_total_population = pd.DataFrame({
        'compartment': ['prison' for year in population_data.year],
        'total_population': population_data[crime].to_list(),
        'time_step': population_data.year.to_list(),
        'crime_type': [crime for year in population_data.year],
        'is_violent': [is_violent_map[crime] for year in population_data.year],
    })
    total_population_data = pd.concat([total_population_data, crime_total_population])

pd.concat([transitions_data, outflows_data, total_population_data]).to_csv(
    'recidiviz/calculator/modeling/population_projection/state/AZ/preprocessed_data_AZ_prison_2808.csv'
)
