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
STATE: AZ
POLICY: Allow judges discretion in deviating from mandatory minimum sentences
VERSION: [version of model at time of implementation]
DATA SOURCE: https://docs.google.com/spreadsheets/d/1PtOgwn20B0x3pGdVKwIZyYzw4_b04hZrbbWiuagNHT0/edit#gid=0&range=Q15
DATA QUALITY: reasonable
HIGHEST PRIORITY MISSING DATA: specific HB_2376 history
REFERENCE_DATE: 2020
TIME_STEP: year
"""
import pandas as pd
import numpy as np
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import (
    upload_spark_model_inputs,
)

historical_data_2019 = pd.read_csv("AZ_data/HB_2376/2019.csv", sep=";", thousands=",")
historical_data_2018 = pd.read_csv("AZ_data/HB_2376/2018.csv", sep=";", thousands=",")
historical_data_2017 = pd.read_csv("AZ_data/HB_2376/2017.csv", sep=";", thousands=",")

felony_classes = historical_data_2019.felony_class.to_list()


def get_field_for_felony_class(df, field, felony_class_type):
    return df.loc[df["felony_class"] == felony_class_type][field].to_list()[0]


def get_yearly_fields_for_felony_class(field, felony_class_type):
    return [
        get_field_for_felony_class(historical_data_2017, field, felony_class_type),
        get_field_for_felony_class(historical_data_2018, field, felony_class_type),
        get_field_for_felony_class(historical_data_2019, field, felony_class_type),
    ]


population_by_felony_class = {
    felony_class: get_yearly_fields_for_felony_class("total", felony_class)
    for felony_class in felony_classes
}
population_data = pd.DataFrame({"year": [-3, -2, -1], **population_by_felony_class})

admissions_by_felony_class = {
    felony_class: get_yearly_fields_for_felony_class("admissions", felony_class)
    for felony_class in felony_classes
}
admissions_data = pd.DataFrame({"year": [-3, -2, -1], **admissions_by_felony_class})

releases_by_felony_class = {
    felony_class: get_yearly_fields_for_felony_class("releases", felony_class)
    for felony_class in felony_classes
}
releases_data = pd.DataFrame({"year": [-3, -2, -1], **releases_by_felony_class})

transitions_data = pd.DataFrame(
    columns=[
        "compartment",
        "outflow_to",
        "total_population",
        "compartment_duration",
        "felony_class",
    ]
)
outflows_data = pd.DataFrame(
    columns=[
        "compartment",
        "outflow_to",
        "total_population",
        "time_step",
        "felony_class",
    ]
)
total_population_data = pd.DataFrame(
    columns=["compartment", "total_population", "time_step", "felony_class"]
)

# TRANSITIONS TABLE
def get_mean_for_felony_class(felony_class_type):
    return np.mean(population_data[felony_class_type]) / np.mean(
        admissions_data[felony_class_type]
    )


average_LOS = {
    felony_class: get_mean_for_felony_class(felony_class)
    for felony_class in felony_classes
}

recidivism_3_year_rate = 0.391

for felony_class in felony_classes:
    felony_transitions_data = pd.DataFrame(
        {
            "compartment": ["prison", "release", "release", "release", "release"],
            "outflow_to": ["release", "prison", "prison", "prison", "release"],
            "total_population": [
                1,
                recidivism_3_year_rate / 3,
                recidivism_3_year_rate / 3,
                recidivism_3_year_rate / 3,
                1 - recidivism_3_year_rate,
            ],
            "compartment_duration": [average_LOS[felony_class], 1, 2, 3, 80],
            "felony_class": [felony_class] * 5,
        }
    )
    transitions_data = pd.concat([transitions_data, felony_transitions_data])

# OUTFLOWS TABLE
for felony_class in felony_classes:
    admissions_outflows_data = pd.DataFrame(
        {
            "compartment": ["pre-trial" for year in admissions_data.year],
            "outflow_to": ["prison" for year in admissions_data.year],
            "total_population": admissions_data[felony_class].to_list(),
            "time_step": admissions_data.year.to_list(),
            "felony_class": [felony_class for year in admissions_data.year],
        }
    )
    prison_outflows_data = pd.DataFrame(
        {
            "compartment": ["prison" for year in releases_data.year],
            "outflow_to": ["release" for year in releases_data.year],
            "total_population": releases_data[felony_class].to_list(),
            "time_step": releases_data.year.to_list(),
            "felony_class": [felony_class for year in releases_data.year],
        }
    )
    outflows_data = pd.concat(
        [outflows_data, admissions_outflows_data, prison_outflows_data]
    )

# TOTAL POPULATION TABLE
for felony_class in felony_classes:
    felony_total_population = pd.DataFrame(
        {
            "compartment": ["prison" for year in population_data.year],
            "total_population": population_data[felony_class].to_list(),
            "time_step": population_data.year.to_list(),
            "felony_class": [felony_class for year in population_data.year],
        }
    )
    total_population_data = pd.concat([total_population_data, felony_total_population])

# STORE DATA
outflows_data = outflows_data.rename({"felony_class": "crime_type"}, axis=1)
transitions_data = transitions_data.rename({"felony_class": "crime_type"}, axis=1)
total_population_data = total_population_data.rename(
    {"felony_class": "crime_type"}, axis=1
)

final_outflows = pd.DataFrame()
for year in outflows_data.time_step.unique():
    year_outflows = outflows_data[outflows_data.time_step == year]
    for month in range(12):
        month_outflows = year_outflows.copy()
        month_outflows.time_step = 12 * month_outflows.time_step - month
        month_outflows.total_population /= 12
        final_outflows = pd.concat([final_outflows, month_outflows])
outflows_data = final_outflows

transitions_data.compartment_duration *= 12

final_pops = pd.DataFrame()
for year in total_population_data.time_step.unique():
    year_pops = total_population_data[total_population_data.time_step == year]
    for month in range(12):
        month_pops = year_pops.copy()
        month_pops.time_step = 12 * month_pops.time_step - month
        final_pops = pd.concat([final_pops, month_pops])
total_population_data = final_pops


upload_spark_model_inputs(
    "recidiviz-staging",
    "AZ_HB_2376",
    outflows_data,
    transitions_data,
    total_population_data,
)
