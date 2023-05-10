# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
STATE: Massachusetts
POLICY: SB 920 Raise the age
VERSION: V1
DATA SOURCE: https://docs.google.com/spreadsheets/d/1vXDTgXOz8p5CMT03WNFEhQJhiiYxEyx3Dcyxdm5Ui6g/edit?usp=sharing
DESCRIPTION: Preprocessing the House of Corrections data from 3 counties in MA and extrapolating to all counties
"""

from datetime import date, datetime

import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.super_simulation.time_converter import (
    TimeConverter,
)

COUNTY_PROP = 0.19142212189616253

hoc = pd.read_csv("Cleaned HOC Data.csv", parse_dates=["COMDATE", "RELDATE"])
hoc = hoc[hoc["LOS (days)"] > 0].copy()

# Convert compartment_duration from daily to monthly granularity
hoc["compartment_duration"] = hoc["LOS (days)"] / 30.44
hoc["compartment_duration"] = np.ceil(hoc["compartment_duration"])

hoc = hoc.rename(columns={"AGE": "age"})

# Only keep sentences for those who would be eligible for this policy
hoc = hoc[hoc["age"] <= 20]

# Count the admissions per month/age
# Only include admissions since Jan 2016 to match the DOC data
admissions = (
    hoc[hoc["COMDATE"] >= datetime(2016, 1, 1)]
    .groupby(["COMDATE", "age"])
    .size()
    .reset_index(name="total_population")
)

admissions = admissions.sort_values(by="COMDATE", ascending=False)
admissions["compartment"], admissions["outflow_to"] = ["pretrial", "HOC"]

# Count the releases per month/age
# Drop releases that occurred after the data transfer date, Nov 2021
releases = (
    hoc[hoc["RELDATE"].fillna(datetime(9999, 1, 1)) < datetime(2021, 11, 1)]
    .groupby(["RELDATE", "age"])
    .size()
    .reset_index(name="total_population")
)
releases["compartment"], releases["outflow_to"] = ["HOC", "release"]

time_converter = TimeConverter(
    reference_year=2021.833333333333333, time_step=0.083333333333
)
outflow_data = pd.DataFrame()
for date_column, df in {"COMDATE": admissions, "RELDATE": releases}.items():
    df[date_column] = pd.to_datetime(df[date_column])
    df["time_step"] = df[date_column].apply(
        time_converter.convert_timestamp_to_time_step
    )
    df = df.astype({"time_step": int})
    outflow_data = pd.concat([outflow_data, df])

outflow_data = outflow_data[
    ["compartment", "outflow_to", "total_population", "time_step", "age"]
]

outflow_data.total_population /= COUNTY_PROP
outflow_data["total_population"] = np.round(outflow_data["total_population"])

outflow_data.sort_values(
    by=["compartment", "outflow_to", "age", "time_step"],
    ascending=[False, False, True, False],
).to_csv("HOC_outflows.csv", index=False)

transitions = (
    hoc.groupby(["compartment_duration", "age"])
    .size()
    .reset_index(name="total_population")
)
transitions["compartment"], transitions["outflow_to"] = ["HOC", "release"]
transitions = transitions[
    ["compartment", "outflow_to", "total_population", "compartment_duration", "age"]
]

transitions.to_csv("HOC_transitions.csv", index=False)

population_data = pd.DataFrame()
population_date_array = pd.date_range(
    date(2015, 11, 1),
    date(2021, 11, 1),
    freq="MS",
)
for month in population_date_array:
    incarcerated_population = hoc[
        (hoc["COMDATE"] <= month) & (month < hoc["RELDATE"].fillna(date(9999, 1, 1)))
    ]
    monthly_population = (
        incarcerated_population.groupby("age").count()["COMDATE"].reset_index()
    )
    time_step = np.round((month - datetime(2021, 11, 1)) / np.timedelta64(1, "M"))
    monthly_population["time_step"] = time_step
    population_data = pd.concat([population_data, monthly_population])

# Create the final population df
population_data.reset_index(drop=True, inplace=True)
population_data = population_data.rename(columns={"COMDATE": "total_population"})
population_data["compartment"] = "HOC"
population_data["time_step"] = population_data["time_step"].astype(int)
population_data["total_population"] = np.round(
    population_data["total_population"] / COUNTY_PROP
)

population_data[["compartment", "total_population", "time_step", "age"]].sort_values(
    by=["age", "time_step"], ascending=[True, False]
).to_csv("HOC_population.csv", index=False)
