# type: ignore
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
STATE: Federal
POLICY: Smarter Sentencing Act: reducing mandatory minimums for drug trafficking offenses
VERSION: V1
DATA SOURCE: https://docs.google.com/spreadsheets/d/18-9ZNenYOxnoT3VOZJPdzWgILjpnexLT4uBlmJKKHew/edit?usp=sharing
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA: Average LOS associated with each mandatory minimum
REFERENCE_DATE: January 2020
TIME_STEP: Month
ADDITIONAL NOTES: None
"""

import pandas as pd
import pyreadstat

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    yearly_to_monthly_data,
)

# 1. COMPUTING PARAMETERS USED IN DATA TRANSFORMATIONS

##### a) Getting case-level data on drug offenders sentenced to federal prison under statutes carrying mandatory minimums.
ussc = pd.read_spss(
    "recidiviz/calculator/modeling/population_projection/state/FED/SSA/USSC_data.sav"
)
ussc = ussc[ussc["PRISDUM"] == 1]  # determines if someone is actually sent to prison

drug_mins = ussc.loc[(ussc["DRUGMIN"] > 0)]

##### b) Iterating through NWSTAT (variable description below) columns to isolate cases sentenced under statutes affected by policy (21 USC 841 & 960)
##### NWSTAT1-NWSTATX: Title, Section, and Subsection number of the UNIQUE statutes for each case generated from all of the statute fields. (USSC Datafile Codebook)
def convicted_for(statute):
    prevcol = [False] * len(drug_mins.index)
    for column in drug_mins.loc[:, "NWSTAT1":"NWSTAT19"]:
        newcol = [statute in x for x in drug_mins[column]]
        prevcol = [x or y for x, y in zip(newcol, prevcol)]
    return prevcol


drug_mins = drug_mins.assign(
    convicted_841=convicted_for("21841"),
    convicted_960=[
        y and not x for x, y in zip(convicted_for("21841"), convicted_for("21960"))
    ],
)

has_841 = drug_mins[drug_mins["convicted_841"]]
only_has_960 = drug_mins[drug_mins["convicted_960"]]

##### c) Calculating parameter values
########## i. Number of offenders convicted under section 841 vs 960, by associated minimum sentence
print(
    drug_mins.groupby("DRUGMIN").agg({"convicted_841": "sum", "convicted_960": "sum"})
)
########## ii. Average sentencing for offenders convicted under section 841 (excluding life sentences, which are encoded as 9996)
print(
    has_841[has_841["TOTPRISN"] != 9996]
    .groupby("DRUGMIN")
    .agg({"TOTPRISN": ["mean", "median"]})
)
########## iii. Average sentencing for offenders convicted under section 960 only (excluding life sentences)
print(
    only_has_960[only_has_960["TOTPRISN"] != 9996]
    .groupby("DRUGMIN")
    .agg({"TOTPRISN": ["mean", "median"]})
)
########## iv. Total numbers of prisoners sentenced in datafile
print(len(ussc))
########## v. Average prison sentenced imposed in datafile (excluding life sentences)
print(
    sum(ussc.loc[ussc["TOTPRISN"] != 9996, "TOTPRISN"])
    / len(ussc.loc[ussc["TOTPRISN"] != 9996, "TOTPRISN"])
)

##############################

# 2. PROCESSING POPULATION/OUTFLOW TABLES

##### a) Reading in historical data
yearly_outflows = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/FED/SSA/outflows.csv"
)
yearly_population = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/FED/SSA/population.csv"
)

##### b) Disaggregating historical data using subtype weights
def disaggregate(to_disagg, prop_df, prop_type):
    final_df = pd.DataFrame()
    for disagg_level in prop_df.index:
        disagg_specific_pop = to_disagg.copy()
        disagg_specific_pop["total_population"] *= prop_df.loc[disagg_level, prop_type]
        disagg_specific_pop["crime_type"] = disagg_level
        final_df = pd.concat([final_df, disagg_specific_pop])
    return final_df


# see data sheet for where these numbers come from "disaggregation proportions"
disagg_props = pd.DataFrame(
    {
        "flow_prop": [
            0.4073426661,
            0.5430037129,
            0.01641834224,
            0.005305161855,
            0.02777408265,
            0.0001560341722,
        ],
        "pop_prop": [
            0.3279540527,
            0.6229037744,
            0.035076561,
            0.001778613344,
            0.01201243198,
            0.0002745665708,
        ],
    },
    index=[
        "841_5yr",
        "841_10yr",
        "841_15yr",
        "courier_960_5yr",
        "courier_960_10yr",
        "courier_960_15yr",
    ],
)

##### c) Final tables, ready for upload
outflows = disaggregate(
    yearly_to_monthly_data(yearly_outflows, True), disagg_props, "flow_prop"
)
population = disaggregate(
    yearly_to_monthly_data(yearly_population, False), disagg_props, "pop_prop"
)

##############################

# 3. COMPILING TRANSITION TABLE

##### a) Reading in estimated release -> prison transition data
release_transitions = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/FED/SSA/transitions.csv"
)

##### b) Isolating cases with affected minimums (5, 10, and 15 years), getting case-level transitions, grouping cases with identical LOS
AVG_LOS_PROP = 0.88

transitions_841 = has_841.loc[
    (
        (has_841["DRUGMIN"] == 60)
        | (has_841["DRUGMIN"] == 120)
        | (has_841["DRUGMIN"] == 180)
    )
    & (has_841["TOTPRISN"] != 9996),
    ["DRUGMIN", "TOTPRISN"],
]
transitions_841 = transitions_841.rename(
    columns={"DRUGMIN": "crime_type", "TOTPRISN": "compartment_duration"}
)

transitions_841["crime_type"] = transitions_841.crime_type.map(
    {60: "841_5yr", 120: "841_10yr", 180: "841_15yr"}
)
transitions_841["compartment_duration"] *= AVG_LOS_PROP

transitions_841 = (
    transitions_841.groupby(["crime_type", "compartment_duration"])
    .size()
    .reset_index(name="total_population")
)

transitions_960 = only_has_960.loc[
    (
        (only_has_960["DRUGMIN"] == 60)
        | (only_has_960["DRUGMIN"] == 120)
        | (only_has_960["DRUGMIN"] == 180)
    )
    & (only_has_960["TOTPRISN"] != 9996),
    ["DRUGMIN", "TOTPRISN"],
]
transitions_960 = transitions_960.rename(
    columns={"DRUGMIN": "crime_type", "TOTPRISN": "compartment_duration"}
)

transitions_960["crime_type"] = transitions_960.crime_type.map(
    {60: "courier_960_5yr", 120: "courier_960_10yr", 180: "courier_960_15yr"}
)
transitions_960["compartment_duration"] *= AVG_LOS_PROP

transitions_960 = (
    transitions_960.groupby(["crime_type", "compartment_duration"])
    .size()
    .reset_index(name="total_population")
)

prison_transitions = pd.concat([transitions_841, transitions_960])
prison_transitions.insert(0, "compartment", "prison")
prison_transitions.insert(1, "outflow_to", "release")

##### c) Final transition table, ready for upload
transitions = pd.concat([prison_transitions, release_transitions])

##############################

# 4. OUTPUT

##### a) Uploading tables
upload_spark_model_inputs(
    "recidiviz-staging",
    "fed_ssa",
    outflows,
    transitions,
    population,
    "recidiviz/calculator/modeling/population_projection/state/FED/SSA/Fed_SSA_model_inputs.yaml",
)

##### b) Saving tables (commented out, uncomment to save the processed tables as CSVs)
# outflows.to_csv('~/Documents/spark/pulse-data/recidiviz/calculator/modeling/population_projection/state/Federal/SSA/final_outflows.csv', index = False)
# population.to_csv('~/Documents/spark/pulse-data/recidiviz/calculator/modeling/population_projection/state/Federal/SSA/final_population.csv', index = False)
# transitions.to_csv('~/Documents/spark/pulse-data/recidiviz/calculator/modeling/population_projection/state/Federal/SSA/final_transitions.csv', index = False)
