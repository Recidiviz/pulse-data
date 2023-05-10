# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
POLICY: HB 5977 Decriminalizing marijuana
VERSION: V1
DATA SOURCE: USSC https://github.com/khwilson/SentencingCommissionDatasets
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA: Total population per month
REFERENCE_DATE: September 2020
TIME_STEP: Month
ADDITIONAL NOTES: Initial policy scoping doc https://docs.google.com/document/d/1mj6Fmm3aCmx08PqhNShV6Rb8MCRuHQ2D56BmxeJKxKg/edit?usp=sharing
"""

import logging

import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.super_simulation.time_converter import (
    TimeConverter,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    convert_dates,
)
from recidiviz.utils.yaml_dict import YAMLDict

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)

# BJS 2016 Federal Justice Statistics, Table 8.3 (https://bjs.ojp.gov/content/pub/pdf/fjs16st.pdf)
AVG_PCT_SERVED = 0.883

FED_DIRECTORY_PATH = "recidiviz/calculator/modeling/population_projection/state/FED/"
SENTENCING_DATA_PATH = FED_DIRECTORY_PATH + "sentencing_data/"

# Load the USSC sentence data and standardize the values between the old and new
# schema versions before concatenating them

# Data dictionary:
# https://www.ussc.gov/sites/default/files/pdf/research-and-publications/datafiles/USSC_Public_Release_Codebook_FY99_FY20.pdf
common_columns = [
    "TOTPRISN",
    "SENTMON",
    "SENTYR",
    "COMBDRG2",
    "SENTIMP",
    "NWSTAT1",
    "NWSTAT2",
    "NWSTAT3",
    "NWSTAT4",
    "NWSTAT5",
]

new_data_columns = common_columns + [
    "OFFGUIDE",
]
old_data_columns = common_columns + [
    "OFFTYPSB",
]

# Supply some of the column types to silence warnings & process the large CSVs faster
column_dtypes = {
    "NWSTAT1": str,
    "NWSTAT2": str,
    "NWSTAT3": str,
    "NWSTAT4": str,
    "NWSTAT5": str,
}

new_data_files = ["opafy20nid.csv", "opafy19nid.csv", "opafy18nid.csv"]
old_data_files = [
    "opafy17nid.csv",
    "opafy16nid.csv",
    "opafy15nid.csv",
    "opafy14nid.csv",
    "opafy13nid.csv",
]


def get_eligible_sentences(df: pd.DataFrame) -> pd.DataFrame:
    """Return only the sentences eligible for this policy:
    * non-life sentences ("TOTPRISN" != 9996)
    * with a marijuana classification ("COMBDRG2" == 4)
    * sentenced to prison ("SENTIMP" is in [1, 2])
    """
    return df[
        (df["COMBDRG2"] == 4) & (df["TOTPRISN"] != 9996) & (df["SENTIMP"].isin([1, 2]))
    ]


new_data = pd.DataFrame()
for file in new_data_files:
    temp = pd.read_csv(
        SENTENCING_DATA_PATH + file, usecols=new_data_columns, dtype=column_dtypes
    )
    temp = get_eligible_sentences(temp)
    new_data = pd.concat([new_data, temp])

old_data = pd.DataFrame()
for file in old_data_files:
    temp = pd.read_csv(
        SENTENCING_DATA_PATH + file, usecols=old_data_columns, dtype=column_dtypes
    )
    temp = get_eligible_sentences(temp)
    old_data = pd.concat([old_data, temp])

offguide_map = {
    1: "Administration of Justice",
    4: "Assault",
    9: "Drug Possession",
    10: "Drug Trafficking",
    12: "Extortion/Racketeering",
    13: "Firearms",
    17: "Immigration",
    21: "Money Laundering",
    22: "Murder",
    26: "Robbery",
    30: "Other",
}
new_data["primary_crime_type"] = new_data["OFFGUIDE"].map(offguide_map).fillna("Other")

offtype_map = {
    10: "Drug Trafficking",
    11: "Drug Communication Facilities",
    12: "Drug Possession",
    13: "Firearms",
    23: "Money Laundering",
    24: "Extortion/Racketeering",
    27: "Immigration",
    30: "Administration of Justice",
}
old_data["primary_crime_type"] = old_data["OFFTYPSB"].map(offtype_map).fillna("Other")

# Concatenate the two datasets together and rename columns
concat_data_columns = [
    "TOTPRISN",
    "primary_crime_type",
    "SENTMON",
    "SENTYR",
    "NWSTAT1",
    "NWSTAT2",
    "NWSTAT3",
    "NWSTAT4",
    "NWSTAT5",
]
sentence_data = pd.concat(
    [new_data[concat_data_columns], old_data[concat_data_columns]]
).reset_index(drop=True)
sentence_data = sentence_data.rename(
    {"TOTPRISN": "total_sentence_months", "SENTMON": "month", "SENTYR": "year"},
    axis=1,
)
# Create a column `sentence_start_month` from the sentencing year & month data
sentence_data["day"] = 1
sentence_data["sentence_start_month"] = pd.to_datetime(
    sentence_data[["year", "month", "day"]]
).dt.date

# Drop extra columns
sentence_data = sentence_data.drop(["day", "month", "year"], axis=1)

# Add a placeholder column to use for groupby operations below
sentence_data["person_count"] = 1

# Drop life sentences since they do not qualify for re-sentencing
sentence_data = sentence_data[sentence_data["total_sentence_months"] != 9996]

logger.info("Processed %d sentence records", len(sentence_data))

# Estimate the full-term release date for each sentence
sentence_data["estimated_release_date"] = (
    sentence_data["sentence_start_month"]
    + np.floor(sentence_data["total_sentence_months"] * AVG_PCT_SERVED).apply(
        pd.offsets.MonthEnd
    )
).dt.date

# Use the policy classification list that maps statutes to the percent that the
# sentence will be updated (100% indicates full decriminalization, 0% indicates no update)
statute_class = pd.read_csv(
    FED_DIRECTORY_PATH + "states_reform_act/statute_classification.csv",
    usecols=["statute", "sentence_update_percent"],
)
statute_class["statute_count"] = 0

# Track this for debugging, make sure all common statutes are captured in this
uncounted_statutes = pd.DataFrame()

# Determine the policy impact for each sentence based on the listed statutes
sentence_data["sentence_update_percent"] = 0
for index, row in sentence_data.iterrows():
    # Start with the value for the biggest impact (100% sentence reduction)
    total_sentence_update_percent = 100
    for statute_index in range(1, 6):
        statute = str(row[f"NWSTAT{statute_index}"])
        # Skip null statutes
        if (statute is None) | (statute == "nan"):
            continue

        # Get all statute classifications that cover the single statute
        # Ex: statute "18922G3A1" will match classifications for "18922" and "18922G3"
        mask = [
            (statute_string in statute)
            for statute_string in statute_class["statute"].values
        ]
        applicable_statutes = statute_class[mask]
        if len(applicable_statutes) == 0:
            missing_statute = pd.DataFrame(
                {
                    "statute": [statute],
                    "primary_crime_type": [row["primary_crime_type"]],
                    "total_sentence_months": [row["total_sentence_months"]],
                    "sentence_start_month": [row["sentence_start_month"]],
                }
            )
            uncounted_statutes = pd.concat([uncounted_statutes, missing_statute])
            continue

        # Pick the classification with the best match
        # Ex: statute "18922G3A1" will best match classification "18922G3"

        # Comment out the fuzzywuzzy import to avoid mypy errors
        # from fuzzywuzzy import fuzz
        # best_match_index = np.argmax(
        #     [
        #         fuzz.ratio(statute, matched_statute)
        #         for matched_statute in applicable_statutes["statute"]
        #     ]
        # )
        raise ValueError(
            "Remove the comments on the code block above before rerunning this"
        )
        best_match_index = 0
        sentence_update_percent = applicable_statutes.iloc[best_match_index][
            "sentence_update_percent"
        ]
        sentence_data.loc[
            index, f"sentence_{statute_index}_impact"
        ] = sentence_update_percent
        statute_class.loc[
            applicable_statutes.index[best_match_index], "statute_count"
        ] += 1

        # Keep the value for the most severe statute with the smallest impact percent
        # Ex. a sentence with a 20% reduction and a 5% reduction statute should get a 5% reduction
        if sentence_update_percent < total_sentence_update_percent:
            total_sentence_update_percent = sentence_update_percent

    sentence_data.loc[index, "sentence_update_percent"] = total_sentence_update_percent

# Save this for debugging
uncounted_statutes.to_csv(
    FED_DIRECTORY_PATH + "states_reform_act/unclassified_statutes.csv"
)

# Only keep sentences that are eligible for re-sentencing under this policy
eligible_sentences = sentence_data[sentence_data["sentence_update_percent"] > 0]

# Get the simulation tag from the model inputs config
yaml_file_path = (
    FED_DIRECTORY_PATH + "states_reform_act/fed_states_reform_act_model_inputs.yaml"
)
simulation_config = YAMLDict.from_path(yaml_file_path)
data_inputs = simulation_config.pop_dict("data_inputs")
simulation_tag = data_inputs.pop("big_query_simulation_tag", str)

# Convert the timestamps to time_steps (relative ints), with 0 being the most recent
# date of data (Sept. 2020)
reference_date = simulation_config.pop("reference_date", float)
time_step = simulation_config.pop("time_step", float)
time_converter = TimeConverter(reference_year=reference_date, time_step=time_step)

eligible_sentences.loc[:, "time_step"] = convert_dates(
    time_converter, eligible_sentences["sentence_start_month"]
)
eligible_sentences.loc[:, "time_step_end"] = convert_dates(
    time_converter, eligible_sentences["estimated_release_date"]
)

# Create the outflows table using sentence start date as a proxy for prison admissions
outflows = (
    eligible_sentences.groupby(["time_step", "sentence_update_percent"])
    .count()["person_count"]
    .reset_index()
    .sort_values(by=["time_step", "sentence_update_percent"])
)
outflows.rename(
    columns={
        "person_count": "total_population",
        "sentence_update_percent": "crime_type",
    },
    inplace=True,
)
outflows["total_population"] = outflows["total_population"].astype(float)
outflows["crime_type"] = outflows["crime_type"].astype(str)

outflows["compartment"] = "pretrial"
outflows["outflow_to"] = "prison"

# Calculate the monthly population by counting the total people estimated to be in
# prison each month using `sentence_start_month` and `estimated_release_date`.
# Start in Jan 2015 so that there are enough older sentences to get a relatively accurate count
total_population_start_ts = -68
total_population = pd.DataFrame()
for time_step in range(total_population_start_ts, 1):
    active_population = eligible_sentences[
        (eligible_sentences["time_step"] <= time_step)
        & (time_step < eligible_sentences["time_step_end"])
    ]
    active_population = (
        active_population.groupby("sentence_update_percent")
        .count()["person_count"]
        .reset_index()
    )
    active_population.rename(
        columns={
            "person_count": "total_population",
            "sentence_update_percent": "crime_type",
        },
        inplace=True,
    )
    active_population["time_step"] = time_step
    total_population = pd.concat([total_population, active_population])

total_population["compartment"] = "prison"
total_population["total_population"] = total_population["total_population"].astype(
    float
)
total_population["crime_type"] = total_population["crime_type"].astype(str)

# Calculate the transitions for the baseline case
eligible_sentences.loc[:, "estimated_los"] = np.clip(
    eligible_sentences["total_sentence_months"] * AVG_PCT_SERVED, a_min=None, a_max=120
)
baseline_transitions = (
    eligible_sentences.groupby(["estimated_los", "sentence_update_percent"])
    .count()["time_step"]
    .reset_index()
)
baseline_transitions.rename(
    columns={
        "estimated_los": "compartment_duration",
        "sentence_update_percent": "crime_type",
        "time_step": "total_population",
    },
    inplace=True,
)
baseline_transitions["compartment_duration"] = baseline_transitions[
    "compartment_duration"
].astype(float)
baseline_transitions["compartment"] = "prison"
baseline_transitions["outflow_to"] = "release"

manual_transitions = [
    {
        "compartment": "release",
        "outflow_to": "release",
        "total_population": 1,
        "compartment_duration": 1,
        "crime_type": str(sentence_update_percent),
    }
    for sentence_update_percent in eligible_sentences[
        "sentence_update_percent"
    ].unique()
]

baseline_transitions = baseline_transitions.append(pd.DataFrame(manual_transitions))
baseline_transitions["total_population"] = baseline_transitions[
    "total_population"
].astype(float)

# Store the input tables in BigQuery
upload_spark_model_inputs(
    project_id="recidiviz-staging",
    simulation_tag=simulation_tag,
    outflows_data_df=outflows,
    transitions_data_df=baseline_transitions,
    total_population_data_df=total_population,
    yaml_path=yaml_file_path,
)
