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
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

# pylint: skip-file


# RAW DATA

mandatory_minimums = {
    "NAR3085": 5,
    "NAR3119": 20,
    "NAR3124": 40,
    "NAR3114": 20,
    "NAR3067": 3,
    "NAR3086": 10,
    "NAR3126": 2,
    "NAR3127": 5,
    "NAR3128": 5,
    "NAR3063": 2,
    "NAR3098": 5,
    "NAR3097": 5,
    "NAR3090": 20,
    "NAR3092": 40,
    "NAR3118": 20,
    "NAR3123": 40,
    "NAR3117": 20,
    "NAR3122": 40,
    "NAR3116": 20,
    "NAR3121": 40,
    "NAR3120": 20,
    "NAR3125": 40,
    "NAR3091": 20,
    "NAR3093": 40,
    "NAR3038": 3,
    "NAR3087": 10,
    "NAR3146": 5,
    "NAR3113": 20,
    "NAR3145": 5,
    "NAR3112": 20,
    "NAR3144": 5,
    "NAR3111": 40,
    "NAR3147": 5,
    "NAR3149": 3,
    "NAR3115": 20,
    "NAR3151": 5,
    "NAR3133": 3,
    "NAR3065": 3,
    "NAR3088": 10,
    "NAR3066": 3,
    "NAR3089": 10,
    "NAR3099": 1,
    "NAR3041": 1,
    "NAR3094": 20,
    "WPN5296": 5,
    "WPN5297": 2,
}

# you may have to change this path to point to wherever you have the file on your computer
historical_sentences = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/VA/VA_data/processed_va_historical_sentences_v2.csv"
)

# Filter to the supported sentence types
supported_sentence_types = ["jail", "prison"]
historical_sentences = historical_sentences[
    historical_sentences["sentence_type"].isin(supported_sentence_types)
]

# Remove sentences with 0 month sentence lengths
historical_sentences = historical_sentences[
    historical_sentences["effective_sentence_months"] > 0
].copy()


# TRANSITIONS TABLE
jail_prison_sentences = (
    historical_sentences[
        ["offense_group", "offense_code", "sentence_type", "effective_sentence_years"]
    ]
    .groupby(
        ["offense_code", "sentence_type", "effective_sentence_years"], as_index=False
    )
    .count()
)

# rename column names to match data schema
jail_prison_sentences = jail_prison_sentences.rename(
    {
        "offense_group": "total_population",
        "effective_sentence_years": "compartment_duration",
        "sentence_type": "inflow_to",
    },
    axis=1,
)
jail_prison_sentences = jail_prison_sentences.rename(
    {"inflow_to": "compartment", "offense_code": "crime"}, axis=1
)

# add a column for 'outflow_to' which is always 'release' because all data is prison sentences
jail_prison_sentences["outflow_to"] = "release"

# for each sub-simulation, add in trivial transitions data to define release behavior
for offense_code in jail_prison_sentences["crime"].unique():
    jail_prison_sentences = jail_prison_sentences.append(
        {
            "crime": offense_code,
            "compartment": "release",
            "compartment_duration": 30,
            "total_population": 1,
            "outflow_to": "release",
        },
        ignore_index=True,
    )
transitions_data = jail_prison_sentences
transitions_data.total_population = transitions_data.total_population.astype(float)

# OUTFLOWS TABLE
jail_prison_admissions = (
    historical_sentences[
        [
            "offense_group",
            "off1_vcc",
            "offense_code",
            "time_step",
            "sentence_type",
            "compartment",
        ]
    ]
    .groupby(
        ["offense_group", "offense_code", "compartment", "sentence_type", "time_step"],
        as_index=False,
    )
    .count()
)

# rename column names to match data schema
jail_prison_admissions = jail_prison_admissions.rename(
    {
        "off1_vcc": "total_population",
        "offense_code": "crime",
        "sentence_type": "outflow_to",
    },
    axis=1,
)
outflows_data = jail_prison_admissions.drop("offense_group", axis=1)
outflows_data.time_step = outflows_data.time_step.astype(int)
outflows_data.total_population = outflows_data.time_step.astype(float)

# this is left over from the last policy we modeled, you'll want to filter differently based on what you're modeling
affected_crimes = [
    "ASL1342",
    "NAR3038",
    "NAR3087",
    "DWI5406",
    "DWI5449",
    "DWI5450",
    "LIC6834",
    "LIC6860",
    "WPN5296",
    "WPN5297",
]

transitions_data = transitions_data[transitions_data.crime.isin(affected_crimes)]
outflows_data = outflows_data[outflows_data.crime.isin(affected_crimes)]

# Don't want sentences listed as hundreds of years to skew our model, so we cap sentence length at 50 years
sentence_cap_data = pd.Series(
    [50 for i in transitions_data.compartment_duration], index=transitions_data.index
)
transitions_data.loc[
    transitions_data.compartment_duration > sentence_cap_data, "compartment_duration"
] = 50

# STORE DATA
upload_spark_model_inputs(
    "recidiviz-staging",
    "VA_prison",
    outflows_data,
    transitions_data,
    pd.DataFrame(),
    "recidiviz/calculator/modeling/population_projection/state/VA/VA_prison_model_inputs.yaml",
)
