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
import time
from datetime import datetime

import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

pd.set_option("display.max_columns", 500)
pd.set_option("display.max_rows", None)


def estimate_sentence_start_date(sentence_group: pd.DataFrame) -> None:
    """Add a new column to the df for `estimatedStartDate`

    Use the expiration date and max sentence length to estimate when an individual
    sentence actually began. For life sentences, use the date received (since there is no
    max sentence length).

    The scraped data does not include time spent in county jail, which counts towards
    the total sentence and length of stay.
    """

    max_sentence_days = np.floor(sentence_group["maxSentence"].astype(float) * 365.25)

    # Attempt to get the max expiration date, but coerce non-date values like "LIFE"
    # to a value far in the future
    max_expiration_date = pd.to_datetime(
        sentence_group["maxExpirationDate"], errors="coerce"
    ).fillna(datetime(2222, 2, 22))

    estimated_start_date = max_expiration_date - pd.to_timedelta(
        max_sentence_days, unit="d"
    )

    # Use the current date received if the start date inference did not work
    # (this could better infer starts for life sentences)
    sentence_group["estimatedStartDate"] = np.minimum(
        estimated_start_date, sentence_group["dateReceivedCurrent"]
    )
    return


mms = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/NY/mms/crime_to_mm.csv"
)
mms = mms.set_index("crime")

date_cols = [
    "DOB",
    "dateReceivedOriginal",
    "dateReceivedCurrent",
    "latestReleaseDate",
    "paroleHearingDate",
    "maxExpirationDateParole",
    "postReleaseMaxExpiration",
    "paroleBoardDischargeDate",
    "paroleEligibilityDate",
]
transition_table_cols = [
    "custodyStatus",
    "dateReceivedOriginal",
    "dateReceivedCurrent",
    "latestReleaseDate",
    "latestReleaseType",
    "earliestReleaseDate",
    "conditionalReleaseDate",
    "maxExpirationDate",
    "minSentence",
    "maxSentence",
    "crime1",
    "class1",
    "crime2",
    "class2",
    "crime3",
    "class3",
    "crime4",
    "class4",
    "paroleEligibilityDate",
    "race",
]
THIRTY_YRS = 360
SAVE_TO_CSV = False

dfs = {
    year: pd.read_csv(
        "recidiviz/calculator/modeling/population_projection/state/NY/sentencing_data/inmates"
        + str(year)
        + ".csv",
        index_col=0,
        parse_dates=date_cols,
        na_filter=False,
        dtype={"minSentence": str, "maxSentence": str},
    )
    for year in range(2000, 2021)
}
df_full = pd.concat(dfs.values())

################### SETUP
df = df_full.copy()
print(len(df))

# # ignore releases from over 10 years ago
df = df[df["latestReleaseDate"].fillna(datetime(9999, 1, 1)) >= datetime(2011, 1, 1)]
print(len(df))

# ignore no crime records
df = df[df.crime1 != "NO CRIME RECORD AVAIL"]
print(len(df))

# Drop parole violators because the latest sentence data is for the violation
df = df[df["paroleHearingType"] != "PAROLE VIOLATOR ASSESSED EXPIRATION"]
print(len(df))

# trim to essential columns
df = df[transition_table_cols]

# Combine less frequent race groups into an "OTHER" category
other_race_rows = ~df.race.isin(["WHITE", "BLACK", "HISPANIC"])
df.loc[other_race_rows, "race"] = "OTHER"

# concatenate the crime and offense class
ny_sentence_data = df.copy()
ny_sentence_data["crime_type"] = ny_sentence_data.crime1 + "|" + ny_sentence_data.class1

# join to the mandatory minimum data
ny_sentence_data = ny_sentence_data.merge(mms, left_on="crime_type", right_index=True)

# infer the start date to account for time spent in jail that applied towards the max sentence
estimate_sentence_start_date(ny_sentence_data)

# Compute the LOS for completed sentences

# Only include people who have been released or discharged
completed_sentences = ny_sentence_data[
    ny_sentence_data["custodyStatus"].isin(["RELEASED", "DISCHARGED"])
].copy()
print(len(completed_sentences))

# Drop deaths so that the shorter length of stay does not bias the distribution
completed_sentences = completed_sentences[
    completed_sentences["latestReleaseType"] != "DECEASED"
]
print(len(completed_sentences))

# drop releases impacted by Covid
completed_sentences = completed_sentences[
    completed_sentences["latestReleaseDate"] < datetime(2020, 1, 1)
]
print(len(completed_sentences))

# drop rows where the release is before the estimated start date
completed_sentences = completed_sentences[
    completed_sentences["estimatedStartDate"] < completed_sentences["latestReleaseDate"]
]
print(len(completed_sentences))
completed_sentences["daysServed"] = (
    completed_sentences["latestReleaseDate"] - completed_sentences["estimatedStartDate"]
).dt.days

# clean up df and prep merge to `mms`
completed_sentences = completed_sentences[
    completed_sentences.crime_type.isin(mms.index.unique())
]
print(len(completed_sentences))

completed_sentences["LOS"] = np.clip(
    np.ceil(completed_sentences.daysServed * 12 / 365.25), a_min=None, a_max=THIRTY_YRS
)

mms["weight"] = 0
for crime in mms.index.unique():
    offenses = completed_sentences[completed_sentences.crime_type == crime]
    mms.loc[crime, "weight"] = len(offenses)
    if offenses.empty:
        continue

    average_duration = np.average(offenses.daysServed / 30)
    variance = np.average(
        ((offenses.daysServed / 30) - average_duration) ** 2,
    )
    std = np.sqrt(variance)
    print(crime, average_duration, std, len(offenses))

    mms.loc[crime, "std"] = std

# Drop all data for offense types that are not impacted by the policy
# (Policy is only for violent MMs)
completed_sentences = completed_sentences[
    completed_sentences["sentence_type"] == "determinate-violent"
]


if SAVE_TO_CSV:
    mms.to_csv(
        "recidiviz/calculator/modeling/population_projection/state/NY/mms/crime_to_mm.csv",
        index=True,
    )

pop_valid = completed_sentences.copy()
completed_sentences = completed_sentences[
    [
        "estimatedStartDate",
        "latestReleaseDate",
        "daysServed",
        "crime_type",
        "race",
        "LOS",
    ]
]

##################### OUTFLOWS
# Count the number of people that start a new sentence each month disaggregated by
# crime type and race. Clip outfows at Jan, 1, 2020 when the simulation starts
outflows = ny_sentence_data[
    (ny_sentence_data["estimatedStartDate"] < datetime(2020, 1, 1))
    & (ny_sentence_data["sentence_type"] == "determinate-violent")
].copy()
outflows = outflows[["estimatedStartDate", "crime_type", "race"]]
outflows["total_population"] = outflows["estimatedStartDate"].apply(
    lambda x: "%d/%d" % (x.month, x.year)
)
outflows["time_step"] = outflows["estimatedStartDate"].apply(
    lambda x: x.month + 12 * (x.year - 2000)
)

outflows = (
    outflows.groupby(["time_step", "crime_type", "race"])["total_population"]
    .count()
    .reset_index()
)
outflows["compartment"] = "pre-trial"
outflows["outflow_to"] = "prison"

# fill in missing outflows data with zeroes
ts_min = outflows["time_step"].min()
ts_max = outflows["time_step"].max()
for (crime_type, race), group_outflows in outflows.groupby(["crime_type", "race"]):
    missing_ts = [
        ts
        for ts in range(ts_min, ts_max + 1)
        if ts not in group_outflows["time_step"].values
    ]
    missing_outflows = pd.DataFrame(
        {
            "time_step": missing_ts,
            "crime_type": crime_type,
            "race": race,
            "compartment": "pre-trial",
            "outflow_to": "prison",
            "total_population": 0,
        }
    )
    outflows = outflows.append(missing_outflows)

outflows = outflows.reset_index(drop=True)

outflows = outflows[(outflows.time_step >= 120)]

######################## TRANISITIONS
# cap LOS at 30yr
d = completed_sentences.copy()
d["LOS"] = np.clip(d["LOS"], a_min=None, a_max=THIRTY_YRS)

transition_df_by_crime = []

for (crime_type, race), by_group in d.groupby(["crime_type", "race"]):

    # combine inmates with the same LOS
    LOS_count = by_group.groupby("LOS").count()

    # calculate proportion of inmates released, of those remaining
    LOS_count["total_population"] = LOS_count.crime_type / LOS_count.crime_type.sum()

    # add crime for disaggregation and save df to list
    LOS_count["crime_type"] = crime_type
    LOS_count["race"] = race
    transition_df_by_crime.append(LOS_count[["total_population", "crime_type", "race"]])

transitions = pd.concat(transition_df_by_crime)

transitions["compartment"] = "prison"
transitions["outflow_to"] = "release"
transitions["compartment_duration"] = transitions.index
transitions = transitions[
    [
        "compartment",
        "outflow_to",
        "total_population",
        "compartment_duration",
        "crime_type",
        "race",
    ]
]
transitions = transitions.reset_index(drop=True)

# taken from here: https://doccs.ny.gov/system/files/documents/2021/03/inmate-releases-three-year-out-post-release-follow-up-2014.pdf
recidivism_transitions = pd.DataFrame(
    {
        "compartment": ["release"] * 4 + ["release_full"],
        "outflow_to": ["prison"] * 3 + ["release_full"] * 2,
        "compartment_duration": [12, 24, 36, 36, 36],
        "total_population": [0.43 * 0.5, 0.43 * 0.34, 0.43 * (1 - 0.83), 1 - 0.43, 1],
    }
)

for (crime, race), group in d.groupby(["crime_type", "race"]):
    recidivism_transitions["crime_type"] = crime
    recidivism_transitions["race"] = race
    transitions = pd.concat([transitions, recidivism_transitions])

# shrink outflows so we don't overcount recidivism
outflows.total_population /= 1 + 0.43

if SAVE_TO_CSV:
    transitions.to_csv(
        "/Users/jpouls/recidiviz/nyrecidiviz/mm_preprocessing/transitionfull/transitionfull"
        + str(int(time.time()))
        + ".csv"
    )


########### TOTAL POPULATION
# Take the rows that were active on the reference date to get the total population data
recent_sentences = ny_sentence_data[
    (ny_sentence_data["custodyStatus"].isin(["RELEASED", "DISCHARGED", "IN CUSTODY"]))
    & (
        ny_sentence_data["latestReleaseDate"].fillna(datetime(9999, 1, 1))
        >= datetime(2020, 1, 1)
    )
    & (ny_sentence_data["sentence_type"] == "determinate-violent")
]
pop = recent_sentences.copy()
pop = pop[["latestReleaseDate", "crime_type", "race"]]

print(f"Estimated total population in custody Jan 1, 2020 = {len(pop)}")

total_pop = pop.groupby(["crime_type", "race"]).count()
total_pop.reset_index(inplace=True)

total_pop["compartment"] = "prison"
total_pop["total_population"] = total_pop.latestReleaseDate.apply(float)
# population as of Feb 2021 == 254 months since 2000
total_pop["time_step"] = 240
pop_out = total_pop[
    ["compartment", "total_population", "time_step", "crime_type", "race"]
]

# if SAVE_TO_CSV:
#     pop_out.to_csv(
#         "/Users/jpouls/recidiviz/nyrecidiviz/mm_preprocessing/total_population/total_population"
#         + str(int(time.time()))
#         + ".csv"
#     )

############ SPARK MODEL UPLOAD
upload_spark_model_inputs(
    "recidiviz-staging",
    "NY_mms",
    outflows,
    transitions,
    pop_out,
    "recidiviz/calculator/modeling/population_projection/state/NY/mms/ny_state_prison_model_inputs.yaml",
)
