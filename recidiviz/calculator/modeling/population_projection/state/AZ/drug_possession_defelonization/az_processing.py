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
"""Preprocess the AZ case-level data for the drug possession defelonization policy"""
from datetime import date

import numpy as np
import pandas as pd

# Change to the path needed on your machine
path = ""
yaml_file = "AZ_drug_possession_defelonization.yaml"

# Available at
# https://drive.google.com/drive/folders/1Gvhn3BePYq9BBgVWTCDdxmS-CLng0bUs
mcao = pd.read_csv(
    path + "mcao.csv",
    usecols=[
        # Days sentenced to probation
        "Prob Days",
        # Months sentenced to probation
        "Prob Months",
        # Years sentenced to probation
        "Prob Years",
        # Days sentenced to prison
        "DOC Days",
        # Months sentenced to prison
        "DOC Months",
        # Years sentenced to prison
        "DOC Years",
        # Days sentenced to jail
        "Jai lDays",
        # Months sentenced to jail
        "Jail Months",
        # Years sentenced to jail
        "Jail Years",
        # Type of sentence (probation, DOC, fines, NA)
        "Sentence Type",
        # Felony Statute for the offense
        "Disposition Statute",
        # Disposition type (F for felony, M for misdemeanor) and class (1-6)
        "Dispo Type/Class",
        # Date sentenced
        "Sentence Date",
        # Proxy columns for person id
        "Main DR Number",
        "Last Name",
        "First Name",
        "Middle Name",
    ],
    parse_dates=["Sentence Date"],
    low_memory=False,
)
# Create a person-level identifier to help with de-duplication
mcao["person_id"] = (
    mcao["Main DR Number"]
    + mcao["Last Name"]
    + mcao["First Name"]
    + mcao["Middle Name"].fillna("")
)

# Bill text:
# 13-3402: take portion of classification A that was F6 and create new B for possession to be M3 (how common is this statute?)
# 13-3403A1: was F5 (but could be M1 w/ discretion) but becomes M3
# 13-3404.01A1: was F2 becomes M3
# 13-3404.01A2: was F5 becomes M3
# 13-3404.01A4: was F5 becomes M3
# 13-3405A1: depends on the weight, < 4lbs was F6 becomes M3, >= 4lbs was F4 becomes M2 (can we determine by previous class/type?)
# 13-3407A1: was F4 becomes M3
# 13-3408A1: was F4 becomes M3
# 13-3411A2: was variable, becomes M1
# 13-3415A: was F6 becomes M3
# 13-3415B: was F6 becomes M3
# 13-3415C: was F6 becomes M3

# Mapping of disposition statute -> new disposition type/class from the bill text
statute_policy_update = {
    "13-3402": "M3",
    "13-3403A1": "M3",
    "13-3404.01A1": "M3",
    "13-3404.01A2": "M3",
    "13-3404.01A4": "M3",
    "13-3405A1": "M3",
    "13-3407A1": "M3",
    "13-3408A1": "M3",
    "13-3411A2": "M1",
    "13-3415A": "M3",
    "13-3415B": "M3",
    "13-3415C": "M3",
}
eligible_statutes = "|".join(statute_policy_update.keys())

mcao = mcao.fillna(
    value={
        "Prob Days": 0,
        "Prob Months": 0,
        "Prob Years": 0,
        "DOC Days": 0,
        "DOC Months": 0,
        "DOC Years": 0,
        "Jai lDays": 0,
        "Jail Months": 0,
        "Jail Years": 0,
    }
)
mcao["probation_total_days"] = np.floor(
    mcao["Prob Days"] + (30.437 * mcao["Prob Months"]) + (365 * mcao["Prob Years"])
)
mcao["doc_total_days"] = np.floor(
    mcao["DOC Days"] + (30.437 * mcao["DOC Months"]) + (365 * mcao["DOC Years"])
)
mcao["jail_total_days"] = np.floor(
    mcao["Jai lDays"] + (30.437 * mcao["Jail Months"]) + (365 * mcao["Jail Years"])
)

mcao["sentence_year"] = mcao["Sentence Date"].dt.year

# Drop the disaggregated sentence length columns and person identifiers (prefer person_id)
mcao = mcao.drop(
    columns=[
        "Prob Days",
        "Prob Months",
        "Prob Years",
        "DOC Days",
        "DOC Months",
        "DOC Years",
        "Jai lDays",
        "Jail Months",
        "Jail Years",
        "Main DR Number",
        "Last Name",
        "First Name",
        "Middle Name",
    ]
)

# Drop Fines, "N", and Null sentence types
convicted_sentences = mcao[mcao["Sentence Type"].isin(["DOC", "Jail", "Probation"])]

# Select the subset of drug possession related statutes
eligible_person_ids = convicted_sentences[
    (
        convicted_sentences["Disposition Statute"]
        .astype(str)
        .str.contains(eligible_statutes, regex=True)
    )
]["person_id"].unique()

eligible_people = convicted_sentences[
    convicted_sentences["person_id"].isin(eligible_person_ids)
].copy()
eligible_people["policy_dispo_type_class"] = eligible_people["Disposition Statute"].map(
    statute_policy_update
)

doc_cases = eligible_people[eligible_people["Sentence Type"] == "DOC"].copy()
doc_cases["estimated_release_date"] = doc_cases["Sentence Date"] + np.floor(
    doc_cases["doc_total_days"]
).apply(pd.offsets.Day)

# Get the sentence length distribution for all cases with the same type/class
# excluding statutes that are impacted by this policy
policy_transitions = (
    convicted_sentences[
        (convicted_sentences["Dispo Type/Class"].isin(statute_policy_update.values()))
        & (~convicted_sentences["person_id"].isin(eligible_people["person_id"]))
    ]
    .groupby(["person_id", "Dispo Type/Class"], as_index=False)[
        ["probation_total_days", "jail_total_days", "doc_total_days"]
    ]
    .max()
)

probation_prison_transitions = policy_transitions[
    policy_transitions["probation_total_days"] > 0
].copy()
probation_prison_transitions["compartment_duration"] = np.clip(
    np.floor(probation_prison_transitions["jail_total_days"] / 365), a_min=1, a_max=None
)
probation_prison_transitions = (
    probation_prison_transitions.groupby(["compartment_duration", "Dispo Type/Class"])
    .count()[["person_id"]]
    .reset_index()
)
probation_prison_transitions["compartment"] = "probation"
probation_prison_transitions["outflow_to"] = "release"
probation_prison_transitions.rename(
    {"person_id": "total_population", "Dispo Type/Class": "crime_type"},
    axis=1,
    inplace=True,
)

print(probation_prison_transitions)


def aggregate_cases(df: pd.DataFrame) -> bool:
    """
    Determine which people/sentences would not be eligible for this policy because of
    other non-eligible felony charges
    """
    non_eligible_statutes = df[df["policy_dispo_type_class"].isnull()][
        "Dispo Type/Class"
    ].unique()
    if any("F" in statute for statute in non_eligible_statutes):
        return False

    return True


eligible_people_sentences = doc_cases.groupby(
    ["person_id", "Sentence Date"], as_index=False
).apply(aggregate_cases)
doc_transitions_df = doc_cases.merge(
    eligible_people_sentences, how="inner", on=["person_id", "Sentence Date"]
)
doc_transitions_df = (
    doc_transitions_df.groupby(["person_id", "Sentence Date"])[["doc_total_days"]]
    .max()
    .reset_index()
)

doc_transitions_df["compartment_duration"] = np.clip(
    np.floor(doc_transitions_df["doc_total_days"] / 365), a_min=1, a_max=None
)
doc_transitions_df["compartment"] = "prison"
doc_transitions_df["outflow_to"] = "release"
doc_transitions_df["crime_type"] = "x"
doc_transitions_df = doc_transitions_df.groupby(
    ["compartment", "outflow_to", "compartment_duration", "crime_type"],
    as_index=False,
)["person_id"].count()
doc_transitions_df.rename({"person_id": "total_population"}, axis=1, inplace=True)
doc_transitions_df[
    [
        "compartment",
        "outflow_to",
        "total_population",
        "compartment_duration",
        "crime_type",
    ]
].to_csv("prison_baseline_transitions.csv", index=False)


# Get the sentence length distribution for the new disposition type/class
non_eligible_convictions = eligible_people[
    ~(
        eligible_people["Disposition Statute"]
        .astype(str)
        .str.contains(eligible_statutes, regex=True)
    )
]

# Select the subset of drug possession related statutes
doc_prob_only = convicted_sentences[
    convicted_sentences["Disposition Statute"]
    .astype(str)
    .str.contains(eligible_statutes, regex=True)
].copy()

probation_table = doc_prob_only[doc_prob_only["Sentence Type"] == "Probation"].copy()
probation_table = probation_table.merge(
    eligible_people_sentences, how="inner", on=["person_id", "Sentence Date"]
)

probation_table = (
    probation_table.sort_values(
        by=["Sentence Date", "probation_total_days"], ascending=False
    )
    .groupby("person_id", as_index=False)
    .first()
)
probation_table = probation_table[probation_table["probation_total_days"] < 3650]
probation_table["estimated_probation_end_date"] = probation_table[
    "Sentence Date"
] + probation_table["probation_total_days"].apply(pd.offsets.Day)

# Estimate the probation termination date and then use it to determine the monthly population
population_dates = pd.date_range(
    date(2014, 1, 1),
    date(2019, 1, 1),
    freq="YS",
)
probation_population = []
for probation_date in population_dates:
    open_probation_population = sum(
        (probation_table["Sentence Date"] <= probation_date)
        & (probation_date <= probation_table["estimated_probation_end_date"])
    )
    probation_population.append(
        {"time_step": probation_date, "total_population": open_probation_population}
    )

probation_population_df = pd.DataFrame(probation_population)
print(probation_population_df)

# Probation admissions (outflows)
print(probation_table.groupby("sentence_year").count()["person_id"].sort_index())

#######

probation_table["probation_total_years"] = np.clip(
    np.floor(probation_table["probation_total_days"] / 365), a_min=1, a_max=None
)
prob_trans = probation_table.probation_total_years.value_counts()
doc_trans = doc_prob_only[
    doc_prob_only["Sentence Type"] == "DOC"
].doc_total_days.value_counts()


OUTLIER_TOLERANCE = 5  # Need to systematically remove obvious input errors

prob_trans = prob_trans[prob_trans > OUTLIER_TOLERANCE].reset_index()
doc_trans = doc_trans[doc_trans > OUTLIER_TOLERANCE].reset_index()

prob_trans = prob_trans.rename(
    columns={
        "probation_total_years": "total_population",
        "index": "compartment_duration",
    }
)
doc_trans = doc_trans.rename(
    columns={"doc_total_days": "total_population", "index": "compartment_duration"}
)

prob_trans["compartment"], prob_trans["outflow_to"], prob_trans["crime_type"] = [
    "probation",
    "release",
    "x",
]
print(prob_trans.sort_values(by="compartment_duration"))
doc_trans["compartment"], doc_trans["outflow_to"], doc_trans["crime_type"] = [
    "prison",
    "release",
    "x",
]

doc_trans["compartment_duration"] /= 365

transitions = pd.concat([prob_trans, doc_trans]).reindex(
    columns=[
        "compartment",
        "outflow_to",
        "total_population",
        "compartment_duration",
        "crime_type",
    ]
)

print(transitions)  # Data used for transition table
