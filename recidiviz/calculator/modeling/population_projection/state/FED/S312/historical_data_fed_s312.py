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
POLICY: S213
VERSION: V1
DATA SOURCE:
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA:
REFERENCE_DATE: September 2020
TIME_STEP: Month
ADDITIONAL NOTES: None
"""

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

from recidiviz.calculator.modeling.population_projection.super_simulation.time_converter import (
    TimeConverter,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

# Downloaded and uncompressed from https://github.com/khwilson/SentencingCommissionDatasets
# Data dict: https://www.ussc.gov/sites/default/files/pdf/research-and-publications/datafiles/USSC_Public_Release_Codebook_FY99_FY20.pdf
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    convert_dates,
)
from recidiviz.utils.yaml_dict import YAMLDict

SIXTY_YEARS = 60 * 12

raw_file_path = "recidiviz/calculator/modeling/population_projection/state/FED/"

new_cols = ["TOTPRISN", "AGE", "VIOL1PTS", "OFFGUIDE", "SENTMON", "SENTYR"]
old_cols = ["TOTPRISN", "AGE", "VIOL1PTS", "OFFTYPSB", "SENTMON", "SENTYR"]
ussc20 = pd.read_csv(raw_file_path + "sentencing_data/opafy20nid.csv", usecols=new_cols)
ussc19 = pd.read_csv(raw_file_path + "sentencing_data/opafy19nid.csv", usecols=new_cols)
ussc18 = pd.read_csv(raw_file_path + "sentencing_data/opafy18nid.csv", usecols=new_cols)
ussc17 = pd.read_csv(raw_file_path + "sentencing_data/opafy17nid.csv", usecols=old_cols)
ussc16 = pd.read_csv(raw_file_path + "sentencing_data/opafy16nid.csv", usecols=old_cols)


ussc20 = ussc20[["TOTPRISN", "AGE", "VIOL1PTS", "OFFGUIDE", "SENTMON", "SENTYR"]]
ussc18 = ussc18[["TOTPRISN", "AGE", "VIOL1PTS", "OFFGUIDE", "SENTMON", "SENTYR"]]

ussc_recent = ussc20.append([ussc19, ussc18])
ussc_recent = ussc_recent[
    ~ussc_recent["OFFGUIDE"].isin([3, 4, 7, 19, 20, 22, 24, 26, 27])
]  # Offense codes corresponding to violent offenses (2018 and later)

ussc_older = ussc17.append(ussc16)
ussc_older = ussc_older[
    ~ussc_older["OFFTYPSB"].isin([1, 2, 3, 4, 5, 6, 9, 28])
]  # Offense codes corresponding to violent offenses (2017 and earlier)
ussc_older.rename(
    columns={"OFFTYPSB": "OFFGUIDE"}, inplace=True
)  # Standardize column name for primary offense type

eligible = ussc_recent.append(ussc_older)
eligible = eligible[
    eligible["VIOL1PTS"] == 0
]  # Filter out offenders who received criminal history points for prior violent offenses
eligible = eligible[
    (eligible["TOTPRISN"] > 0) & (eligible["TOTPRISN"] < 9992)
]  # Filter out offenders with no prison sentence, along with offenders with life sentences, death sentences, etc.


AVG_PCT_SERVED = 0.883  # BJS 2016 Federal Justice Statistics, Table 8.3 (https://bjs.ojp.gov/content/pub/pdf/fjs16st.pdf)
eligible["expected_los"] = np.floor(
    np.clip(eligible["TOTPRISN"] * AVG_PCT_SERVED, a_min=1, a_max=SIXTY_YEARS)
)
eligible["AGE"] *= 12  # Standardize all time units to months

# Get the simulation tag from the model inputs config
yaml_file_path = raw_file_path + "S312/fed_s312_prison_model_inputs.yaml"
simulation_config = YAMLDict.from_path(yaml_file_path)
data_inputs = simulation_config.pop_dict("data_inputs")
simulation_tag = data_inputs.pop("big_query_simulation_tag", str)

# Convert the timestamps to time_steps (relative ints), with 0 being the most recent
# date of data (Sept. 2020)
reference_date = simulation_config.pop("reference_date", float)
time_step = simulation_config.pop("time_step", float)
time_converter = TimeConverter(reference_year=reference_date, time_step=time_step)

eligible["year"] = eligible["SENTYR"]
eligible["month"] = eligible["SENTMON"]
eligible["day"] = 1
eligible["month_year"] = pd.to_datetime(eligible[["year", "month", "day"]])
eligible["time_step"] = convert_dates(time_converter, eligible["month_year"])
eligible = eligible.drop(["year", "month", "day", "month_year"], axis=1)

# Offenders are eligible if they are 60 at time of sentencing, or if they turn 60 at some point during their sentence.
# All others are filtered out.
eligible = eligible[eligible["AGE"] + eligible["expected_los"] > SIXTY_YEARS]

# Compute the new Prison LOS under the policy
# The policy reduces LOS by 50% for eligible offenders over 60 years old.
# Offenders who will turn 60 during their sentence AFTER the halfway point should be
# released in the month they turn 60 and become eligible.
eligible["policy_los"] = np.ceil(
    np.where(
        ((0.5 * eligible["expected_los"]) + eligible["AGE"]) < SIXTY_YEARS,
        SIXTY_YEARS - eligible["AGE"],
        0.5 * eligible["expected_los"],
    ),
)

# Offenders serve the remainder of their sentence on home detention, prior to being released.
eligible["home_detent_los"] = eligible["expected_los"] - eligible["policy_los"]

assert_series_equal(
    eligible["expected_los"],
    (eligible["policy_los"] + eligible["home_detent_los"]),
    check_names=False,
)

assert (eligible["home_detent_los"] <= eligible["expected_los"] * 0.5).all()

# Estimate the monthly prison admissions based on sentence start date
# This conflates admission date and sentencing date, which is likely incorrect,
# but shouldn't distort the model too much if time between sentence start and BOP admission are relatively constant
outflows = eligible["time_step"].value_counts().reset_index()
outflows.rename(
    columns={"time_step": "total_population", "index": "time_step"}, inplace=True
)

outflows["compartment"] = "pretrial"
outflows["outflow_to"] = "prison"
outflows["age"] = "x"
outflows = outflows[
    ["compartment", "outflow_to", "total_population", "time_step", "age"]
]
outflows = outflows.sort_values(by=["time_step"]).reset_index(drop=True)
outflows["total_population"] = outflows["total_population"].astype(float)
# outflows.to_csv('outflows.csv',index=False)

population = eligible.groupby("time_step")["expected_los"].mean().reset_index()

# Estimate total population for each time step as (admissions during time step) * (average LOS for people sentenced during time step)
population["expected_los"] *= outflows["total_population"]
population.rename(columns={"expected_los": "total_population"}, inplace=True)
population["compartment"] = "prison"
population["age"] = "x"
population = population[["compartment", "total_population", "time_step", "age"]]
# population.to_csv('population.csv',index=False)

baseline_transitions = eligible["expected_los"].value_counts().reset_index()
baseline_transitions.rename(
    columns={"index": "compartment_duration", "expected_los": "total_population"},
    inplace=True,
)
baseline_transitions["compartment"] = "prison"
baseline_transitions["outflow_to"] = "release"
baseline_transitions["age"] = "x"
baseline_transitions = baseline_transitions[
    ["compartment", "outflow_to", "total_population", "compartment_duration", "age"]
]
manual_transitions = {
    "compartment": ["home_detent", "release", "prison"],
    "outflow_to": ["home_detent", "release", "home_detent"],
    "total_population": [1, 1, 0.1],
    "compartment_duration": [1, 1, SIXTY_YEARS + 1],
    "age": ["x", "x", "x"],
}
baseline_transitions = baseline_transitions.append(pd.DataFrame(manual_transitions))
baseline_transitions["total_population"] = baseline_transitions[
    "total_population"
].astype(float)
# baseline_transitions.to_csv('baseline_transitions.csv',index=False)

# Have non-eligible people transition prison -> release instead
# Grant rate 73/105 = 69.52% taken from p.20 https://www.gao.gov/assets/gao-12-807r.pdf
# Don't transfer someone to home detention if they have less than 6 months left of their sentence
non_eligible_mask = eligible["expected_los"] == eligible["policy_los"]
eligible["direct_release_to_prison"] = 1 - (73 / 105)
eligible.loc[non_eligible_mask, "direct_release_to_prison"] = 1
non_eligible_trans = (
    eligible.groupby("expected_los").sum()["direct_release_to_prison"].reset_index()
)

non_eligible_trans.rename(
    columns={
        "expected_los": "compartment_duration",
        "direct_release_to_prison": "total_population",
    },
    inplace=True,
)
non_eligible_trans["compartment"] = "prison"
non_eligible_trans["outflow_to"] = "release"


# Add the transition from prison -> home detention
eligible["prison_to_home_detention"] = 1 - eligible["direct_release_to_prison"]
policy_prison_trans = (
    eligible.groupby("policy_los").sum()["prison_to_home_detention"].reset_index()
)
policy_prison_trans.rename(
    columns={
        "policy_los": "compartment_duration",
        "prison_to_home_detention": "total_population",
    },
    inplace=True,
)
policy_prison_trans["compartment"] = "prison"
policy_prison_trans["outflow_to"] = "home_detent"

eligible["home_detent_los_shifted"] = np.clip(
    np.floor(eligible["home_detent_los"] * 0.65), a_min=1, a_max=None
)
shorter_terms = eligible[eligible["home_detent_los_shifted"] <= 120]
policy_home_trans = (
    shorter_terms.groupby("home_detent_los_shifted")
    .sum()["prison_to_home_detention"]
    .reset_index()
)
policy_home_trans.rename(
    columns={
        "home_detent_los_shifted": "compartment_duration",
        "prison_to_home_detention": "total_population",
    },
    inplace=True,
)
policy_home_trans["compartment"] = "home_detent"
policy_home_trans["outflow_to"] = "release"

policy_transitions = pd.concat(
    [policy_prison_trans, policy_home_trans, non_eligible_trans]
)
policy_transitions["age"] = "x"
policy_transitions = policy_transitions[
    ["compartment", "outflow_to", "total_population", "compartment_duration", "age"]
]

policy_transitions.to_csv(raw_file_path + "S312/policy_transitions.csv", index=False)

upload_spark_model_inputs(
    project_id="recidiviz-staging",
    simulation_tag=simulation_tag,
    outflows_data_df=outflows,
    transitions_data_df=baseline_transitions,
    total_population_data_df=population,
    yaml_path=yaml_file_path,
)
