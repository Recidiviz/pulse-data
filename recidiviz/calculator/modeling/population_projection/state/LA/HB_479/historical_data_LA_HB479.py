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
STATE: LA
POLICY: HB 479 - Expand eligibility for good time sentence dimunition, but reduce the rate at which sentence dimunitions can be earned for all prisoners.
Abolish discretionary parole for those admitted for crimes committed after August 1, 2021.
VERSION: V1
DATA SOURCE: https://docs.google.com/spreadsheets/d/1nVSCb5oFcZw9fsm_cgl0gABIB4bw5a9JeGfHXT8ceI0/edit?usp=sharing
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA: Average time on discretionary parole/average prison LOS prior to parole release
REFERENCE_DATE: June 2020
TIME_STEP: Month
ADDITIONAL NOTES: Currently modeling both sides of the bill simultaneously; can be split into two separate memos if necessary.
"""

import pandas as pd
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    transitions_interpolation,
)

outflows = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/LA/HB_479/outflows.csv"
)
population = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/LA/HB_479/population.csv"
)
monthly_transitions = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/LA/HB_479/transitions.csv"
)


def time_step_to_month(df, months_in_ts, divide):
    final_df = pd.DataFrame()
    for ts_original in df.time_step.unique():
        df_ts = df[df.time_step == ts_original]
        for ts_month in range(months_in_ts):
            month_df = df_ts.copy()
            month_df.time_step = months_in_ts * month_df.time_step + ts_month
            if divide:
                month_df.total_population /= months_in_ts
            final_df = pd.concat([final_df, month_df])
    return final_df


monthly_outflows = pd.concat(
    [
        time_step_to_month(
            outflows[outflows["compartment"].isin(["pretrial", "prison"])], 12, True
        ),
        time_step_to_month(
            outflows[outflows["compartment"].isin(["pretrial", "prison"]) == False],
            3,
            True,
        ),
    ]
)
monthly_population = pd.concat(
    [
        population[population["compartment"] == "prison"],
        time_step_to_month(
            population[population["compartment"].isin(["parole", "gtps"])], 12, False
        ),
        time_step_to_month(
            population[
                population["compartment"].isin(["prison", "parole", "gtps"]) == False
            ],
            3,
            False,
        ),
    ]
)

# just take prison populations while the supervision compartments are screwy
monthly_population = monthly_population[monthly_population.compartment == "prison"]
monthly_population.total_population = monthly_population.total_population.apply(float)

# move releases to parole to be releases to gtps
monthly_transitions.loc[
    (monthly_transitions.compartment == "prison")
    & (monthly_transitions.outflow_to == "gtps"),
    "total_population",
] += monthly_transitions.loc[
    (monthly_transitions.compartment == "prison")
    & (monthly_transitions.outflow_to == "parole"),
    "total_population",
].iloc[
    0
]

# rename gtps releases to be normal releases
monthly_transitions.loc[
    (monthly_transitions.compartment == "prison")
    & (monthly_transitions.outflow_to == "gtps"),
    "outflow_to",
] = "release"

# drop gtps and parole compartments, and outflow form prison to parole
monthly_transitions = monthly_transitions[
    ~(
        (monthly_transitions.compartment == "prison")
        & (monthly_transitions.outflow_to == "parole")
    )
]
monthly_transitions = monthly_transitions[~(monthly_transitions.compartment == "gtps")]
monthly_transitions = monthly_transitions[
    ~(monthly_transitions.compartment == "parole")
]

# remove technical revocations
monthly_transitions = monthly_transitions[
    ~(
        (monthly_transitions.compartment == "prison_parole_rev_new")
        | (monthly_transitions.compartment == "prison_parole_rev_tech")
    )
]
monthly_transitions = monthly_transitions[
    ~(monthly_transitions.compartment == "prison_gtps_rev_tech")
]

# change revocations to go to gtps re-incarceration
monthly_transitions.loc[
    (monthly_transitions.compartment == "release")
    & (monthly_transitions.outflow_to == "prison_parole_rev_new"),
    "outflow_to",
] = "prison"

# change revocations to be monthly
revocations_data = monthly_transitions.loc[
    (monthly_transitions.compartment == "release")
    & (monthly_transitions.outflow_to == "prison")
]
rates = revocations_data.total_population
years = (revocations_data.compartment_duration / 12).apply(int)
monthly_revocations = transitions_interpolation(
    "release", "prison", list(rates.values), list(years.values), disagg_label="age"
)
monthly_transitions = monthly_transitions.drop(revocations_data.index).append(
    monthly_revocations
)

# collapse outflows to just go to release
pretrial_outflows = monthly_outflows[monthly_outflows.compartment == "pretrial"]
prison_outflows = monthly_outflows[monthly_outflows.compartment == "prison"]
prison_outflows = (
    prison_outflows.groupby("time_step").sum().total_population.reset_index()
)
prison_outflows["age"] = "x"
prison_outflows["compartment"] = "prison"
prison_outflows["outflow_to"] = "release"

# redo releases double-counting scaling
prison_outflows.total_population /= 0.9191672315
pretrial_outflows.total_population /= 1 + monthly_revocations.total_population.sum()

monthly_outflows = pd.concat([pretrial_outflows, prison_outflows])

# redo population double-counting scaling
monthly_population.total_population /= 0.924889013


# redo outflows double-counting scaling

upload_spark_model_inputs(
    "recidiviz-staging",
    "la_hb479",
    monthly_outflows,
    monthly_transitions,
    monthly_population,
    "./recidiviz/calculator/modeling/population_projection/state/LA/HB_479/LA_HB479_model_inputs.yaml",
)
