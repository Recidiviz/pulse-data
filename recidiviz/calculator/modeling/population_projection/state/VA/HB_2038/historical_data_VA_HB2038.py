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
STATE: VA
POLICY: The proposed policy would create caps for lengths of probation terms and implement graduated sanctions for technical violations.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1D2TLdYBECEIkAxTWDnQsBt7bOJY5Tiyr?usp=sharing
DATA QUALITY: moderate
HIGHEST PRIORITY MISSING DATA: LOS distributions, sentence distributions, release distributions
REFERENCE_DATE: January 2015
TIME_STEP: 1 month
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

transitions_data = pd.DataFrame(
    columns=[
        "compartment",
        "outflow_to",
        "compartment_duration",
        "total_population",
        "crime",
    ]
)
outflows_data = pd.DataFrame(
    columns=["compartment", "outflow_to", "time_step", "total_population", "crime"]
)
total_population_data = pd.DataFrame(
    columns=["compartment", "time_step", "total_population", "crime"]
)

# TRANSITIONS TABLE
transitions_data = pd.concat(
    [
        transitions_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/VA/HB_2038/VAHB2038_transitions_data.csv"
        ),
    ]
)
prison_transitions = transitions_data.loc[transitions_data.compartment == "prison"]
transitions_data.loc[transitions_data.compartment == "prison", "compartment"] = (
    prison_transitions.compartment + "_" + prison_transitions.crime_type
)
to_prison_transitions = transitions_data.loc[transitions_data.outflow_to == "prison"]
transitions_data.loc[transitions_data.outflow_to == "prison", "outflow_to"] = (
    to_prison_transitions.outflow_to + "_" + to_prison_transitions.crime_type
)
transitions_data.crime_type = "NA"
transitions_data.loc[
    transitions_data.outflow_to == "prison_technical", "total_population"
] *= 2.7 / (2.7 + 7.8)
transitions_data.loc[
    transitions_data.outflow_to == "prison_newcrime", "total_population"
] *= 7.8 / (2.7 + 7.8)

COMPLETION_DURATION = 21
for crime_type in ["prison_newcrime", "prison_technical"]:
    chopped_total = transitions_data.loc[
        (transitions_data.compartment == "probation")
        & (transitions_data.outflow_to == crime_type)
        & (transitions_data.compartment_duration > COMPLETION_DURATION),
        "total_population",
    ].sum()
    remaining_total = transitions_data.loc[
        (transitions_data.compartment == "probation")
        & (transitions_data.outflow_to == crime_type)
        & (transitions_data.compartment_duration <= COMPLETION_DURATION),
        "total_population",
    ].sum()
    transitions_data.loc[
        (transitions_data.compartment == "probation")
        & (transitions_data.outflow_to == crime_type)
        & (transitions_data.compartment_duration <= COMPLETION_DURATION),
        "total_population",
    ] *= (remaining_total + chopped_total) / remaining_total
    transitions_data = transitions_data[
        ~(
            (transitions_data.compartment == "probation")
            & (transitions_data.outflow_to == crime_type)
            & (transitions_data.compartment_duration > COMPLETION_DURATION)
        )
    ]


# OUTFLOWS TABLE
outflows_data = pd.DataFrame(
    {
        "compartment": ["pretrial"] * 7 * 12,
        "outflow_to": ["probation"] * 7 * 12,
        "crime_type": ["NA"] * 7 * 12,
        "total_population": [33897 / 12] * 12
        + [28885 / 12] * 12
        + [28465 / 12] * 12
        + [28831 / 12] * 12
        + [20539 / 12] * 12
        + [24884 / 12] * 12
        + [25625 / 12] * 12,
        "time_step": [i for i in range(12, -72, -1)],
    }
)


# TOTAL POPULATION TABLE

# https://vadoc.virginia.gov/media/1473/vadoc-offender-population-trend-report-2015-2019.pdf
PROBATION_POP_DATA = pd.DataFrame(
    {
        "compartment": ["probation"] * 5,
        "crime_type": ["NA"] * 5,
        "total_population": [51019, 53445, 57058, 57944, 58520],
        "time_step": [0, 1, 2, 3, 4],
    }
)
final_pops = pd.DataFrame()
for year in PROBATION_POP_DATA.time_step.unique():
    year_pops = PROBATION_POP_DATA[PROBATION_POP_DATA.time_step == year]
    for month in range(12):
        month_pops = year_pops.copy()
        month_pops.time_step = 12 * month_pops.time_step + month
        final_pops = pd.concat([final_pops, month_pops])
PROBATION_POP_DATA = final_pops


total_population_data = pd.concat(
    [
        total_population_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/VA/HB_2038/VAHB2038_total_population.csv"
        ),
    ]
)
total_population_data = total_population_data[
    (total_population_data.time_step < 0)
    | (total_population_data.compartment != "probation")
]
total_population_data = pd.concat([total_population_data, PROBATION_POP_DATA])

# drop duplicate probation data
total_population_data = total_population_data[
    (total_population_data.compartment != "probation")
    | (total_population_data.crime_type != "newcrime")
]
# move disaggregation axis to compartments
prison_populations = total_population_data.loc[
    total_population_data.compartment == "prison"
]
total_population_data.loc[
    total_population_data.compartment == "prison", "compartment"
] = (prison_populations.compartment + "_" + prison_populations.crime_type)
total_population_data.crime_type = "NA"


# STORE DATA
simulation_tag = "VA_HB2038"
upload_spark_model_inputs(
    "recidiviz-staging",
    simulation_tag,
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/VA/HB_2038/VAHB2038_model_inputs.yaml",
)
