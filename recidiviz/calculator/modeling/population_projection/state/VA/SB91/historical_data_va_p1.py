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
POLICY: SB 91
VERSION: V1
DATA SOURCE:https://docs.google.com/spreadsheets/d/1J1BL_xZxi-fM-FNhRt0bLHDgTkVvIAc0dv2xwHv4Z3U/edit#gid=1705924760
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA: breakdown of technical vs. new crime revocations for time revoked & time imposed (currently using same average for both)
REFERENCE_DATE: 2020
TIME_STEP: month
ADDITIONAL NOTES: 
"""
# pylint: skip-file
import pandas as pd
import numpy as np
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

transitions_data = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/VA/SB91/va_p1_transitions.csv"
)
outflows_data = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/VA/SB91/va_p1_outflows.csv"
)
total_population_data = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/VA/SB91/va_p1_population.csv"
)

# Drop parole revocations outflows and population, which are all zero
outflows_data = outflows_data[outflows_data.compartment == "pre-trial"]
total_population_data = total_population_data[
    total_population_data.compartment == "pre-trial"
]

# Use parole to include all people with a possibility of recidivism
transitions_data.loc[transitions_data.compartment == "prison", "outflow_to"] = "parole"
transitions_data = transitions_data[transitions_data.compartment != "release"]
transitions_data.loc[
    transitions_data.compartment == "prison_parole_rev_new", "outflow_to"
] = "parole_two"

prison_data = transitions_data[transitions_data.compartment == "prison"]
transitions_data.loc[
    transitions_data.compartment == "parole", "compartment_duration"
] = 0.15 * np.average(
    prison_data.compartment_duration, weights=prison_data.total_population
)


outflows_data.time_step -= 1
total_population_data.time_step -= 1
# separate out multiple offenders

two_parole_transitions = transitions_data.loc[
    (transitions_data.compartment == "parole")
].copy()
two_rev_transitions = transitions_data.loc[
    transitions_data.compartment == "prison_parole_rev_new"
].copy()
three_parole_transitions = two_parole_transitions.copy()
three_rev_transitions = two_rev_transitions.copy()

two_parole_transitions.loc[
    two_parole_transitions.outflow_to == "prison_parole_rev_new", "outflow_to"
] = "prison_parole_rev_new_two"
two_parole_transitions.compartment = "parole_two"
three_parole_transitions.loc[
    three_parole_transitions.outflow_to == "prison_parole_rev_new", "outflow_to"
] = "prison_parole_rev_new_three"
three_parole_transitions.compartment = "parole_three"

two_rev_transitions.outflow_to = "parole_three"
two_rev_transitions.compartment = "prison_parole_rev_new_two"
three_rev_transitions.outflow_to = "parole_three"
three_rev_transitions.compartment = "prison_parole_rev_new_three"


transitions_data = pd.concat(
    [
        transitions_data,
        two_rev_transitions,
        two_parole_transitions,
        three_rev_transitions,
        three_parole_transitions,
    ]
)

upload_spark_model_inputs(
    "recidiviz-staging",
    "va_prison_p1",
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/NY/mms/ny_state_prison_model_inputs",
)
