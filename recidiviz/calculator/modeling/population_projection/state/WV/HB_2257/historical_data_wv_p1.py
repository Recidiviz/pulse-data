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

STATE: WV
POLICY: HB 2257
VERSION: V1
DATA SOURCE:https://docs.google.com/spreadsheets/d/11KHDNucxaYcRQeM_NmWHwE_yS1Ha5tbd-GdaxcMbtMA/edit#gid=2136222412
DATA QUALITY: decent
HIGHEST PRIORITY MISSING DATA: costs, supervised release parameters (percent assigned supervised release, average term length)
REFERENCE_DATE: 2019
TIME_STEP: month
ADDITIONAL NOTES:
"""
# pylint: skip-file
import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    transitions_uniform,
)

transitions_data = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/WV/HB_2257/wv_p1_transitions.csv"
)
outflows_data = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/WV/HB_2257/wv_p1_outflows.csv"
)

new_crime_revocation = transitions_data.loc[
    (transitions_data.compartment == "release")
    & (transitions_data.outflow_to == "prison_new_crime"),
    "total_population",
].iloc[0]

technical_revocation = transitions_data.loc[
    (transitions_data.compartment == "supervised_release")
    & (transitions_data.outflow_to == "prison_tech_rev"),
    "total_population",
].iloc[0]


# drop the supervised_release compartment, since the only different is technical revocations
transitions_data = transitions_data[
    (transitions_data.compartment != "supervised_release")
    & (transitions_data.total_population > 0)
]

# use uniform recidivism instead of single event
transitions_data = transitions_data[transitions_data.outflow_to != "prison_new_crime"]

transitions_data = transitions_data.append(
    transitions_uniform(
        c_from="release",
        c_to="prison_new_crime",
        mean_los=36,
        prob=new_crime_revocation,
        disagg_type="age",
    )
)

# switch tech revs to leapfrog to full_release
transitions_data.loc[
    transitions_data.compartment == "prison_tech_rev", "outflow_to"
] = "full_release"

# scale down outflows to avoid double counting
outflows_data.total_population *= (
    1
    - transitions_data.loc[
        (transitions_data.compartment == "release")
        & (transitions_data.outflow_to == "prison_new_crime"),
        "total_population",
    ].iloc[0]
)


column_names = ["compartment", "total_population", "time_step", "age"]
total_population_data = pd.DataFrame(columns=column_names)
transitions_data = transitions_data.astype({"compartment_duration": "float64"})
upload_spark_model_inputs(
    "recidiviz-staging",
    "wv_prison_p1",
    outflows_data,
    transitions_data,
    total_population_data,
    "./recidiviz/calculator/modeling/population_projection/state/WV/HB_2257/WV_hb2257_model_inputs.yaml",
)
