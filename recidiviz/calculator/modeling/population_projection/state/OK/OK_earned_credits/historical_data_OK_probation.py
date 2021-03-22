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
STATE: OK
POLICY: The proposed policy would allow individuals to earn time off their probation through good behavior.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1bC-B-e3IAUh9XKhi7sU0HNlttDiWWn02?usp=sharing
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA: Actual total population and release data (as opposed to sentencing projections)
REFERENCE_DATE: January 2010
TIME_STEP: 1 month
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import (
    upload_spark_model_inputs,
)

transitions_data = pd.DataFrame(
    columns=[
        "compartment",
        "outflow_to",
        "placeholder_axis",
        "compartment_duration",
        "total_population",
    ]
)
outflows_data = pd.DataFrame(
    columns=[
        "compartment",
        "outflow_to",
        "placeholder_axis",
        "time_step",
        "total_population",
    ]
)

# TRANSITIONS TABLE
transitions_data = pd.concat(
    [
        transitions_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/OK/OK_data/Transitions Data-Table 1.csv"
        ),
    ]
)
transitions_data.loc[
    transitions_data.compartment == "prison_technical", "outflow_to"
] = "release"
transitions_data.loc[
    transitions_data.compartment == "probation ", "compartment"
] = "probation"

# OUTFLOWS TABLE
outflows_data = pd.concat(
    [
        outflows_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/OK/OK_data/Outflows Data-Table 1.csv"
        ),
    ]
)

outflows_data = outflows_data.rename({"placeholder_axis": "crime_type"}, axis=1)

transitions_data = transitions_data.rename({"placeholder_axis": "crime_type"}, axis=1)

# STORE DATA
# NB IF YOU RUN THIS FILE: There were two yaml files in the folder - please make sure the one passed in below is the correct one
upload_spark_model_inputs(
    "recidiviz-staging",
    "OK_probation",
    outflows_data,
    transitions_data,
    pd.DataFrame(),
    "recidiviz/calculator/modeling/population_projection/state/OK/OK_earned_credits/OK_probation_model_inputs_average_cost.yaml",
)
