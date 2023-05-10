# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
STATE: FL
POLICY: Probation earned credits and remote reporting (SB 1378)
VERSION: v1.261.0
DATA SOURCE: FL annual reports
DATA QUALITY: poor
HIGHEST PRIORITY MISSING DATA: subset of community corrections serving felony probation
REFERENCE_DATE: July 2021
TIME_STEP: Year
ADDITIONAL NOTES:
"""

import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

DIRECTORY_PATH = "recidiviz/calculator/modeling/population_projection/state/FL/probation_earned_credits/"


# OUTFLOWS TABLE (pretrial to probation)
outflows_data = pd.read_csv(
    # DIRECTORY_PATH +
    "FL Probation Credits Data - outflows.csv"
)
outflows_data["total_population"] = outflows_data["total_population"].astype(float)


# TRANSITIONS TABLE
transitions_data = pd.read_csv(
    # DIRECTORY_PATH +
    "FL Probation Credits Data - transitions.csv"
)
transitions_data["compartment_duration"] = transitions_data[
    "compartment_duration"
].astype(float)

# TOTAL POPULATION DATA
total_population_data = pd.read_csv(
    # DIRECTORY_PATH +
    "FL Probation Credits Data - population.csv"
)

# STORE DATA
upload_spark_model_inputs(
    project_id="recidiviz-staging",
    simulation_tag="FL_SB_1378",
    outflows_data_df=outflows_data,
    transitions_data_df=transitions_data,
    total_population_data_df=total_population_data,
    yaml_path=(
        # DIRECTORY_PATH +
        "FL_SB_1378_model_inputs.yaml"
    ),
)
