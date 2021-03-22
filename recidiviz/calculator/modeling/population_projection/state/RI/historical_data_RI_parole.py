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
STATE: RI
POLICY:
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1XgE5JBdjlwrDJO5cX-3xMd1ZwHAmw39v?usp=sharing
DATA QUALITY: reasonable
HIGHEST PRIORITY MISSING DATA: LOS distribution data
REFERENCE_DATE: January 2014
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
        "crime_type",
        "compartment_duration",
        "total_population",
    ]
)
outflows_data = pd.DataFrame(
    columns=["compartment", "outflow_to", "crime_type", "time_step", "total_population"]
)
total_population_data = pd.DataFrame(
    columns=["compartment", "crime_type", "time_step", "total_population"]
)

# TRANSITIONS TABLE
transitions_data = pd.concat(
    [
        transitions_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/RI/RI_data/Parole Transitions Data-Table 1.csv"
        ),
    ]
)

# OUTFLOWS TABLE
outflows_data = pd.concat(
    [
        outflows_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/RI/RI_data/Parole Outflows Data-Table 1.csv"
        ),
    ]
)

# TOTAL POPULATION TABLE
total_population_data = pd.concat(
    [
        total_population_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/RI/RI_data/Parole Total Population Data-Table 1.csv"
        ),
    ]
)

# STORE DATA
simulation_tag = "RI_parole"
upload_spark_model_inputs(
    "recidiviz-staging",
    simulation_tag,
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/RI/RI_parole_model_inputs.yaml",
)
