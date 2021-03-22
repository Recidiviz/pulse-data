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
STATE: TN
POLICY: The proposed policy would provide financial incentives for probation and parole offices to reduce
revocations to prison through better supervision practices. The incentives would be funded through money
saved from a reduced prison population.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1-CmF2fcfpegxHn5VFIDNf4WKidBDv74s?usp=sharing
DATA QUALITY: good
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
        "crime",
        "compartment_duration",
        "total_population",
    ]
)
outflows_data = pd.DataFrame(
    columns=["compartment", "outflow_to", "crime", "time_step", "total_population"]
)
total_population_data = pd.DataFrame(
    columns=["compartment", "crime", "time_step", "total_population"]
)

# TRANSITIONS TABLE
transitions_data = pd.concat(
    [
        transitions_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/TN/TN_data/Transitions Data-Table 1.csv"
        ),
    ]
)

# OUTFLOWS TABLE
outflows_data = pd.concat(
    [
        outflows_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/TN/TN_data/Outflows Data-Table 1.csv"
        ),
    ]
)
missing_outflows_low = (
    outflows_data[
        (outflows_data.time_step == 11) & (outflows_data.outflow_to == "probation")
    ]
    .iloc[0]
    .total_population
)
missing_outflows_high = (
    outflows_data[
        (outflows_data.time_step == 24) & (outflows_data.outflow_to == "probation")
    ]
    .iloc[0]
    .total_population
)

missing_outflows = pd.DataFrame(
    {
        "compartment": ["pre-supervision"] * 12,
        "outflow_to": ["probation"] * 12,
        "crime": ["x"] * 12,
        "time_step": range(12, 24),
        "total_population": [
            missing_outflows_high * i / 12 + missing_outflows_low * (1 - i / 12)
            for i in range(1, 13)
        ],
    }
)
outflows_data = outflows_data.append(missing_outflows)

# TOTAL POPULATION TABLE
total_population_data = pd.concat(
    [
        total_population_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/TN/TN_data/Total Population Data-Table 1.csv"
        ),
    ]
)

# STORE DATA
simulation_tag = "TN_prison_revocations"
upload_spark_model_inputs(
    "recidiviz-staging",
    simulation_tag,
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/TN/TN_prison_revocations_model_inputs.yaml",
)
