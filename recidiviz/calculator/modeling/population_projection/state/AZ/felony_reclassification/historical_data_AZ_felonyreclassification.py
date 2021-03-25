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
STATE: AZ
POLICY: The proposed policy would classify non-dangerous class 6 felonies as misdemeanors to reduce
length of sentences in prison.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1qPNt3Zy06AyG5hLO0oO4bj1qPnl0XdWb?usp=sharing
DATA QUALITY: moderate
HIGHEST PRIORITY MISSING DATA: LOS distributions, sentence distributions, probation data
REFERENCE_DATE: January 2016
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
            "recidiviz/calculator/modeling/population_projection/state/AZ/felony_reclassification/transitions_data_felonyAZ.csv"
        ),
    ]
)
transitions_data.loc[
    (transitions_data.compartment == "probation")
    & (transitions_data.outflow_to == "release"),
    "compartment_duration",
] = 6


# OUTFLOWS TABLE
outflows_data = pd.concat(
    [
        outflows_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/AZ/felony_reclassification/outflows_data_felonyAZ.csv"
        ),
    ]
)

# TOTAL POPULATION TABLE
total_population_data = pd.concat(
    [
        total_population_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/AZ/felony_reclassification/total_population_data_felonyAZ.csv"
        ),
    ]
)

# STORE DATA
simulation_tag = "AZ_reclassification"
upload_spark_model_inputs(
    "recidiviz-staging",
    simulation_tag,
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/AZ/felony_reclassification/AZ_reclassification_model_inputs.yaml",
)
