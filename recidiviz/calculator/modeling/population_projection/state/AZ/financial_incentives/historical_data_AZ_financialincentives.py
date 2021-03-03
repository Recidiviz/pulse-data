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
POLICY: The proposed policy would provide financial incentives for probation and parole offices to reduce 
revocations to prison. The incentives would be funded through money saved from a reduced prison population.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1qPNt3Zy06AyG5hLO0oO4bj1qPnl0XdWb?usp=sharing
DATA QUALITY: moderate
HIGHEST PRIORITY MISSING DATA: LOS distributions
REFERENCE_DATE: January 2016
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
            "recidiviz/calculator/modeling/population_projection/state/AZ/financial_incentives/transitions_AZ_data.csv"
        ),
    ]
)

# OUTFLOWS TABLE
outflows_data = pd.concat(
    [
        outflows_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/AZ/financial_incentives/outflows_data AZ.csv"
        ),
    ]
)

# TOTAL POPULATION TABLE
total_population_data = pd.concat(
    [
        total_population_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/AZ/financial_incentives/total_population_data AZ.csv"
        ),
    ]
)

# STORE DATA
simulation_tag = "AZ_financialincentives"
upload_spark_model_inputs(
    "recidiviz-staging",
    simulation_tag,
    outflows_data,
    transitions_data,
    total_population_data,
)
