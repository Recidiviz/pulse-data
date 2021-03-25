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
STATE: IL
POLICY: eliminating mandatory prison sentences (EMPS)
VERSION: V1
DATA_SOURCE: upload files from Illinois Prison Data Monthly folder (see Spark tracker/IL folder)
DATA QUALITY: excellent
TIME STEP: 1 month
POLICY DESCRIPTION: Currently, felony offenders who are convicted of (1) residential burglary,
(2) a second or subsequent Class 2 or Class 1 felony, or (3) a drug offense that is less serious
than Class X receive a mandatory prison sentence. The policy would give judges the option of sentencing
them to probation instead.
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

pd.set_option("display.max_rows", None, "display.max_columns", None)
# pylint: skip-file


reference_year = 2011

# DISAGGREGATION AXES
race = ["white", "non-white"]
crime_type = ["residential burglary", "second offense", "drugs"]
# residential burglary: self-explanatory
# second offense: conviction of class 1 or 2 felony from previously discharged individuals
# drugs: class 1, 2, 3, or 4 drug-related offense

# TRANSITIONS DATA
transitions_data = pd.DataFrame()

prison_transitions_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/IL/IL_data/EMPS Prison Transitions Data-Table 1.csv"
)

probation_transitions_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/IL/IL_data/EMPS Probation Transitions Data-Table 1.csv"
)

release_transitions_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/IL/IL_data/EMPS Release Transitions Data-Table 1.csv"
)

transitions_data = pd.concat(
    [
        transitions_data,
        prison_transitions_data,
        probation_transitions_data,
        release_transitions_data,
    ]
)

# OUTFLOWS DATA

outflows_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/IL/IL_data/EMPS Prison Admissions Data-Table 1.csv"
)

monthly_outflows_data = pd.DataFrame()

for year in range(2011, 2020):
    temp_monthly_outflows_data = pd.DataFrame(
        {
            "time_step": [
                i
                for i in range(
                    (year - reference_year) * 12, (year - reference_year + 1) * 12
                )
            ]
            * 6,
            "compartment": ["pretrial"] * 72,
            "outflow_to": ["prison"] * 72,
            "crime_type": ["residential burglary"] * 24
            + ["second offense"] * 24
            + ["drugs"] * 24,
            "race": ["white"] * 12
            + ["non-white"] * 12
            + ["white"] * 12
            + ["non-white"] * 12
            + ["white"] * 12
            + ["non-white"] * 12,
            "total_population": [
                outflows_data.iloc[(year - reference_year) * 6, 5] / 12
                for month in range(12)
            ]
            + [
                outflows_data.iloc[(year - reference_year) * 6 + 1, 5] / 12
                for month in range(12)
            ]
            + [
                outflows_data.iloc[(year - reference_year) * 6 + 2, 5] / 12
                for month in range(12)
            ]
            + [
                outflows_data.iloc[(year - reference_year) * 6 + 3, 5] / 12
                for month in range(12)
            ]
            + [
                outflows_data.iloc[(year - reference_year) * 6 + 4, 5] / 12
                for month in range(12)
            ]
            + [
                outflows_data.iloc[(year - reference_year) * 6 + 5, 5] / 12
                for month in range(12)
            ],
        }
    )
    monthly_outflows_data = pd.concat(
        [monthly_outflows_data, temp_monthly_outflows_data]
    )

# TOTAL POPULATION DATA

total_population_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/IL/IL_data/EMPS Total Prison Population Data-Table 1.csv"
)

monthly_total_population_data = pd.DataFrame()

for year in range(2011, 2020):
    temp_monthly_total_population_data = pd.DataFrame(
        {
            "time_step": [
                i
                for i in range(
                    (year - reference_year) * 12, (year - reference_year + 1) * 12
                )
            ]
            * 6,
            "compartment": ["prison"] * 72,
            "crime_type": ["residential burglary"] * 24
            + ["second offense"] * 24
            + ["drugs"] * 24,
            "race": ["white"] * 12
            + ["non-white"] * 12
            + ["white"] * 12
            + ["non-white"] * 12
            + ["white"] * 12
            + ["non-white"] * 12,
            "total_population": [
                total_population_data.iloc[(year - reference_year) * 6, 4]
                for month in range(12)
            ]
            + [
                total_population_data.iloc[(year - reference_year) * 6 + 1, 4]
                for month in range(12)
            ]
            + [
                total_population_data.iloc[(year - reference_year) * 6 + 2, 4]
                for month in range(12)
            ]
            + [
                total_population_data.iloc[(year - reference_year) * 6 + 3, 4]
                for month in range(12)
            ]
            + [
                total_population_data.iloc[(year - reference_year) * 6 + 4, 4]
                for month in range(12)
            ]
            + [
                total_population_data.iloc[(year - reference_year) * 6 + 5, 4]
                for month in range(12)
            ],
        }
    )
    monthly_total_population_data = pd.concat(
        [monthly_total_population_data, temp_monthly_total_population_data]
    )

# STORE DATA
upload_spark_model_inputs(
    "recidiviz-staging",
    "IL_prison_EMPS",
    monthly_outflows_data,
    transitions_data,
    monthly_total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/IL/IL_prison_EMPS_model_inputs.yaml",
)
