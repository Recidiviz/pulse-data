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
STATE: LA
POLICY: The proposed policy would reclassify nonviolent habitual offenders as regular offenders and sentence
them under regular sentencing guidelines. It would do the same for violent habitual offenders who have only
committed nonviolent felonies in the past.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/1SLNtsOcbV_qBF0eaQsDfUZNdzQSFeRyB?usp=sharing
DATA QUALITY: reasonable
HIGHEST PRIORITY MISSING DATA: previous conviction history of violent habitual offenders
REFERENCE_DATE: January 2013
TIME_STEP: 1 month
ADDITIONAL NOTES: AFP would like us to model two scenarios: the first is the original language of the bill
that reclassifies both nonviolent offenders and violent offenders with no prior violent felonies; the second
is a more conservative reform that only applies to nonviolent offenders. To model the first policy, run the
full simulation; to model the second, only run the simulation for the 'nonviolent' disaggregation category.
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import (
    upload_spark_model_inputs,
)

reference_year = 2013

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
            "recidiviz/calculator/modeling/population_projection/state/AZ/AZ_data/AZ_transitions.csv"
        ),
    ]
)

# OUTFLOWS TABLE
yearly_outflows_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/LA/LA_data/Habitual Offender Prison Admissions Data-Table 1.csv"
)

for year in range(2009, 2019):
    temp_monthly_outflows_data = pd.DataFrame(
        {
            "time_step": [
                i
                for i in range(
                    (year - reference_year) * 12, (year - reference_year + 1) * 12
                )
            ]
            * 2,
            "compartment": ["pretrial"] * 24,
            "outflow_to": ["prison"] * 24,
            "crime_type": ["non-violent"] * 12 + ["violent"] * 12,
            "total_population": [
                yearly_outflows_data.iloc[(year - 2009) * 2, 4] / 12
                for month in range(12)
            ]
            + [
                yearly_outflows_data.iloc[(year - 2009) * 2 + 1, 4] / 12
                for month in range(12)
            ],
        }
    )
    outflows_data = pd.concat([outflows_data, temp_monthly_outflows_data], sort=False)

# TOTAL POPULATION TABLE
yearly_total_population_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/LA/LA_data/Habitual Offender Total Population Data-Table 1.csv"
)

for year in range(2013, 2021):
    temp_monthly_total_population_data = pd.DataFrame(
        {
            "time_step": [
                i
                for i in range(
                    (year - reference_year) * 12, (year - reference_year + 1) * 12
                )
            ]
            * 2,
            "compartment": ["prison"] * 24,
            "crime_type": ["non-violent"] * 12 + ["violent"] * 12,
            "total_population": [
                yearly_total_population_data.iloc[(year - reference_year) * 2, 3]
                for month in range(12)
            ]
            + [
                yearly_total_population_data.iloc[(year - reference_year) * 2 + 1, 3]
                for month in range(12)
            ],
        }
    )
    total_population_data = pd.concat(
        [total_population_data, temp_monthly_total_population_data], sort=False
    )

# STORE DATA
upload_spark_model_inputs(
    "recidiviz-staging",
    "LA_HB_364",
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/LA/LA_prison_habitual_model_inputs.yaml",
)
