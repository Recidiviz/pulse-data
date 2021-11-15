# type: ignore
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
STATE: federal
POLICY: Equal Act
VERSION: ?
DATA SOURCE:
DATA QUALITY: medium
HIGHEST PRIORITY MISSING DATA: population disaggregated by drug type

"""

import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    yearly_to_monthly_interpolation,
)

pd.set_option("display.max_rows", None)
# pylint: skip-file

# RAW DATA
# time step length = 1 month
# reference_year = 2017
# DISAGGREGATION AXES
# "crime_type" as proxy for powder/crack

# OUTFLOWS TABLE (pre-trial to prison)
# Admissions data from Sourcebooks here: https://www.ussc.gov/research/sourcebook-2020
# Tables titled SENTENCE IMPOSED RELATIVE TO THE GUIDELINE RANGE FOR DRUG TRAFFICKING OFFENDERS
crack_outflows_data = pd.DataFrame(
    {
        "time_step": [0, 1, 2, 3],
        "compartment": ["pretrial"] * 4,
        "outflow_to": ["prison"] * 4,
        "crime_type": ["crack"] * 4,
        "total_population": [1613, 1417, 1564, 1217],
    }
)
# Scale to account for recidivism (see sources below)
crack_outflows_data.total_population /= 1 - 0.294

powder_outflows_data = pd.DataFrame(
    {
        "time_step": [0, 1, 2, 3],
        "compartment": ["pretrial"] * 4,
        "outflow_to": ["prison"] * 4,
        "crime_type": ["powder"] * 4,
        "total_population": [3988, 3635, 3580, 2702],
    }
)
# Scale to account for recidivism (see sources below)
powder_outflows_data.total_population /= 1 - 0.185

outflows_data = pd.concat([crack_outflows_data, powder_outflows_data])

final_outflows = pd.DataFrame()
for year in outflows_data.time_step.unique():
    year_outflows = outflows_data[outflows_data.time_step == year]
    for month in range(12):
        month_outflows = year_outflows.copy()
        month_outflows.time_step = 12 * month_outflows.time_step - month
        month_outflows.total_population /= 12
        final_outflows = pd.concat([final_outflows, month_outflows])
outflows_data = final_outflows

# TRANSITIONS TABLE (prison to release)
# Recidivism (rearrest) is tracked for 8 years here: https://www.ussc.gov/research/research-reports/recidivism-among-federal-drug-trafficking-offenders
# Estimating annual amounts manually looking at graph.

crack_one_yr = 0.07
crack_two_yr = 0.12
crack_three_yr = 0.16
crack_four_yr = 0.19
crack_five_yr = 0.22
crack_six_yr = 0.25
crack_seven_yr = 0.28
crack_eight_yr = 0.294

powder_one_yr = 0.0333
powder_two_yr = 0.0667
powder_three_yr = 0.1
powder_four_yr = 0.12
powder_five_yr = 0.14
powder_six_yr = 0.15
powder_seven_yr = 0.17
powder_eight_yr = 0.185

monthly_crack_recidivism = pd.DataFrame(
    {
        "compartment": ["release"] * 97,
        "outflow_to": ["prison"] * 96 + ["release"],
        "crime_type": ["crack"] * 97,
        "compartment_duration": list(range(1, 97)) + [96.0],
        "total_population": yearly_to_monthly_interpolation(
            [
                crack_one_yr,
                crack_two_yr - crack_one_yr,
                crack_three_yr - crack_two_yr,
                crack_four_yr - crack_three_yr,
                crack_five_yr - crack_four_yr,
                crack_six_yr - crack_five_yr,
                crack_seven_yr - crack_six_yr,
                crack_eight_yr - crack_seven_yr,
            ],
            release_compartment="release_full",
        ),
    }
)

monthly_powder_recidivism = pd.DataFrame(
    {
        "compartment": ["release"] * 97,
        "outflow_to": ["prison"] * 96 + ["release"],
        "crime_type": ["powder"] * 97,
        "compartment_duration": list(range(1, 97)) + [96.0],
        "total_population": yearly_to_monthly_interpolation(
            [
                powder_one_yr,
                powder_two_yr - powder_one_yr,
                powder_three_yr - powder_two_yr,
                powder_four_yr - powder_three_yr,
                powder_five_yr - powder_four_yr,
                powder_six_yr - powder_five_yr,
                powder_seven_yr - powder_six_yr,
                powder_eight_yr - powder_seven_yr,
            ],
            release_compartment="release_full",
        ),
    }
)

# Sentence numbers from 2020 data here: https://www.ussc.gov/sites/default/files/pdf/research-and-publications/annual-reports-and-sourcebooks/2020/FigureD3.pdf
release_data = pd.DataFrame(
    {
        "compartment": ["prison", "release_full"] * 2,
        "outflow_to": ["release", "release_full"] * 2,
        "crime_type": ["crack", "crack", "powder", "powder"],
        "compartment_duration": [74, 74, 66, 66],
        "total_population": [1] * 4,
    }
)

transitions_data = pd.concat(
    [monthly_crack_recidivism, monthly_powder_recidivism, release_data]
)

total_population_data = pd.DataFrame()

upload_spark_model_inputs(
    "recidiviz-staging",
    "federal_prison",
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/FED/federal_prison_model_inputs.yaml",
)
