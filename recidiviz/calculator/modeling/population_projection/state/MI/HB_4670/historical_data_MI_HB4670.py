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
STATE: MI
POLICY: HB 4670 - productivity credits for early release
VERSION: V1
DATA SOURCE: https://docs.google.com/spreadsheets/d/1dogLgdmc6PQcwHxxj1kbQu0M3ezmtpDAsWb2YPqSxxk/edit?usp=sharing
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA: Average prison LOS (currently extrapolating)
REFERENCE_DATE: 2019
TIME_STEP: Year
ADDITIONAL NOTES: None
"""

import pandas as pd
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    yearly_to_monthly_data,
)

outflows = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/MI/HB_4670/outflows.csv"
)
transitions = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/MI/HB_4670/transitions.csv"
)
population = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/MI/HB_4670/population.csv"
)

outflows["total_population"] = outflows["total_population"].astype(float)
population["total_population"] = population["total_population"].astype(float)
transitions["total_population"] = transitions["total_population"].astype(float)

# convert to monthly data
outflows = yearly_to_monthly_data(outflows, True)
population = yearly_to_monthly_data(population, False)
transitions.compartment_duration *= 12

upload_spark_model_inputs(
    "recidiviz-staging",
    "mi_hb4670",
    outflows,
    transitions,
    population,
    "./recidiviz/calculator/modeling/population_projection/state/MI/HB_4670/MI_HB4670_model_inputs.yaml",
)
