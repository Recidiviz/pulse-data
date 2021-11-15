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
STATE: KY
POLICY: pretrial reform, modeled as elimination of pretrial jail incarceration for nonviolent class D felons
VERSION: V1
DATA SOURCE: https://docs.google.com/spreadsheets/d/1J7fdNX-sISbzxuDwMam-S6v-32_-UL5r6GgTnHXKCaE/edit?usp=sharing
DATA QUALITY: poor
HIGHEST PRIORITY MISSING DATA: Actual jail LOS, jail admissions (currently extrapolating), jail LOS breakdown for violent vs nonviolent felons
REFERENCE_DATE: 2019
TIME_STEP: Year
ADDITIONAL NOTES: None
"""

import pandas as pd
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

outflows = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/KY/Pretrial/outflows.csv"
)
transitions = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/KY/Pretrial/transitions.csv"
)
population = pd.read_csv(
    "./recidiviz/calculator/modeling/population_projection/state/KY/Pretrial/population.csv"
)

outflows["total_population"] = outflows["total_population"].astype(float)
population["total_population"] = population["total_population"].astype(float)
transitions["total_population"] = transitions["total_population"].astype(float)

upload_spark_model_inputs(
    "recidiviz-staging",
    "ky_pretrial",
    outflows,
    transitions,
    population,
    "./recidiviz/calculator/modeling/population_projection/state/KY/Pretrial/KY_pretrial_model_inputs.yaml",
)
