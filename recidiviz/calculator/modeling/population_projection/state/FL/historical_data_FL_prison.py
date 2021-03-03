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
WIP
STATE: FL
POLICY: elderly release (SB 574)
VERSION: ?
DATA SOURCE: FL DOC reports
DATA QUALITY: reasonable
HIGHEST PRIORITY MISSING DATA: sentencing data beyond averages
ADDITIONAL NOTES:
Admissions info was only available in five year age increaments, so assumed to be even across increment (eg 55-59, 60-64, etc.)
Proportion of people incarcerated for each crime was based on data for all inmates 50+
Compartment duration for prison based on average sentences
"""

import pandas as pd
from recidiviz.calculator.modeling.population_projection.spark_bq_utils import (
    upload_spark_model_inputs,
)

pd.set_option("display.max_rows", None)
# pylint: skip-file


# RAW DATA
# time step length = 1 year
# reference_year = 2018
# DISAGGREGATION AXES
# ages 55-94
# offenses: murder/manslaughter, sexual, robbery, violent personal, burglary, theft/forgery/fraud, drug, weapons, other

# OUTFLOWS TABLE (pretrial to prison)
outflows_data_2019 = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/FL/2019-admissions-data.csv"
)
outflows_data_2018 = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/FL/2018-admissions-data.csv"
)
outflows_data = pd.DataFrame()
outflows_data = pd.concat([outflows_data, outflows_data_2019, outflows_data_2018])
# print(annual_outflows_data)

# TRANSITIONS TABLE
# data for average sentences in http://www.dc.state.fl.us/pub/annual/1819/FDC_AR2018-19.pdf
# data for recidivism rates in http://www.dc.state.fl.us/pub/recidivism/2019-2020/FDC_AR2019-20.pdf
prison_to_release_transitions_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/FL/transitions-data.csv"
)
release_to_prison_transitions_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/FL/transitions-recidivism.csv"
)
transitions_data = pd.concat(
    [prison_to_release_transitions_data, release_to_prison_transitions_data]
)

# TOTAL POPULATION DATA
# none

transitions_data = transitions_data.rename({"offense": "crime_type"}, axis=1).drop(
    ["rate_by_age", "rate_by_offense"], axis=1
)
transitions_data.loc[transitions_data.total_population.isnull(), "total_population"] = 1
outflows_data = outflows_data.rename({"offense": "crime_type"}, axis=1)

# STORE DATA
upload_spark_model_inputs(
    "recidiviz-staging", "FL_prison", outflows_data, transitions_data, pd.DataFrame()
)
