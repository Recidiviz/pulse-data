# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
Historical data to be ingested for a particular state x policy combination.

File name should be `historical_data_{state_code}_{primary_compartment}.py`
 where state_code is of form NJ and primary compartment is the tag of the main
 compartment relevant to the policy.

STATE: WI
POLICY: The proposed policy would provide financial incentives for probation and parole
offices to reduce revocations to prison through better supervision practices. The
incentives would be funded through money saved from a reduced prison population.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/13I6JFfu7TuTdXXAcvjYx_nFOrDrcJEGs
DATA QUALITY: 2/5
HIGHEST PRIORITY MISSING DATA: LOS for P&P depending on revocation or not
REFERENCE_DATE: January 2010
TIME_STEP: 1 month
"""

# add whatever modules you need
import pandas as pd
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    transitions_uniform,
)


# TRANSITIONS TABLE
# populate transitions_data using utils
compartments_from = [
    "parole",
    "parole",
    "probation",
    "probation",
    "parole_revocation",
    "parole_revocation",
    "probation_revocation",
    "probation_revocation",
]
compartments_to = [
    "release",
    "parole_revocation",
    "release",
    "probation_revocation",
] + ["release"] * 4
lengths_of_stay = [37, 37, 24, 24, 31, 8, 31, 8]
probabilities = [0.61, 0.39, 0.71, 0.29, 0.26, 0.74, 0.10, 0.90]
transitions_data = pd.DataFrame()
for i, _ in enumerate(lengths_of_stay):
    transitions_data = pd.concat(
        [
            transitions_data,
            transitions_uniform(
                compartments_from[i],
                compartments_to[i],
                lengths_of_stay[i],
                probabilities[i],
            ),
        ]
    )


# OUTFLOWS TABLE
# populate outflows_data from raw data
outflows_data = pd.read_csv("/Users/zack/Downloads/outflows.csv")
outflows_data = outflows_data.loc[
    :, ["compartment", "outflow_to", "total_population", "time_step", "crime_type"]
]
# print(outflows_data.tail())


# TOTAL POPULATION TABLE
# populate total_population_data from raw data
total_population_data = pd.read_csv("/Users/zack/Downloads/totalpopulation.csv")
total_population_data = total_population_data[
    ["compartment", "total_population", "time_step", "crime_type"]
]
# print(total_population_data.head())


# STORE DATA
simulation_tag = "WI_po_incentives"
yaml_path = "recidiviz/calculator/modeling/population_projection/state/WI/WI_prison_model_inputs.yaml"
upload_spark_model_inputs(
    "recidiviz-staging",
    simulation_tag,
    outflows_data,
    transitions_data,
    total_population_data,
    yaml_path,
)
