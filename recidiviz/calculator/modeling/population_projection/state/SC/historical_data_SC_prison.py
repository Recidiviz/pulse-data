"""
STATE: SC
POLICY: Parole reform
VERSION: ?
DATA SOURCE: Mostly state FAQ docs
DATA QUALITY: bad
HIGHEST PRIORITY MISSING DATA: LOS by offense; offense more granular than "drugs"
ADDITIONAL NOTES:

"""

import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    transitions_lognorm,
)

pd.set_option("display.max_rows", None)
# pylint: skip-file

# RAW DATA
# time step length = 1 month
# reference_year = 2012
# DISAGGREGATION AXES
# none, using 'crime_type'

# OUTFLOWS TABLE (pre-trial to prison)
# prison admissions from FAQ reports - used only "new admissions from court", adjusted for percentage that were drug offenses
outflows_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/SC/outflows_data.csv"
)

final_outflows = pd.DataFrame()
for year in outflows_data.time_step.unique():
    year_outflows = outflows_data[outflows_data.time_step == year]
    for month in range(12):
        month_outflows = year_outflows.copy()
        month_outflows.time_step = 12 * month_outflows.time_step - month
        month_outflows.total_population /= 12
        final_outflows = pd.concat([final_outflows, month_outflows])
outflows_data = final_outflows

# TRANSITIONS TABLE

# Prison --> parole. Based on 2020 FAQ, time served by inmates released in 2020
# mean, std = get_lognorm_params(
#     [2, 5, 8, 11, 36, 90, 150, 210, 270, 330], # midpoints of LOS ranges in 2020 FAQ doc
#     [.04/3/.137, .098/3/.137, .124/3/.137, .112/3/.137, .48/48/.137, .079/60/.137, .034/60/.137, .017/60/.137, .008/60/.137, .003/60/.137]
# )
# Paco and Zack did some stuff here because it was complicated

# ratio_pop_drug_crimes = .15
# ratio_admissions_drug_crimes = .251
# adjusted_mean = mean * .15 / .251

transitions_data_prison_to_parole = pd.DataFrame()

prison_to_parole_transitions = pd.concat(
    [
        transitions_data_prison_to_parole,
        transitions_lognorm("prison", "parole", 0.85, 19.5, 48, 0.48, 330, 100),
    ]
)

# Parole --> prison (recidivism)

one_yr_rate = 0.05
two_yr_rate = 0.138
three_yr_rate = 0.202
released = 1 - three_yr_rate

parole_to_prison_transitions = pd.DataFrame(
    {
        "compartment": ["parole", "parole", "parole", "parole", "release"],
        "outflow_to": ["prison", "prison", "prison", "release", "release"],
        "crime_type": ["x", "x", "x", "x", "x"],
        "compartment_duration": [12.0, 24.0, 36.0, 36.0, 36.0],
        "total_population": [
            one_yr_rate,
            two_yr_rate - one_yr_rate,
            three_yr_rate - two_yr_rate,
            released,
            1.0,
        ],
    }
)

transitions_data = pd.concat(
    [prison_to_parole_transitions, parole_to_prison_transitions]
)

# TOTAL POPULATION TABLE (prison)
total_population_data = pd.read_csv(
    "recidiviz/calculator/modeling/population_projection/state/SC/total_population.csv"
)
# ignore parole

final_pop_data = pd.DataFrame()
for year in total_population_data.time_step.unique():
    year_pops = total_population_data[total_population_data.time_step == year]
    for month in range(12):
        month_pops = year_pops.copy()
        month_pops.time_step = 12 * month_pops.time_step + month
        final_pop_data = pd.concat([final_pop_data, month_pops])
total_population_data = final_pop_data


# STORE DATA

upload_spark_model_inputs(
    "recidiviz-staging",
    "SC_prison",
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/SC/SC_prison_model_inputs.yaml",
)
