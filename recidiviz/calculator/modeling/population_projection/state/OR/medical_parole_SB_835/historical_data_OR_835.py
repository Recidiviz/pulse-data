"""
STATE: OR
POLICY: Early medical release
VERSION: 1.0
DATA SOURCE: https://docs.google.com/spreadsheets/d/1jc7WdyBxaLhnAa1L9sQRSUySuLmzv7pRSR6TYYQlq84/edit?usp=sharing
DATA QUALITY: OK
HIGHEST PRIORITY MISSING DATA: Parole data (average pct. of sentence served)
ADDITIONAL NOTES:

"""

import pandas as pd

from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

data_path = "recidiviz/calculator/modeling/population_projection/state/OR/geriatric_parole_SB_835/"
outflow_data = pd.read_csv(data_path + "outflows.csv")
population_data = pd.read_csv(data_path + "population.csv")
transition_data = pd.read_csv(data_path + "transitions.csv")

outflow_data["total_population"] = outflow_data["total_population"].astype(float)
population_data["total_population"] = population_data["total_population"].astype(float)
transition_data["total_population"] = transition_data["total_population"].astype(float)


upload_spark_model_inputs(
    "recidiviz-staging",
    "OR_835",
    outflow_data,
    transition_data,
    population_data,
    data_path + "or_medical_parole_SB_835_model_inputs.yaml",
)
