"""
STATE: GA
POLICY: The proposed policy would provide financial incentives for probation and parole offices to reduce
revocations to prison through better supervision practices. The incentives would be funded through money
saved from a reduced prison population.
VERSION: v1
DATA SOURCE: https://drive.google.com/drive/folders/18Zlw80mVuHPa5iK4TshOT0Dz4JwzrpOG?usp=sharing
DATA QUALITY: good
HIGHEST PRIORITY MISSING DATA: LOS distribution data
REFERENCE_DATE: January 2010
TIME_STEP: 1 month
"""
import pandas as pd
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)

# pylint: skip-file

transitions_data = pd.DataFrame(
    columns=[
        "compartment",
        "outflow_to",
        "placeholder_axis",
        "compartment_duration",
        "total_population",
    ]
)
outflows_data = pd.DataFrame(
    columns=[
        "compartment",
        "outflow_to",
        "placeholder_axis",
        "time_step",
        "total_population",
    ]
)
total_population_data = pd.DataFrame(
    columns=["compartment", "placeholder_axis", "time_step", "total_population"]
)

# TRANSITIONS TABLE
transitions_data = pd.concat(
    [
        transitions_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/GA/GA_data/Transitions Data-Table 1.csv"
        ),
    ]
)
transitions_data.loc[
    transitions_data.compartment == "prison_probation_revocations", "outflow_to"
] = "release"
transitions_data = transitions_data.rename({"placeholder_axis": "crime_type"}, axis=1)
# OUTFLOWS TABLE
outflows_data = pd.concat(
    [
        outflows_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/GA/GA_data/Outflows Data-Table 1.csv"
        ),
    ]
)
outflows_data = outflows_data.rename({"placeholder_axis": "crime_type"}, axis=1)
# TOTAL POPULATION TABLE
total_population_data = pd.concat(
    [
        total_population_data,
        pd.read_csv(
            "recidiviz/calculator/modeling/population_projection/state/GA/GA_data/Total Population Data-Table 1.csv"
        ),
    ]
)
total_population_data = total_population_data.rename(
    {"placeholder_axis": "crime_type"}, axis=1
)
# STORE DATA
# STORE DATA
upload_spark_model_inputs(
    "recidiviz-staging",
    "GA_prison",
    outflows_data,
    transitions_data,
    total_population_data,
    "recidiviz/calculator/modeling/population_projection/state/GA/GA_prison_revocations_model_inputs.yaml",
)
