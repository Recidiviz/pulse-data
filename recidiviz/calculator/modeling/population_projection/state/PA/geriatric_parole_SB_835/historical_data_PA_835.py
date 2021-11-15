"""
STATE: OR
POLICY: Early medical release
DATA SOURCE: https://docs.google.com/spreadsheets/d/1ZPIp3Q4GzU_DErXX7uxKmKGxTn9El12ZTSv4u6HmZP0/edit?usp=sharing
DATA QUALITY: Excellent - case level Recidiviz state
HIGHEST PRIORITY MISSING DATA: None
ADDITIONAL NOTES:

"""

import numpy as np
import pandas as pd

from recidiviz.calculator.modeling.population_projection.super_simulation.time_converter import (
    TimeConverter,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_bq_utils import (
    upload_spark_model_inputs,
)
from recidiviz.calculator.modeling.population_projection.utils.spark_preprocessing_utils import (
    convert_dates,
)
from recidiviz.utils.yaml_dict import YAMLDict

DATA_PATH = "recidiviz/calculator/modeling/population_projection/state/PA/geriatric_parole_SB_835/"

# Load the data into pandas
outflow_data = pd.read_csv(DATA_PATH + "PA_SB_835 - outflows.csv")
population_data = pd.read_csv(DATA_PATH + "PA_SB_835 - population.csv")
prison_transition_data = pd.read_csv(DATA_PATH + "PA_SB_835 - baseline_transitions.csv")
parole_transition_data = pd.read_csv(DATA_PATH + "PA_SB_835 - parole_transitions.csv")

# Cap prison transitions at 20 years
prison_transition_data["compartment_duration"] = np.clip(
    prison_transition_data["compartment_duration"], a_min=None, a_max=360
)
prison_transition_data = (
    prison_transition_data.groupby(
        ["compartment_duration", "compartment", "outflow_to"]
    )
    .sum()
    .reset_index()
)

# Manually add transitions to release (18%) and death (9%) from the 72% of transitions that go to parole
prison_to_release = prison_transition_data.copy()
prison_to_release["outflow_to"] = "release"
prison_to_release["total_population"] *= 0.18 / 0.72

prison_to_death = prison_transition_data.copy()
prison_to_death["outflow_to"] = "death"
prison_to_death["total_population"] *= 0.09 / 0.72

prison_transition_data = pd.concat(
    [prison_transition_data, prison_to_release, prison_to_death]
)

# Combine pre-policy parole transitions with the prison transitions
pre_policy_parole_transitions = parole_transition_data[
    parole_transition_data["baseline"] == 1
]
pre_policy_parole_transitions = pre_policy_parole_transitions.drop(
    ["baseline", "policy"], axis=1
)
transition_data = prison_transition_data.append(pre_policy_parole_transitions)

# Cast the columns to the required types
outflow_data["total_population"] = outflow_data["total_population"].astype(float)
population_data["total_population"] = population_data["total_population"].astype(float)
transition_data["total_population"] = transition_data["total_population"].astype(float)
transition_data["compartment_duration"] = transition_data[
    "compartment_duration"
].astype(float)

# Add a dummy disaggregation (policy is modeled using only the eligible population)
outflow_data["age"] = "x"
population_data["age"] = "x"
transition_data["age"] = "x"

# Get the simulation tag from the model inputs config
yaml_file_path = DATA_PATH + "pa_geriatric_parole_model_inputs.yaml"
simulation_config = YAMLDict.from_path(yaml_file_path)
data_inputs = simulation_config.pop_dict("data_inputs")
simulation_tag = data_inputs.pop("big_query_simulation_tag", str)

# Convert the timestamps to time_steps (relative ints)
reference_date = simulation_config.pop("reference_date", float)
time_step = simulation_config.pop("time_step", float)
time_converter = TimeConverter(reference_year=reference_date, time_step=time_step)

outflow_data["time_step"] = convert_dates(time_converter, outflow_data["month_year"])
outflow_data = outflow_data.drop("month_year", axis=1)
population_data["time_step"] = convert_dates(
    time_converter, population_data["population_date"]
)
population_data = population_data.drop("population_date", axis=1)

upload_spark_model_inputs(
    project_id="recidiviz-staging",
    simulation_tag=simulation_tag,
    outflows_data_df=outflow_data,
    transitions_data_df=transition_data,
    total_population_data_df=population_data,
    yaml_path=yaml_file_path,
)
