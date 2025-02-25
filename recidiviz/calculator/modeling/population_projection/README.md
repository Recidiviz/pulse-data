# Spark Population Projection

## What is Spark
The Spark model is designed to simulate the criminal justice system in 
order to forecast populations and predict the impact of different policies. 
The model is designed as a stock and flow simulation that uses historical 
data to calculate how people flow through the criminal justice system. 
Then, when policies are applied to the simulation the flow changes 
and the estimated population difference can be measured, and 
the cost differential can be computed.

## Where things live
**Config Files**

These are yaml files detailing the inputs for state-specific projections.
- For micro-simulations these live in `recidiviz/calculator/modeling/population_projection/microsimulations/`
- For macro-simulations these live in `recidiviz/calculator/modeling/population_projection/state/` (inside state-specific folders)

**Modeling**

The classes and methods used for modeling are in `recidiviz/calculator/modeling/population_projection/` and are often accessed through Jupyter notebooks, such as the ones in `recidiviz/calculator/modeling/population_projection/notebooks/`

**Data Inputs**

The type of input depends on whether we're doing a MacroSim or MicroSim. MacroSims are done on aggregated data, while MicroSims are done on case-level data. 

For MacroSims, `recidiviz/calculator/modeling/population_projection/state/{state}/historical_data_{state}.py` files contain code to wrangle publicly available data and to upload it for modeling.
The underlying data imported by these files may exist in the Spark Google Drive folder.

For MicroSims, `pulse-data/recidiviz/calculator/query/state/views/population_projection/` contain views that feed into the population projections.

## Other Resources

The Spark Google Drive folder contains a lot of additional resources including:
- More detailed Spark methodology
- Data pre-processing onboarding materials
- Output from data pre-processing