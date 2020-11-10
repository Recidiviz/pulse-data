# Changelog
All notable changes to the Spark project will be documented in this file.

## [Unreleased]
### Added
- Include new features here

### Changed
- List any functional changes here

### Removed
- Describe what has been removed here

## [2.0.2] - 2020-11-04
### Added
- Models IL policies for reduced mandatory minimums, eliminated automatic weapon enhancements,
    reduced three strikes policy, truth in sentencing.
- Key features are the addition of helper functions for policy functions and vectorization
    for efficiency

### Changed
- New helper functions in CompartmentTransitions
- New state-data and notebooks for IL
- Cohort object replaced with CohortTable object and FullCompartment._generate_outflows()
    vectorized

### Removed
- NA

## [2.0.1] - 2020-10-13
### Added
- Models VA SB 5046
- Key feature is a generalized approach to removing mms, which is handled by subtracting 
    the fraction of convictions sentenced at the mm multiplied by the standard deviation
    the sentencing distribution from all sentences.

### Changed
- Nit changes to SubSimulation, SuperSimuluation, CompartmentTransitions
- New policy function added to IncarceratedTransitions

### Removed
- NA

## [2.0.0] - 2020-09-25
### Added
- State & policy specific configuration files for simulation inputs
- Dynamic simulation initialization using the input data to define the model structure
- Unit testing infrastructure

### Changed
- Moved the data preprocessing flow from notebooks to a generalized python pipeline
- Updated the simulation time granularity from yearly to customizable based on the data

### Removed
- Deleted deprecated Spark modules in favor of the new E2E_v2 module

## [1.0.0] - 2020-09-22
### Added
- The V1 Spark Simulation Model which simulates the incarcerated population and a set of sentencing policies
for Virginia and New Jersey
- ARIMA forecasting for predicted future admissions in the simulation
