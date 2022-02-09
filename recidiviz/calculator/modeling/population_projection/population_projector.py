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
"""Run the entire micro simulation projection pipeline for a single state"""
import logging
from datetime import date, datetime, timedelta
from typing import Optional

import numpy as np
import pandas as pd
from pytz import timezone

from recidiviz.calculator.modeling.population_projection.super_simulation.super_simulation_factory import (
    SuperSimulationFactory,
)

# Number of years for the validation window and duration of the prediction interval
NUM_VALIDATION_YEARS = 5

# Compartments to exclude from the final population projection output
EXCLUDED_COMPARTMENTS = [
    "INCARCERATION - ABSCONSION",
    "INCARCERATION - INTERNAL_UNKNOWN",
    "INCARCERATION_OUT_OF_STATE - GENERAL",
    "PENDING_CUSTODY - PENDING_CUSTODY",
    "INCARCERATION_OUT_OF_STATE",
    "INTERNAL_UNKNOWN - INTERNAL_UNKNOWN",
    "PENDING_SUPERVISION - PENDING_SUPERVISION",
    # TODO(#9488): include absconsion, bench warrant, and informal probation in the output
    "SUPERVISION - ABSCONSION",
    "SUPERVISION - BENCH_WARRANT",
    "SUPERVISION - INTERNAL_UNKNOWN",
    "SUPERVISION - INFORMAL_PROBATION",
    "SUPERVISION_OUT_OF_STATE - ABSCONSION",
    "SUPERVISION_OUT_OF_STATE - BENCH_WARRANT",
    "SUPERVISION_OUT_OF_STATE - INFORMAL_PROBATION",
    "SUPERVISION_OUT_OF_STATE - PAROLE",
    "SUPERVISION_OUT_OF_STATE - PROBATION",
]


class PopulationProjector:
    """Encapsulate all of the logic to run a micro simulation end to end"""

    def __init__(self, yaml_path: str):
        self.simulation = SuperSimulationFactory.build_super_simulation(yaml_path)
        self.one_month_error = pd.DataFrame()
        self.historical_population = pd.DataFrame()
        self.prediction_intervals = pd.DataFrame()

    def run_projection(self) -> None:
        """Execute the entire projection pipeline:

        - Run the validation projections starting 5 years ago
        - Compute the 1-month prediction error for each validation model
        - Run the baseline forecast for the current month (defined in the YAML)
        - Calculate the prediction intervals and apply to the baseline forecast
        """
        last_validation_date = datetime.now(tz=timezone("US/Pacific")) - timedelta(
            weeks=4
        )
        end_year = last_validation_date.year
        end_month = last_validation_date.month

        # Run the validation projections
        # freq options = QS - quarter start, YS - year start, MS - month start
        # TODO(#9955): use longer forecasts to compute 5 year prediction intervals
        run_dates = pd.date_range(
            date(end_year - NUM_VALIDATION_YEARS, end_month, 1),
            date(end_year, end_month, 1),
            freq="MS",
        )
        logging.info("Running %s simulations", (len(run_dates) + 1))
        # Run all the validation sims for only 1 prediction month
        # projection_time_steps_override = 2 since the first time step is spent on initialization
        self.simulation.microsim_baseline_over_time(
            run_dates.tolist(), projection_time_steps_override=2
        )
        # Load the historical data and compute the error for the first prediction month
        self.historical_population = self.get_historical_population_data()
        self.one_month_error = self.compute_prediction_error()

        # Run the baseline projection over the latest month
        print(
            f"Running the baseline simulation on {self.simulation.initializer.get_user_inputs().run_date}"
        )
        self.simulation.simulate_baseline(display_compartments=[], reset=False)
        self.prediction_intervals = self.get_prediction_intervals()

    def compute_prediction_error(self, time_steps: int = 1) -> pd.DataFrame:
        """Return the prediction error over all the validation models for a
        particular number of `time_steps` following the projection start. Default
        value is 1 for the one month error."""
        # Get 1-month pop projections for error calculation by group/compartment
        validation_error = pd.DataFrame()
        population_simulation_dict = self.simulation.get_population_simulations()

        for _, population_simulation in population_simulation_dict.items():
            # Get population projection
            projection_df = population_simulation.get_population_projections()

            # Only keep the prediction that is `time_steps` after the projection start
            projection_start = projection_df.time_step.min()
            projection_df = projection_df.loc[
                projection_df.time_step == projection_start + time_steps
            ].copy()
            validation_error = pd.concat([validation_error, projection_df])

        # Reformat the data to match the backend expectations
        validation_error = self._format_data(validation_error)

        # Join historical data and calculate error
        validation_error = pd.merge(
            validation_error,
            self.historical_population,
            on=["compartment", "simulation_group", "time_step"],
            how="inner",
        )
        validation_error["error"] = (
            validation_error.total_population - validation_error.total_population_actual
        )
        self.convert_time_step(validation_error)
        return validation_error

    def get_historical_population_data(self) -> pd.DataFrame:
        """Format the historical data from the simulation to match the other tables"""
        simulation_data_inputs = self.simulation.initializer.get_data_inputs()
        historical_population = simulation_data_inputs.total_population_data
        # Drop the historical data outside the validation time period
        last_ts = self.simulation.initializer.get_first_relevant_ts()
        first_ts = last_ts - int(
            NUM_VALIDATION_YEARS / self.simulation.initializer.time_converter.time_step
        )
        historical_population = historical_population.loc[
            (historical_population.run_date == historical_population.run_date.max())
            & (historical_population.time_step.between(first_ts, last_ts))
        ]
        historical_population = historical_population[
            ["compartment", "gender", "time_step", "total_population"]
        ].copy()

        # Format the data to match the other tables
        historical_population.columns = [
            "compartment",
            "simulation_group",
            "time_step",
            "total_population_actual",
        ]
        historical_population = self._format_data(
            df=historical_population,
            total_population_col="total_population_actual",
        )
        return historical_population

    def get_prediction_intervals(self, alpha: float = 0.05) -> pd.DataFrame:
        """
        Compute the prediction intervals for each compartment/simulation group.

        Prediction interval size = (1-`alpha`)%
        """
        prediction_intervals = pd.DataFrame()

        population_projection = (
            self.simulation.get_population_simulations()["baseline_projections"]
            .get_population_projections()
            .copy()
        )
        population_projection = self._format_data(population_projection)

        for (
            compartment,
            simulation_group,
        ), one_month_error in self.one_month_error.groupby(
            ["compartment", "simulation_group"]
        ):

            # Get the projection value to center the interval around
            group_projection = population_projection.loc[
                (compartment, simulation_group)
            ].reset_index()

            # Compute the prediction intervals over the projection window
            num_samples = len(group_projection)
            group_bounds = self.resample_stationary(
                error=one_month_error.error.values,
                num_samples=num_samples,
                iterations=5000,
                weights=None,
                alpha=alpha,
            )

            # Center the prediction interval around the projection values
            # TODO(#9955): investigate if relative bounds are a better fit than absolute
            group_bounds["width"] = group_bounds.upper_bound - group_bounds.lower_bound
            x_pred = group_projection.time_step.values
            lower_bound = [
                max(y - x / 2, 0)
                for x, y in zip(group_bounds.width, group_projection.total_population)
            ]
            upper_bound = [
                y + x / 2
                for x, y in zip(group_bounds.width, group_projection.total_population)
            ]

            # Append the results to the prediction_intervals df
            n = len(x_pred)
            group_interval = pd.DataFrame(
                {
                    "year": x_pred,
                    "compartment": [compartment] * n,
                    "simulation_group": [simulation_group] * n,
                    "total_population": group_projection.total_population,
                    "total_population_min": lower_bound,
                    "total_population_max": upper_bound,
                }
            )
            prediction_intervals = pd.concat([prediction_intervals, group_interval])

        return prediction_intervals

    @staticmethod
    def resample_stationary(
        error: np.ndarray,
        num_samples: int = 12,
        iterations: int = 1000,
        weights: Optional[str] = None,
        alpha: float = 0.05,
    ) -> pd.DataFrame:
        """
        Resamples from array/list `error` and returns (1-`alpha`)% PIs calculated by
        bootstrapping `num_samples` periods `iters` times.

        Default error weights are uniform, but other `weights` options are 'triangular'
        or 'quadratic'.
        """
        # Default weights are uniform
        probs = np.ones(len(error))
        if weights == "triangular":
            probs = np.cumsum(probs)
        elif weights == "quadratic":
            probs = np.array([x**2 for x in np.cumsum(probs)])
        elif weights is not None:
            raise ValueError(
                f"Unsupported `weights` value {weights} supplied, accepted values are `triangular` or `quadratic`"
            )

        # regardless of weights type, probabilities should sum to 1
        probs = np.array([w / sum(probs) for w in probs])

        # prep dfs for storage
        sampled_error = pd.DataFrame()
        prediction_intervals = pd.DataFrame(
            {
                "lower_bound": [0] * (num_samples + 1),
                "upper_bound": [0] * (num_samples + 1),
            },
            index=list(range(num_samples + 1)),
        )

        # Resample the error array `num_samples` times
        for t in range(0, num_samples):
            sampled_error.loc[:, t] = np.random.choice(
                error, size=iterations, replace=True, p=probs
            )
            # Compute the cumulative sum
            if t > 0:
                sampled_error.loc[:, t] += sampled_error.loc[:, t - 1]

            # Get percentiles
            prediction_intervals.loc[t + 1, "lower_bound"] = float(
                np.percentile(sampled_error[t], 100 * alpha / 2)
            )
            prediction_intervals.loc[t + 1, "upper_bound"] = float(
                np.percentile(sampled_error[t], 100 * (1 - alpha / 2))
            )

        # Set the first 'observation' to zero error
        prediction_intervals.loc[0, ["lower_bound", "upper_bound"]] = [0, 0]

        return prediction_intervals

    def upload_projection(self) -> None:
        """Upload the projection data to BigQuery"""
        self.simulation.upload_baseline_simulation_results_to_bq(
            override_population_data=self.prediction_intervals
        )

    def convert_time_step(
        self, df: pd.DataFrame, time_step_col: str = "time_step"
    ) -> None:
        """Convert the time_step column from a relative int to a floating year value"""
        df[
            time_step_col
        ] = self.simulation.initializer.time_converter.convert_time_steps_to_year(
            df[time_step_col]
        )

    def _format_data(
        self, df: pd.DataFrame, total_population_col: str = "total_population"
    ) -> pd.DataFrame:
        df = PopulationProjector._convert_reincarceration(df)
        df = df[~df.index.get_level_values("compartment").isin(EXCLUDED_COMPARTMENTS)]
        df = PopulationProjector._add_aggregate_rows(df, total_population_col)
        self.convert_time_step(df)
        return df

    @staticmethod
    def _convert_reincarceration(df: pd.DataFrame) -> pd.DataFrame:
        """Change the RE-INCARCERATION compartment to GENERAL to match the back end"""

        def convert_compartment_name(compartment_name: str) -> str:
            if compartment_name == "INCARCERATION - RE-INCARCERATION":
                return "INCARCERATION - GENERAL"
            return compartment_name

        df["compartment"] = df["compartment"].apply(convert_compartment_name)
        df = (
            df.groupby(["compartment", "simulation_group", "time_step"])
            .sum()
            .reset_index("time_step", drop=False)
        )
        return df

    @staticmethod
    def _add_aggregate_rows(
        df: pd.DataFrame, total_pop_col: str = "total_population"
    ) -> pd.DataFrame:
        """Add aggregate rows for simulation group 'ALL' and legal status 'ALL'"""

        # Append rows for the "ALL" simulation_group
        simulation_group_all = pd.DataFrame(
            df.groupby(["compartment", "time_step"]).sum()[total_pop_col]
        ).reset_index(drop=False)
        simulation_group_all.loc[:, "simulation_group"] = "ALL"
        df = pd.concat([df.reset_index(drop=False), simulation_group_all])

        # Append rows for the "ALL" legal status
        # Add a temp column to store the `compartment_level_1` (incarceration vs supervision)
        df["compartment_level_1"] = [
            split[0] for split in df["compartment"].str.split(" - ")
        ]
        # Sum the total population per sector
        legal_status_all = pd.DataFrame(
            df.groupby(["compartment_level_1", "simulation_group", "time_step"]).sum()[
                total_pop_col
            ]
        ).reset_index(drop=False)
        # Reformat the legal status "ALL" df and combine with the original df
        legal_status_all.loc[:, "compartment"] = (
            legal_status_all.loc[:, "compartment_level_1"] + " - ALL"
        )
        # Drop the temp `compartment_level_1` column
        legal_status_all = legal_status_all.drop("compartment_level_1", axis=1)
        df = (
            pd.concat([df, legal_status_all])
            .set_index(["compartment", "simulation_group"])
            .sort_index()
        )
        return df
