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
"""outflow calculating object for ShellCompartments"""

from enum import Enum, auto
from typing import Dict, Tuple

import numpy as np
import pandas as pd
from numpy.linalg.linalg import LinAlgError
from statsmodels.tsa.arima.model import ARIMA, ARIMAResults

ORDER = (1, 1, 0)
MIN_NUM_DATA_POINTS = 4


class PredictionDirectionType(Enum):
    FORWARD = auto()
    BACKWARD = auto()


class PredictedAdmissions:
    """Predict the new admissions based on the historical trend"""

    def __init__(
        self,
        historical_data: pd.DataFrame,
        constant_admissions: bool,
    ):
        """
        historical_data is a DataFrame with columns for each time step and rows for each outflow_to type (jail, prison).
        Columns need to be numeric.
        Data must be continuous for each outflow, i.e. only NaN values on either end.

        The input data will not necessarily be sorted in temporal order, so that step is done here. Additionally, an
        ARIMA model will fail if all data is 0, so any rows with no data will be dropped as well.
        """
        historical_data, constant_admissions = self._infer_missing_data(
            historical_data, constant_admissions
        )
        self.historical_data = historical_data
        self.trained_model_dict: Dict[
            Tuple[str, PredictionDirectionType], ARIMAResults
        ] = {}
        self.predictions_df = pd.DataFrame(
            columns=["outflow_to", "time_step"]
        ).set_index(["outflow_to", "time_step"])

        # if historical data has more than specified number of years, train an ARIMA model
        if (
            len(self.historical_data.columns) >= MIN_NUM_DATA_POINTS
            and not constant_admissions
        ):
            self._train_arima_models()
            self.predict_constant_value = False
        else:
            self.predict_constant_value = True

        # add warnings attribute that prints at the end of shell_compartment initialization
        self.warnings: list = []

    def get_time_step_estimate(self, time_step: int) -> Dict[str, float]:
        """
        Return the estimated admissions for the time_step provided as a dict of compartment -> predicted value.

        If the time period is one for which we have actual data, return that. If it is for a year for which
        predictions have already been made, take that prediction from the dataframe. Else, generate predictions
        for the requested time period + an additional 10 steps.
        """
        default_steps_forward = 10
        if time_step in self.historical_data.columns:
            return self.historical_data[time_step].to_dict()

        if (
            time_step
            in self.predictions_df.index.get_level_values("time_step").unique()
        ):
            return (
                self.predictions_df.unstack(0).loc[time_step, "predictions"].to_dict()
            )

        last_time_step_to_process = int(
            max(self.historical_data.columns.max(), time_step) + default_steps_forward
        )
        self._gen_predicted_data(time_step, last_time_step_to_process)
        return self.predictions_df.unstack(0).loc[time_step, "predictions"].to_dict()

    def gen_arima_output_df(self) -> pd.DataFrame:
        """Return the prediction DataFrame"""
        historical_data = pd.Series(
            self.historical_data.stack(), name="actuals"
        ).to_frame()
        full_arima_output = pd.concat(
            [self.predictions_df, historical_data]
        ).sort_index()
        return full_arima_output

    @staticmethod
    def _infer_missing_data(
        historical_data: pd.DataFrame, constant_admissions: bool
    ) -> Tuple[pd.DataFrame, bool]:
        """Fill in historical data so all outflows cover the same time steps of data"""
        historical_data = historical_data.sort_index(axis=1)

        for outflow, row in historical_data.iterrows():
            missing_data = historical_data.columns[row.isnull()]

            min_data_ts = row.dropna().index.min()
            max_data_ts = row.dropna().index.max()

            missing_data_backward = missing_data[missing_data < min_data_ts]
            missing_data_forward = missing_data[missing_data > max_data_ts]

            if not missing_data_backward.empty:
                if len(row.dropna()) < MIN_NUM_DATA_POINTS:
                    constant_admissions = True
                    historical_data.loc[
                        outflow, missing_data_backward
                    ] = historical_data.loc[outflow, min_data_ts]
                else:
                    model_backcast = (
                        ARIMA(row.iloc[::-1].dropna().values, order=ORDER, trend="t")
                        .fit()
                        .forecast(steps=len(missing_data_backward))
                    )

                    # flip the predictions back around so they're ordered correctly for the historical data indexing
                    historical_data.loc[
                        outflow, missing_data_backward
                    ] = model_backcast[::-1]

            if not missing_data_forward.empty:
                if len(row.dropna()) < MIN_NUM_DATA_POINTS:
                    constant_admissions = True
                    historical_data.loc[
                        outflow, missing_data_forward
                    ] = historical_data.loc[outflow, max_data_ts]
                else:
                    model_forecast = (
                        ARIMA(row.dropna().values, order=ORDER, trend="t")
                        .fit()
                        .forecast(steps=len(missing_data_forward))
                    )

                    historical_data.loc[outflow, missing_data_forward] = model_forecast
        return historical_data, constant_admissions

    def _train_arima_models(self) -> None:
        """
        Create a dictionary to store the forecasted and backcasted trained ARIMA model objects
        A dictionary is created for each admission type with both a forecasting model and a backcasting model
        """
        trained_model_dict = {}
        for outflow_compartment, row in self.historical_data.iterrows():
            model_forecast = ARIMA(row.values, order=ORDER, trend="t")
            model_backcast = ARIMA(row.iloc[::-1].values, order=ORDER, trend="t")
            try:
                trained_model_dict[
                    (outflow_compartment, PredictionDirectionType.FORWARD)
                ] = model_forecast.fit()
                trained_model_dict[
                    (outflow_compartment, PredictionDirectionType.BACKWARD)
                ] = model_backcast.fit()

            except LinAlgError:
                # Add warnings
                warn_text = "Singular matrix encountered fitting ARIMA model."
                if warn_text not in self.warnings:
                    self.warnings.append(warn_text)

                # adjust forecast and backcast
                model_forecast = ARIMA(
                    row.values + np.random.normal(0, 0.001, len(row.values)),
                    order=ORDER,
                    trend="t",
                )
                model_backcast = ARIMA(
                    row.iloc[::-1].values + np.random.normal(0, 0.001, len(row.values)),
                    order=ORDER,
                    trend="t",
                )
                trained_model_dict[
                    (outflow_compartment, PredictionDirectionType.FORWARD)
                ] = model_forecast.fit()
                trained_model_dict[
                    (outflow_compartment, PredictionDirectionType.BACKWARD)
                ] = model_backcast.fit()

        self.trained_model_dict = trained_model_dict

    def _gen_predicted_data(self, start_period: int, end_period: int) -> None:
        """Generate the predictions between the start and end periods"""

        # calculate the range of time steps to forecast forward and backward
        pred_periods_forward = range(
            int(self.historical_data.columns.max()) + 1, int(end_period) + 1
        )
        pred_periods_backward = range(
            int(start_period), int(self.historical_data.columns.min())
        )[::-1]

        predictions_df = pd.DataFrame()

        for outflow_compartment, row in self.historical_data.iterrows():
            # If not specified to use the constant rate assumption...
            if not self.predict_constant_value:
                # Create dataframes to store forecasted and backcasted model outputs
                predictions_df_sub = pd.DataFrame()
                if len(pred_periods_backward) > 0:
                    backward_df = self._get_arima_predictions_df(
                        outflow_compartment=outflow_compartment,
                        cast_type=PredictionDirectionType.BACKWARD,
                        prediction_indexes=pred_periods_backward,
                    )
                    predictions_df_sub = pd.concat([predictions_df_sub, backward_df])

                if len(pred_periods_forward) > 0:
                    forward_df = self._get_arima_predictions_df(
                        outflow_compartment=outflow_compartment,
                        cast_type=PredictionDirectionType.FORWARD,
                        prediction_indexes=pred_periods_forward,
                    )
                    predictions_df_sub = pd.concat([predictions_df_sub, forward_df])

                # Combine forecast and backcast data
                predictions_df_sub = predictions_df_sub.sort_index().loc[
                    start_period:end_period
                ]

            # If using the constant rate assumption, just take the last value
            else:
                predictions_df_sub = pd.DataFrame(
                    index=range(start_period, end_period + 1),
                    columns=["predictions"],
                ).sort_index()
                predictions_df_sub.loc[
                    predictions_df_sub.index < int(self.historical_data.columns.min()),
                    "predictions",
                ] = row.iloc[0]
                predictions_df_sub.loc[
                    predictions_df_sub.index > int(self.historical_data.columns.max()),
                    "predictions",
                ] = row.iloc[-1]
                predictions_df_sub = predictions_df_sub[
                    ~predictions_df_sub.index.isin(self.historical_data.columns)
                ]

            # Label the dataframe indices
            predictions_df_sub.index.name = "time_step"
            predictions_df_sub = pd.concat(
                {outflow_compartment: predictions_df_sub}, names=["outflow_to"]
            )

            # Clip negative values at 0, throw warning if lower bound hit
            predictions_df_sub.loc[
                predictions_df_sub.predictions < 0, "predictions"
            ] = 0
            warn_text = "Warning: lower bound hit when predicting admissions."
            if any(predictions_df_sub.predictions == 0) and (
                warn_text not in self.warnings
            ):
                self.warnings.append(warn_text)

            # append df_sub to df
            max_allowable_pred = predictions_df_sub["predictions"].max()
            predictions_df_sub["predictions"] = predictions_df_sub["predictions"].clip(
                0, max_allowable_pred
            )

            predictions_df = pd.concat([predictions_df, predictions_df_sub])

        # If predictions are made more than once on an overlapping set of periods we will get duplicates. Drop those.
        predictions_df = pd.concat([predictions_df, self.predictions_df])
        predictions_df = predictions_df[
            ~predictions_df.index.duplicated(keep="first")
        ].sort_index()

        # Store the predictions
        self.predictions_df = predictions_df

    def _get_arima_predictions_df(
        self,
        outflow_compartment: str,
        cast_type: PredictionDirectionType,
        prediction_indexes: range,
    ) -> pd.DataFrame:
        """Helper function to generate the ARIMA forecast DataFrame for the provided prediction period

        Args:
            outflow_compartment: The compartment to generate the predicted admissions for
            cast_type: the type of forecast to use (forecast or backcast) from within the trained model
            prediction_indexes: the index labels for the generated prediction DataFrame

        Returns:
            pd.DataFrame with columns for the prediction, high/low conf interval, and standard error
        """
        outflow_model = self.trained_model_dict[outflow_compartment, cast_type]
        predictions_array = outflow_model.forecast(steps=len(prediction_indexes))
        prediction_data = {
            "predictions": predictions_array,
        }
        predictions_df = pd.DataFrame(index=prediction_indexes, data=prediction_data)
        return predictions_df

    def __eq__(self, other: object) -> bool:
        """Check if two PredictedAdmissions are equal (does not require projection_df to be equal)"""
        if not isinstance(other, PredictedAdmissions):
            return False

        try:
            if (self.historical_data != other.historical_data).any().any():
                return False
        except ValueError:
            return False

        if self.trained_model_dict != other.trained_model_dict:
            return False

        return True
