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
"""outflow calculating object for ShellCompartments"""
from typing import Optional

from statsmodels.tsa.arima_model import ARIMA
import pandas as pd

ORDER = (1, 1, 0)
MIN_NUM_DATA_POINTS = 4
MIN_THRESHOLD_PCT = 0.5
MAX_THRESHOLD_PCT = 0.5


class PredictedAdmissions:
    """Predict the new admissions based on the historical trend"""

    def __init__(self, historical_data: pd.DataFrame, constant_admissions: bool, projection_type: Optional[str] = None):
        """
        historical_data is a DataFrame with columns for each time step and rows for each outflow_to type (jail, prison).
        Columns need to be numeric.
        Data must be continuous for each outflow, i.e. only NaN values on either end.

        The input data will not necessarily be sorted in temporal order, so that step is done here. Additionally, an
        ARIMA model will fail if all data is 0, so any rows with no data will be dropped as well.
        """

        historical_data = historical_data.sort_index(axis=1)

        # fill out historical data so all outflows have the same time steps of data
        for outflow, row in historical_data.iterrows():
            missing_data = historical_data.columns[row.isnull()]

            min_data_ts = row.dropna().index.min()
            max_data_ts = row.dropna().index.max()

            missing_data_backward = missing_data[missing_data < min_data_ts]
            missing_data_forward = missing_data[missing_data > max_data_ts]

            if not missing_data_backward.empty:
                if len(row.dropna()) < MIN_NUM_DATA_POINTS:
                    constant_admissions = True
                    historical_data.loc[outflow, missing_data_backward] = historical_data.loc[outflow, min_data_ts]
                else:
                    model_backcast = ARIMA(row.iloc[::-1].dropna(), order=ORDER).fit(disp=False).forecast(
                        steps=len(missing_data_backward))[0]

                    # flip the predictions back around so they're ordered correctly for the historical data indexing
                    historical_data.loc[outflow, missing_data_backward] = model_backcast.iloc[::-1]

            if not missing_data_forward.empty:
                if len(row.dropna()) < MIN_NUM_DATA_POINTS:
                    constant_admissions = True
                    historical_data.loc[outflow, missing_data_forward] = historical_data.loc[outflow, max_data_ts]
                else:
                    model_forecast = ARIMA(row.dropna(), order=ORDER).fit(disp=False).forecast(
                        steps=len(missing_data_forward))[0]

                    historical_data.loc[outflow, missing_data_forward] = model_forecast

        self.historical_data = historical_data
        self.trained_model_dict = None
        self.pred_thresh_dict = None
        self.predictions_df = pd.DataFrame(columns=['outflow_to', 'time_step']).set_index(['outflow_to', 'time_step'])
        self.arima_output_df = None

        #if historical data has more than specified number of years, train an ARIMA model
        if len(self.historical_data.columns) >= MIN_NUM_DATA_POINTS and not constant_admissions:
            self._train_arima_models()
            self.predict_constant_value = False
        else:
            self.predict_constant_value = True

        if projection_type is None:
            projection_type = 'middle'

        if self.predict_constant_value and projection_type != 'middle':
            raise ValueError("Only 'middle' projection_type is supported for constant admissions")

        supported_projection_types = ['min', 'middle', 'max']
        if projection_type not in supported_projection_types:
            raise ValueError(f"'{projection_type}' projection_type is not supported. "
                             f"Expected {', '.join(supported_projection_types)}")

        self.projection_type = projection_type

    def _train_arima_models(self):
        # Create a dictionary to store the forecasted and backcasted trained ARIMA model objects
        # A dictionary is created for each admission type with both a forecasting model and a backcasting model
        trained_model_dict = {}
        for outflow_compartment, row in self.historical_data.iterrows():
            model_forecast = ARIMA(row.values, order=ORDER)
            model_backcast = ARIMA(row.iloc[::-1].values, order=ORDER)

            trained_model_dict[outflow_compartment] = {'forecast': model_forecast.fit(disp=False),
                                     'backcast': model_backcast.fit(disp=False)}
        self.trained_model_dict = trained_model_dict

    def _gen_predicted_data(self, start_period, end_period):
        """Generate the predictions between the start and end periods"""

        # calculate the number of steps to forecast forward and backward
        steps_forward = int(end_period - self.historical_data.columns.max())
        steps_backward = int(self.historical_data.columns.min() - start_period)
        pred_periods_forward = range(int(self.historical_data.columns.max()) + 1, int(end_period) + 1)
        pred_periods_backward = range(int(start_period), int(self.historical_data.columns.min()))[::-1]

        predictions_df = pd.DataFrame()

        for outflow_compartment, row in self.historical_data.iterrows():
            # If not specified to use the constant rate assumption...
            if not self.predict_constant_value:
                # Create dataframes to store forecasted and backcasted model outputs
                if steps_backward > 0:
                    predictions_array, stderr, conf = \
                        self.trained_model_dict[outflow_compartment]['backcast'].forecast(steps=steps_backward)

                    backward_df = pd.DataFrame(index=pred_periods_backward, data={'pred_middle': predictions_array,
                                                                                  'pred_min': conf[:, 0],
                                                                                  'pred_max': conf[:, 1],
                                                                                  'stderr': stderr})
                else:
                    backward_df = pd.DataFrame()

                if steps_forward > 0:
                    predictions_array, stderr, conf = \
                        self.trained_model_dict[outflow_compartment]['forecast'].forecast(steps=steps_forward)

                    forward_df = pd.DataFrame(index=pred_periods_forward, data={'pred_middle': predictions_array,
                                                                                'pred_min': conf[:, 0],
                                                                                'pred_max': conf[:, 1],
                                                                                'stderr': stderr})
                else:
                    forward_df = pd.DataFrame()

                # Combine forecast and backcast data
                predictions_df_sub = pd.concat([backward_df, forward_df]).sort_index().loc[start_period:end_period]

            # If using the constant rate assumption, just take the last value
            elif self.predict_constant_value:
                predictions_df_sub = pd.DataFrame(index=range(start_period, end_period + 1), columns=[
                    'pred_middle', 'pred_min', 'pred_max', 'stderr']).sort_index()
                predictions_df_sub.loc[predictions_df_sub.index < int(self.historical_data.columns.min()),
                                       'pred_middle'] = row.iloc[0]
                predictions_df_sub.loc[predictions_df_sub.index > int(self.historical_data.columns.max()),
                                       'pred_middle'] = row.iloc[-1]
                predictions_df_sub = predictions_df_sub[~predictions_df_sub.index.isin(self.historical_data.columns)]

            # Label the dataframe indices
            predictions_df_sub.index.name = 'time_step'
            predictions_df_sub = pd.concat({outflow_compartment: predictions_df_sub}, names=['outflow_to'])

            # Define max and min allowable predictions based on thresholds defined
            min_val_data, max_val_data = row.describe().loc[['min', 'max']].tolist()
            max_allowable_pred = max_val_data * MAX_THRESHOLD_PCT + max_val_data
            min_allowable_pred = min_val_data - min_val_data * MIN_THRESHOLD_PCT
            predictions_df_sub[['pred_middle', 'pred_min', 'pred_max']] = \
                predictions_df_sub[['pred_middle', 'pred_min', 'pred_max']].clip(min_allowable_pred, max_allowable_pred)

            predictions_df = predictions_df.append(predictions_df_sub)

        # If predictions are made more than once on an overlapping set of periods we will get duplicates. Drop those.
        predictions_df = pd.concat([predictions_df, self.predictions_df])
        predictions_df = predictions_df[~predictions_df.index.duplicated(keep='first')].sort_index()

        # combine historical and preds
        arima_output_df = \
            predictions_df.append(pd.Series(self.historical_data.stack(), name='actuals').to_frame()).sort_index()
        self.predictions_df = predictions_df
        self.arima_output_df = arima_output_df

    def get_time_step_estimate(self, time_step):
        """
        If the time period is one for which we have actual data, return that. If it is for a year for which
        predictions have already been made, take that prediction from the dataframe. Else, generate predictions
        for the requested time period + an additional 10 steps.
        """
        default_steps_forward = 10
        if time_step in self.historical_data.columns:
            return self.historical_data[time_step].to_dict()

        if time_step in self.predictions_df.index.get_level_values('time_step').unique():
            return self.predictions_df.unstack(0).loc[time_step, f'pred_{self.projection_type}'].to_dict()

        last_time_step_to_process = int(max(self.historical_data.columns.max(), time_step) + default_steps_forward)
        self._gen_predicted_data(time_step, last_time_step_to_process)
        return self.predictions_df.unstack(0).loc[time_step, f'pred_{self.projection_type}'].to_dict()

    def gen_arima_output_df(self):
        return self.arima_output_df
