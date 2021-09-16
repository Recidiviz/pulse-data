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
from typing import Any, Optional, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns
from sklearn.metrics import mean_squared_error
from statsmodels.tsa.arima_model import ARIMA

# pylint: skip-file


class AdmissionsForecasting(object):
    def __init__(
        self,
        input_series: pd.Series,
        output_start_end: list,
        training_start_end: Optional[list] = None,
        max_forecast: float = 0.75,
        min_forecast: float = 0.75,
    ) -> None:
        self.input_series = input_series
        self.years = [x for x in range(output_start_end[0], output_start_end[1] + 1)]

        self.data_years = self.input_series.index

        if training_start_end:
            self.training_years = [
                x for x in range(training_start_end[0], training_start_end[1] + 1)
            ]
        else:
            self.training_years = self.input_series.index.tolist()

        self.steps_forward = max(self.years) - max(self.training_years)
        self.steps_back = min(self.training_years) - min(self.years)
        self.max_forecast = max_forecast
        self.min_forecast = min_forecast

        if self.max_forecast and self.min_forecast:
            self.min_val_training, self.max_val_training = (
                self.input_series.loc[self.training_years]
                .describe()
                .loc[["min", "max"]]
                .tolist()
            )
            self.max_allowable_pred = (
                self.max_val_training * self.max_forecast + self.max_val_training
            )
            self.min_allowable_pred = (
                self.min_val_training - self.min_val_training * self.min_forecast
            )

        # if not self.training_years: self.training_years = self.years

    def return_model_results(
        self, pred_type: str, order: Optional[tuple] = None
    ) -> pd.DataFrame:
        # needs to be subsetted for the years we are using to train
        input_series = self.input_series.loc[self.training_years]
        if pred_type == "forecast":
            pred_years = [x for x in self.years if x > max(self.training_years)]
            steps = self.steps_forward

        if pred_type == "backcast":
            input_series = input_series.iloc[::-1]
            pred_years = [x for x in self.years if x < min(self.training_years)]
            steps = self.steps_back

        model = ARIMA(input_series.values, order=order)
        model_fit = model.fit(disp=False)
        predictions_array, stderr, conf = model_fit.forecast(steps=steps)
        if pred_type == "forecast":
            results = pd.DataFrame(index=pred_years)
        if pred_type == "backcast":
            results = pd.DataFrame(index=pred_years[::-1])
        results["preds"] = predictions_array
        results["pred_min"] = conf[:, 0]
        results["pred_max"] = conf[:, 1]
        results["stderr"] = stderr
        return results

    def gen_forecasted_data(
        self, method: str = "arima", order: Optional[Tuple] = None
    ) -> pd.DataFrame:

        if method == "constant":
            back_years = [x for x in self.years if x < min(self.training_years)]
            forward_years = [x for x in self.years if x > max(self.training_years)]
            predicted_data = pd.Series(
                index=back_years + forward_years, dtype=np.float64
            ).sort_index()
            predicted_data.loc[back_years] = self.input_series.loc[
                min(self.training_years)
            ]
            predicted_data.loc[forward_years] = self.input_series.loc[
                max(self.training_years)
            ]
            predicted_data.name = "preds"

            predicted_data = pd.DataFrame(
                predicted_data, columns=["preds", "pred_min", "pred_max", "stderr"]
            )

        if method == "arima":
            if self.steps_forward > 0:
                forecast = self.return_model_results("forecast", order=order)
            else:
                forecast = pd.DataFrame()

            if self.steps_back > 0:
                backcast = self.return_model_results("backcast", order=order)[::-1]
            else:
                backcast = pd.DataFrame()

            predicted_data = pd.concat([backcast, forecast]).sort_index()

        predicted_data = predicted_data.reindex(self.years)
        predicted_data = predicted_data.join(
            pd.Series(self.input_series, name="actuals")
        ).sort_index()
        predicted_data = predicted_data[
            ["actuals", "preds", "pred_min", "pred_max", "stderr"]
        ]

        if self.max_forecast and self.min_forecast:

            predicted_data.loc[
                predicted_data.preds > self.max_allowable_pred, "preds"
            ] = self.max_allowable_pred
            predicted_data.loc[
                predicted_data.preds < self.min_allowable_pred, "preds"
            ] = self.min_allowable_pred

        return predicted_data

    def gen_plots(
        self,
        method: str = "arima",
        order: Optional[Tuple] = None,
        display_bounds: bool = True,
        title: Optional[str] = None,
        display_cutoff: Optional[bool] = True,
    ) -> None:
        sns.set()
        to_vis = self.gen_forecasted_data(method, order=order)
        fig, ax = plt.subplots(figsize=(8, 6))

        # actuals - training
        to_vis.loc[self.training_years]["actuals"].plot(
            ax=ax, color="tab:cyan", marker="o"
        )
        # actuals - validation

        validation_years = to_vis[
            (to_vis.preds.notnull() & (to_vis.actuals.notnull()))
        ].index.tolist()
        forecast_validation_years = [
            x for x in validation_years if x > max(self.training_years)
        ]
        backcast_validation_years = [
            x for x in validation_years if x < min(self.training_years)
        ]

        if len(validation_years) > 0:
            to_vis.loc[forecast_validation_years]["actuals"].plot(
                ax=ax, color="tab:blue", marker="o", label="actuals - validation"
            )
            to_vis.loc[backcast_validation_years]["actuals"].plot(
                ax=ax, color="tab:blue", marker="o", label="actuals - validation"
            )

        # return to_vis[(to_vis.preds.notnull())]['actuals']#.plot(ax = ax, color = 'blue', marker = 'o', label = 'actuals_validation')

        # first plot actuals
        # if display_validation_data:
        #    to_vis['actuals'].plot(ax = ax, color = 'blue', marker = 'o', label = 'actuals')
        # else:
        #    to_vis[to_vis.preds.isnull()]['actuals'].plot(ax = ax, color = 'blue', marker = 'o', label = 'actuals')

        # now plot predictions
        to_vis["preds"].plot(ax=ax, color="red", marker="o", label="predictions")

        # give training years a different color
        # to_vis.loc[self.training_years]['actuals'].plot(ax = ax, marker='o', color = 'black', label = 'training', linewidth=1)

        if display_bounds:
            if to_vis.pred_min.notnull().any():
                ax.fill_between(
                    to_vis.index,
                    to_vis["pred_min"],
                    to_vis["pred_max"],
                    alpha=0.4,
                    color="orange",
                )

        if title:
            plt.title(title)
        else:
            plt.title(self.input_series.name)
        plt.ylim(bottom=0, top=max([to_vis.preds.max(), to_vis.actuals.max()]) * 1.1)

        handles, labels = plt.gca().get_legend_handles_labels()
        by_label = dict(zip(labels, handles))
        plt.legend(by_label.values(), by_label.keys(), loc="lower left")

        if self.max_forecast and self.min_forecast and display_cutoff:
            ax.axhline(y=self.max_allowable_pred, ls="--", alpha=0.5)
            ax.axhline(y=self.min_allowable_pred, ls="--", alpha=0.5)

        # ax.set_xticks(self.years)
        # return to_vis


def error_metrics(model_results: Any) -> Tuple[float, float]:
    test_set = model_results[
        model_results.actuals.notnull() & model_results.preds.notnull()
    ]

    ##results
    try:
        rmse = np.sqrt(mean_squared_error(test_set["actuals"], test_set["preds"]))
        pct_error = (
            abs(test_set["actuals"] - test_set["preds"]) / test_set["actuals"]
        ).mean()
    except:
        rmse, pct_error = np.nan, np.nan
    return rmse, pct_error
    # return test_set


def nj_data() -> pd.DataFrame:
    # NJ Historical counts of adult offenders in the prison system at the beginning of the year
    # 5595 admissions 2018
    historical_offender_counts = pd.DataFrame(
        {
            "year": [2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012, 2011],
            "drug_sentences": [
                1525,
                1541,
                1639,
                1749,
                2010,
                2286,
                2411,
                2825,
                3128,
                3741,
            ],
            "property_sentences": [
                817,
                904,
                1048,
                1137,
                1276,
                1419,
                1557,
                1665,
                1706,
                1836,
            ],
            #'median_sentence_length': [7, 6, 6, 6, 6, 6, 6, 6, 6, 5.7]
            "property_median_sentence_length": [
                3.5,
                3.5,
                3.5,
                3.5,
                3.5,
                3.5,
                3.5,
                3.5,
                3.5,
                3,
            ],
            "drug_median_sentence_length": [2.5, 2, 2, 2, 2, 2, 2, 2, 1.75, 1.5],
        }
    ).sort_values(by="year")
    historical_offender_counts["drug_p_y"] = 1 - (
        1 / historical_offender_counts["drug_median_sentence_length"]
    )
    historical_offender_counts["property_p_y"] = 1 - (
        1 / historical_offender_counts["property_median_sentence_length"]
    )
    historical_offender_counts["drug_releases"] = np.round(
        historical_offender_counts["drug_sentences"]
        - (
            historical_offender_counts["drug_sentences"]
            * historical_offender_counts["drug_p_y"]
        ),
        0,
    ).astype(int)
    historical_offender_counts["property_releases"] = np.round(
        historical_offender_counts["property_sentences"]
        - (
            historical_offender_counts["property_sentences"]
            * historical_offender_counts["property_p_y"]
        ),
        0,
    ).astype(int)
    historical_offender_counts["drug_admissions"] = historical_offender_counts[
        "drug_releases"
    ] - historical_offender_counts["drug_sentences"].diff(periods=-1)
    historical_offender_counts["property_admissions"] = historical_offender_counts[
        "property_releases"
    ] - historical_offender_counts["property_sentences"].diff(periods=-1)

    nj_df = historical_offender_counts.set_index("year")[
        ["drug_admissions", "property_admissions"]
    ].dropna()
    return nj_df


def nd_data(time_agg: str = "year") -> pd.DataFrame:
    ignored_subgroups = [
        "TRANSFER",
        "TRANSFER_FROM_OTHER_JURISDICTION",
        "TEMPORARY_CUSTODY",
        "EXTERNAL_UNKNOWN",
        "INTERNAL_UNKNOWN" "TRANSFER_FROM_OTHER_JURISDICTION",
        "ADMITTED_IN_ERROR",
        "RETURN_FROM_ERRONEOUS_RELEASE",
        "RETURN_FROM_ESCAPE",
    ]

    admissions_data = pd.read_csv(
        "/Users/agaidus/Downloads/bq-results-20200826-165151-rffcxvw8yokh.csv"
    )
    admissions_data["admission_date"] = pd.to_datetime(admissions_data.admission_date)
    admissions_data["year"] = admissions_data["admission_date"].dt.year
    admissions_data = admissions_data[
        ~admissions_data.admission_reason.isin(ignored_subgroups)
    ]
    admissions_data = admissions_data[admissions_data["year"] >= 2005]
    admissions_data = admissions_data[admissions_data["year"] < 2020]
    admissions_data["month"] = admissions_data.admission_date.apply(
        lambda x: x.date().replace(day=1)
    )

    if time_agg == "year":
        nd_df = (
            admissions_data.groupby(
                [
                    admissions_data.year,
                    admissions_data.incarceration_type.map(
                        {"COUNTY_JAIL": "jail", "STATE_PRISON": "prison"}
                    ),
                ]
            )
            .size()
            .unstack()
            .fillna(0)
            .astype(int)
        )

    if time_agg == "month":
        nd_df = (
            admissions_data.groupby(
                [
                    admissions_data.month,
                    admissions_data.incarceration_type.map(
                        {"COUNTY_JAIL": "jail", "STATE_PRISON": "prison"}
                    ),
                ]
            )
            .size()
            .unstack()
            .fillna(0)
            .astype(int)
        )

    nd_df["total"] = nd_df.sum(1)
    return nd_df


def va_data() -> pd.DataFrame:
    historical_admissions = pd.read_csv(
        "/Users/agaidus/recidiviz-research/spark/sentencing_policy_impact_v1/VA_data/processed_va_historical_sentences_v2.csv"
    )

    # Filter to the supported sentence types
    supported_sentence_types = ["jail", "prison"]
    jail_prison_admissions = historical_admissions[
        historical_admissions["sentence_type"].isin(supported_sentence_types)
    ]

    # Remove sentences with 0 month sentence lengths
    jail_prison_admissions = jail_prison_admissions[
        jail_prison_admissions["effective_sentence_months"] > 0
    ].copy()
    # print(len(jail_prison_admissions))

    jail_prison_admissions["simulation_group_name"] = jail_prison_admissions[
        "offense_code"
    ]

    jail_prison_admissions["year"] = jail_prison_admissions["year"].astype(int)
    jail_prison_admissions.loc[
        jail_prison_admissions.offense_group == "DRUG/SCHEDULE I/II", "crime_category"
    ] = "drug_crime"
    jail_prison_admissions.loc[
        jail_prison_admissions.offense_group != "DRUG/SCHEDULE I/II", "crime_category"
    ] = "other_crime"

    jail_prison_admissions = jail_prison_admissions.groupby(
        ["year", "crime_category"]
    ).size()
    jail_prison_admissions = jail_prison_admissions.unstack(1).fillna(0).astype(int)

    return jail_prison_admissions


def get_tuning_results(z: "AdmissionsForecasting") -> pd.DataFrame:
    p_values = range(1, 10)
    d_values = range(0, 3)
    q_values = range(0, 3)
    order_combs = [(p, d, q) for p in p_values for d in d_values for q in q_values]

    tuning_results = pd.DataFrame(index=order_combs)
    for order in order_combs:
        try:
            rmse, pct_error = error_metrics(
                z.gen_forecasted_data(method="arima", order=order)
            )
            tuning_results.loc[order, "rmse"] = rmse
            tuning_results.loc[order, "pct_error"] = pct_error
        except Exception as e:
            tuning_results.loc[order, "error_message"] = e
    try:
        tuning_results = tuning_results.sort_values("rmse", ascending=True)
    except:
        pass
    return tuning_results
