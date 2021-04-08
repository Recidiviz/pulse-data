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
"""SuperSimulation composed object for validating simulation results."""
from typing import Dict, List, Optional, Tuple, Union
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from recidiviz.calculator.modeling.population_projection.population_simulation.population_simulation import (
    PopulationSimulation,
)
from recidiviz.calculator.modeling.population_projection.super_simulation.time_converter import (
    TimeConverter,
)
from recidiviz.calculator.modeling.population_projection.predicted_admissions import (
    ProjectionType,
)


class Validator:
    """Manage validations and analyses on SuperSimulation outputs."""

    def __init__(self, microsim: bool, time_converter: TimeConverter) -> None:
        self.microsim = microsim
        self.time_converter = time_converter
        self.output_data: Dict[str, pd.DataFrame] = dict()
        self.pop_simulations: Dict[str, PopulationSimulation] = dict()

    def reset(
        self,
        pop_simulations: Dict[str, PopulationSimulation],
        output_data: Optional[Dict[str, pd.DataFrame]] = None,
    ) -> None:
        if output_data:
            self.output_data = output_data
        else:
            self.output_data = dict()
        self.pop_simulations = pop_simulations

    def calculate_baseline_transition_error(
        self, validation_pairs: Dict[str, str]
    ) -> pd.DataFrame:
        """Calculates baseline transition error."""
        self.output_data["baseline_transition_error"] = pd.DataFrame(
            columns=["compartment", "outflow", "subgroup", "year", "error"]
        )
        for compartment, outflow_to in validation_pairs.items():
            for sub_group in self.pop_simulations["baseline_middle"].sub_simulations:
                error = pd.DataFrame(
                    self.pop_simulations["baseline_middle"]
                    .sub_simulations[sub_group]
                    .get_error(compartment)[outflow_to]
                ).reset_index()
                error = error.rename({"time_step": "year", outflow_to: "error"}, axis=1)
                error["outflow"] = outflow_to
                error["compartment"] = compartment
                error["subgroup"] = sub_group

                self.output_data["baseline_transition_error"] = pd.concat(
                    [self.output_data["baseline_transition_error"], error]
                )

        self.output_data[
            "baseline_transition_error"
        ].year = self.time_converter.convert_time_steps_to_year(
            self.output_data["baseline_transition_error"].year
        )

        self.output_data["baseline_population_error"] = self.pop_simulations[
            "baseline_middle"
        ].gen_scale_factors_df()
        return self.output_data["baseline_population_error"]

    def gen_arima_output_df(self, simulation_title: str) -> pd.DataFrame:
        return self.pop_simulations[simulation_title].gen_arima_output_df()

    def gen_arima_output_plots(
        self,
        simulation_title: str,
        fig_size: Tuple[int, int],
        by_simulation_group: bool,
    ) -> List[plt.subplot]:
        """
        Generates admissions forecast plots broken up by compartment and outflow.
        `simulation_title` should be the tag of the PopulationSimulation of interest
        If by_simulation_group = False, plots are generated for each shell compartment and outflow_to compartment
        If by_simulation_group = True these are further subdivided by simulation group
        """
        arima_output_df = self.gen_arima_output_df(simulation_title)
        if not by_simulation_group:
            arima_output_df = arima_output_df.groupby(
                level=["compartment", "outflow_to", "time_step"]
            ).apply(lambda x: x.sum(skipna=False))
        levels_to_plot = [x for x in arima_output_df.index.names if x != "time_step"]
        dfs_to_plot = arima_output_df.groupby(levels_to_plot)

        axes = []
        for i, df_to_plot in dfs_to_plot:
            _, ax = plt.subplots(figsize=fig_size)
            sub_plot = df_to_plot.reset_index()
            sub_plot.index = self.time_converter.convert_time_steps_to_year(
                pd.Series(sub_plot["time_step"])
            )
            sub_plot["actuals"].plot(
                ax=ax, color="tab:cyan", marker="o", label="Actuals"
            )
            sub_plot[ProjectionType.MIDDLE.value].plot(
                ax=ax, color="tab:red", marker="o", label="Predictions"
            )

            ax.fill_between(
                sub_plot.index,
                sub_plot[ProjectionType.LOW.value],
                sub_plot[ProjectionType.HIGH.value],
                alpha=0.4,
                color="orange",
            )

            plt.ylim(
                bottom=0,
                top=max(
                    [sub_plot[ProjectionType.HIGH.value].max(), sub_plot.actuals.max()]
                )
                * 1.1,
            )
            plt.legend(loc="lower left")
            plt.title("\n".join([": ".join(z) for z in zip(levels_to_plot, i)]))
            axes.append(ax)
        return axes

    def calculate_outflows_error(
        self, simulation_title: str, outflows_data: pd.DataFrame
    ) -> pd.DataFrame:
        raw_outflows = self.gen_arima_output_df(simulation_title).groupby(
            ["compartment", "outflow_to", "time_step"]
        )

        outflows = pd.DataFrame()
        outflows["model"] = raw_outflows[ProjectionType.MIDDLE.value].sum()
        outflows["actual"] = outflows_data.groupby(
            ["compartment", "outflow_to", "time_step"]
        ).total_population.sum()

        return outflows.fillna(0)

    def gen_total_population_error(self, simulation_tag: str) -> pd.DataFrame:
        # Convert the index from relative time steps to floating point years
        error_results = self.pop_simulations[
            simulation_tag
        ].gen_total_population_error()
        error_results.index = self.time_converter.convert_time_steps_to_year(
            pd.Series(error_results.index)
        )
        return error_results

    def gen_full_error_output(self, simulation_tag: str) -> pd.DataFrame:
        error_results = self.pop_simulations[simulation_tag].gen_full_error()
        # Convert the index from relative time steps to floating point years
        error_results.index = error_results.index.set_levels(
            self.time_converter.convert_time_steps_to_year(
                pd.Series(error_results.index.levels[1])
            ),
            level=1,
        )
        error_results["compartment_type"] = [
            x.split()[0] for x in error_results.index.get_level_values(0)
        ]
        return error_results

    def calculate_cohort_hydration_error(
        self,
        output_compartment: str,
        outflow_to: str,
        range_start: int,
        range_end: int,
        step_size: float,
        unit: str,
    ) -> pd.DataFrame:
        """
        `output_compartment` is the compartment whose error you want to get
        `outflow_to` is the outflow from that compartment you want to get the error on
        `unit is either mse or abs`
        """
        cohort_population_error = pd.DataFrame()
        for test_sim in self.pop_simulations:
            errors = pd.Series(dtype=float)
            for sub_group in self.pop_simulations[test_sim].sub_simulations:
                sub_group_sim = self.pop_simulations[test_sim].sub_simulations[
                    sub_group
                ]
                errors[sub_group] = (
                    sub_group_sim.get_error(output_compartment, unit=unit)[outflow_to]
                    .abs()
                    .mean()
                )
            cohort_population_error[test_sim] = errors

        cohort_population_error = cohort_population_error.transpose()
        cohort_population_error.index = np.arange(range_start, range_end, step_size)
        # skip first step because can't calculate ts-over-ts error differential from previous ts
        error_differential = pd.DataFrame(
            index=np.arange(range_start + step_size, range_end, step_size),
            columns=cohort_population_error.columns,
        )
        # compute the ts-over-ts error differential
        for ts in range(len(error_differential.index)):
            for sub_group in error_differential.columns:
                error_differential.iloc[ts][sub_group] = (
                    cohort_population_error.iloc[ts + 1][sub_group]
                    - cohort_population_error.iloc[ts][sub_group]
                )

        error_differential.plot(
            ylabel=f"time_step-over-time_step differential in {unit}",
            xlabel="number of max_sentences of back-filling",
            title=f"error in releases from {output_compartment}",
        )

        return cohort_population_error

    def get_output_data_for_upload(
        self,
    ) -> Dict[str, Union[PopulationSimulation, pd.DataFrame]]:
        if self.microsim:
            return {
                simulation_title: self.pop_simulations[simulation_title]
                .get_population_projections()
                .sort_values("time_step")
                for simulation_title in self.pop_simulations
            }
        return self.output_data
