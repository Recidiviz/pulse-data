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
"""SuperSimulation composed object for outputting simulation results."""
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd
from recidiviz.calculator.modeling.population_projection.utils import (
    ignite_bq_utils,
    spark_bq_utils,
)
from recidiviz.calculator.modeling.population_projection.simulations.super_simulation.initializer import (
    Initializer,
)


class Exporter:
    """Manage model exports from SuperSimulation."""

    def __init__(
        self, microsim: bool, compartment_costs: Dict[str, float], simulation_tag: str
    ):
        self.microsim = microsim
        self.compartment_costs = compartment_costs
        self.simulation_tag = simulation_tag

    def upload_simulation_results_to_bq(
        self,
        project_id: str,
        simulation_tag: Optional[str],
        output_data: Dict[str, pd.DataFrame],
        excluded_pop: pd.DataFrame,
        total_pop: pd.DataFrame,
        cost_multipliers: pd.DataFrame,
        sub_group_ids_dict: Dict[str, Dict[str, Any]],
        time_step: float,
        reference_year: float,
        disaggregation_axes: List[str],
    ) -> Optional[Dict[str, pd.DataFrame]]:
        """Format then upload simulation results to Big Query."""
        # TODO(#5607): split this method into a baseline version and a policy version
        if self.microsim:
            required_keys = ["baseline_middle", "baseline_min", "baseline_max"]
            missing_keys = [
                key for key in required_keys if key not in output_data.keys()
            ]
            if len(missing_keys) != 0:
                raise ValueError(
                    f"Microsim output data is missing the required columns {missing_keys}"
                )

            join_cols = ["time_step", "compartment", "simulation_group"]
            microsim_data = (
                output_data["baseline_middle"]
                .merge(output_data["baseline_min"], on=join_cols, suffixes=["", "_min"])
                .merge(output_data["baseline_max"], on=join_cols, suffixes=["", "_max"])
            )

            microsim_data["year"] = Initializer.convert_to_absolute_year(
                time_step, reference_year, microsim_data["time_step"]
            )
            microsim_data = microsim_data.drop("time_step", axis=1)
            if not excluded_pop.empty:
                microsim_data = self._prep_for_upload(
                    microsim_data, excluded_pop, total_pop
                )
            ignite_bq_utils.upload_ignite_results(
                project_id, microsim_data, self.simulation_tag
            )
            return {"microsim_data": microsim_data}

        (
            spending_diff,
            compartment_life_years_diff,
            spending_diff_non_cumulative,
        ) = self._get_output_metrics(
            output_data["policy_simulation"],
            sub_group_ids_dict,
            time_step,
            disaggregation_axes,
            cost_multipliers,
        )
        aggregate_output_data = (
            output_data["policy_simulation"]
            .reset_index(drop=True)
            .groupby(["year", "compartment"])
            .sum()
            .reset_index()
        )
        aggregate_output_data.index = aggregate_output_data.year
        spark_bq_utils.upload_spark_results(
            project_id,
            simulation_tag if simulation_tag else self.simulation_tag,
            spending_diff,
            compartment_life_years_diff,
            aggregate_output_data,
            spending_diff_non_cumulative,
        )
        return {
            "spending_diff": spending_diff,
            "compartment_life_years_diff": compartment_life_years_diff,
            "spending_diff_non_cumulative": spending_diff_non_cumulative,
        }

    @classmethod
    def _prep_for_upload(
        cls,
        projection_data: pd.DataFrame,
        excluded_pop: pd.DataFrame,
        total_pop: pd.DataFrame,
    ) -> pd.DataFrame:
        """function for scaling and any other state-specific operations required pre-upload"""

        scalar_dict = cls._calculate_prep_scale_factor(excluded_pop, total_pop)
        print(scalar_dict)

        output_data = projection_data.copy()
        output_data["scale_factor"] = output_data.compartment.map(scalar_dict).fillna(1)

        error_scale_factors = output_data[output_data["scale_factor"] == 0]
        if not error_scale_factors.empty:
            raise ValueError(
                f"The scale factor cannot be 0 for the population scaling: {error_scale_factors}"
            )

        scaling_columns = [
            "total_population",
            "total_population_min",
            "total_population_max",
        ]
        output_data.loc[:, scaling_columns] = output_data.loc[
            :, scaling_columns
        ].multiply(output_data.loc[:, "scale_factor"], axis="index")
        output_data = output_data.drop("scale_factor", axis=1)

        return output_data

    @classmethod
    def _calculate_prep_scale_factor(
        cls,
        excluded_pop: pd.DataFrame,
        total_pop: pd.DataFrame,
    ) -> Dict[str, float]:
        """Compute the scale factor per compartment by calculating the fraction of the initial total population
        that should have been excluded.
        """
        # Make sure there is only one row per compartment
        if excluded_pop["compartment"].nunique() != len(excluded_pop["compartment"]):
            raise ValueError(
                f"Excluded population has duplicate rows for compartments: {excluded_pop}"
            )

        scale_factors = dict()
        for _index, row in excluded_pop:
            compartment_total_pop = total_pop[
                total_pop.compartment == row.compartment
            ].total_population.sum()
            scale_factors[row.compartment] = (
                1 - row.total_population / compartment_total_pop
            )

        return scale_factors

    def _get_output_metrics(
        self,
        formatted_simulation_results: pd.DataFrame,
        sub_group_ids_dict: Dict[str, Dict[str, Any]],
        time_step: float,
        disaggregation_axes: List[str],
        cost_multipliers: pd.DataFrame,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Generates savings and life-years saved; helper function for simulate_policy()
        `cost_multipliers` should be a df of how to scale the per_year_cost for each subgroup
        """
        compartment_life_years_diff = pd.DataFrame()
        spending_diff_non_cumulative = pd.DataFrame()
        spending_diff = pd.DataFrame()

        cost_multipliers = self._get_complete_cost_multipliers(
            cost_multipliers, sub_group_ids_dict, disaggregation_axes
        )

        # go through and calculate differences for each subgroup
        for subgroup_tag, subgroup_dict in sub_group_ids_dict.items():
            subgroup_data = formatted_simulation_results[
                formatted_simulation_results["simulation_group"] == subgroup_tag
            ]

            subgroup_life_years_diff = pd.DataFrame(
                index=subgroup_data.year.unique(),
                columns=subgroup_data.compartment.unique(),
            )

            for compartment_name, compartment_data in subgroup_data.groupby(
                "compartment"
            ):
                subgroup_life_years_diff.loc[
                    compartment_data.year, compartment_name
                ] = (
                    compartment_data["control_total_population"]
                    - compartment_data["policy_total_population"]
                ) * time_step

            subgroup_spending_diff_non_cumulative = (
                subgroup_life_years_diff.copy() / time_step
            )
            subgroup_life_years_diff = subgroup_life_years_diff.cumsum()
            subgroup_spending_diff = subgroup_life_years_diff.copy()

            # pull out cost multiplier for this subgroup
            multiplier = (
                cost_multipliers[
                    (
                        cost_multipliers[disaggregation_axes]
                        == pd.Series(subgroup_dict)
                    ).all(axis=1)
                ]
                .iloc[0]
                .multiplier
            )

            for compartment, cost in self.compartment_costs.items():
                subgroup_spending_diff[compartment] *= cost * multiplier
                subgroup_spending_diff_non_cumulative[compartment] *= cost * multiplier

            # add subgroup outputs to total outputs
            compartment_life_years_diff = compartment_life_years_diff.add(
                subgroup_life_years_diff, fill_value=0
            )
            spending_diff_non_cumulative = spending_diff_non_cumulative.add(
                subgroup_spending_diff_non_cumulative, fill_value=0
            )
            spending_diff = spending_diff.add(subgroup_spending_diff, fill_value=0)

        spending_diff.index.name = "year"
        compartment_life_years_diff.index.name = "year"
        spending_diff_non_cumulative.index.name = "year"

        return spending_diff, compartment_life_years_diff, spending_diff_non_cumulative

    @classmethod
    def _get_complete_cost_multipliers(
        cls,
        cost_multipliers: pd.DataFrame,
        sub_group_ids_dict: Dict[str, Dict[str, Any]],
        disaggregation_axes: List[str],
    ) -> pd.DataFrame:
        """Gets the complete cost multipliers."""
        if cost_multipliers.empty:
            cost_multipliers = pd.DataFrame(
                columns=disaggregation_axes + ["multiplier"]
            )

        missing_disaggregation_axes = [
            axis for axis in disaggregation_axes if axis not in cost_multipliers
        ]
        if len(missing_disaggregation_axes) > 0:
            raise ValueError(
                f"Cost multipliers df missing disaggregation axes: {missing_disaggregation_axes}"
            )

        # fill in missing subgroups with identity multiplier = 1
        for subgroup_dict in sub_group_ids_dict.values():
            if cost_multipliers[
                (cost_multipliers[disaggregation_axes] == pd.Series(subgroup_dict)).all(
                    axis=1
                )
            ].empty:
                cost_multipliers = cost_multipliers.append(
                    {**subgroup_dict, **{"multiplier": 1}}, ignore_index=True
                )

        return cost_multipliers
