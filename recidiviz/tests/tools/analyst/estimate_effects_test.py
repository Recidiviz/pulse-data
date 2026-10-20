# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for effects estimation methods"""

import unittest
from unittest.mock import patch

import pandas as pd
from linearmodels import PanelOLS

from recidiviz.tools.analyst.estimate_effects import (
    DidEffectResult,
    WildBootstrapWeightDistribution,
    est_did_effect,
    est_did_effect_with_inference,
    est_es_effect,
    get_panel_ols_result,
    pairwise_stratify,
    validate_df,
)

# create simulated dataframes for testing
_DUMMY_DF = pd.DataFrame(
    {
        "outcome": [1, 1, 1, 2],
        "unit_of_analysis": ["a", "a", "b", "b"],
        "unit_of_treatment": ["c", "c", "d", "d"],
        "start_date": pd.to_datetime(["2020-01-01", "2020-02-01"] * 2),
        "weights": [1] * 4,
        "other_column": ["a", "b", "c", "d"],
        "other_column_2": ["a", "b", "c", "d"],
        "excluded_column": ["a", "b", "c", "d"],
        "bad_outcome": ["a", "b", "c", "d"],
        "bad_unit_of_analysis": [1, 2, 3, 4],
        "bad_start_date": [1, 2, 3, 4],
        "cluster_column": ["x", "x", "x", "x"],
    }
)
_EXPECTED_COLUMNS = [
    "outcome",
    "unit_of_analysis",
    "unit_of_treatment",
    "start_date",
    "weights",
]
_OTHER_COLUMNS = ["other_column", "other_column_2"]

# PanelOLS raises if entity and time effects leave no residual degrees of freedom
# or if a clustered covariance has a single cluster, so this panel needs more
# observations than _DUMMY_DF and at least two clusters.
_PANEL_DF = pd.DataFrame(
    {
        "outcome": [1, 1, 2, 1, 2, 2, 2, 1, 3, 1, 1, 2],
        "unit_of_analysis": ["a"] * 3 + ["b"] * 3 + ["c"] * 3 + ["d"] * 3,
        "unit_of_treatment": ["e"] * 6 + ["f"] * 6,
        "start_date": pd.to_datetime(["2020-01-01", "2020-02-01", "2020-03-01"] * 4),
        "weights": [1] * 12,
        "cluster_column": ["x"] * 6 + ["y"] * 6,
        "controls": [1, 0, 0.5, 1, 1, 0, 0.5, 1, 1, 0, 1, 0.5],
    }
)


class TestValidateDf(unittest.TestCase):
    """Tests for validate_df() method in estimate_effects.py"""

    def test_validate_df(self) -> None:
        """Verify that validate_df() works as expected"""

        # get output of validate_df() run on dummy df
        validated_df = validate_df(
            df=_DUMMY_DF,
            outcome_column="outcome",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
            weight_column="weights",
            other_columns=_OTHER_COLUMNS,
        )

        # check that validate_df() returns a df
        self.assertIsInstance(validated_df, pd.DataFrame)

        # verify correct columns are returned
        self.assertListEqual(
            validated_df.columns.tolist(),
            _EXPECTED_COLUMNS + _OTHER_COLUMNS,
        )

        # verify correct number of rows are returned
        self.assertEqual(validated_df.shape[0], _DUMMY_DF.shape[0])

    def test_validate_df_no_weights(self) -> None:
        """Verify that validate_df() works as expected when no weights are provided"""

        # get output of validate_df() run on dummy df
        validated_df = validate_df(
            df=_DUMMY_DF,
            outcome_column="outcome",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
            other_columns=_OTHER_COLUMNS,
        )

        # verify correct columns are returned
        self.assertListEqual(
            validated_df.columns.tolist(),
            _EXPECTED_COLUMNS + _OTHER_COLUMNS,
        )

        # verify that weights are added
        self.assertListEqual(validated_df["weights"].tolist(), [1] * _DUMMY_DF.shape[0])

    def test_validate_df_no_other_columns(self) -> None:
        """Verify that validate_df() works as expected when no other columns are provided"""

        # get output of validate_df() run on dummy df
        validated_df = validate_df(
            df=_DUMMY_DF,
            outcome_column="outcome",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
        )

        # verify correct columns are returned
        self.assertListEqual(
            validated_df.columns.tolist(),
            _EXPECTED_COLUMNS,
        )

    def test_validate_df_wrong_types(self) -> None:
        """Validate that error is thrown if wrong datatypes present in key columns"""

        # non-numeric outcome column
        with self.assertRaises(TypeError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="bad_outcome",
                unit_of_analysis_column="unit_of_analysis",
                unit_of_treatment_column="unit_of_treatment",
                date_column="start_date",
            )

        # non-string unit_of_analysis column
        with self.assertRaises(TypeError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="outcome",
                unit_of_analysis_column="bad_unit_of_analysis",
                unit_of_treatment_column="unit_of_treatment",
                date_column="start_date",
            )

        # non-datetime date column
        with self.assertRaises(TypeError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="outcome",
                unit_of_analysis_column="unit_of_analysis",
                unit_of_treatment_column="unit_of_treatment",
                date_column="bad_start_date",
            )

    def test_validate_df_missing_columns(self) -> None:
        """Validate that error is thrown if missing key columns in df"""

        # missing outcome column
        with self.assertRaises(ValueError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="missing_outcome",
                unit_of_analysis_column="unit_of_analysis",
                unit_of_treatment_column="unit_of_treatment",
                date_column="start_date",
            )

        # missing unit_of_analysis column
        with self.assertRaises(ValueError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="outcome",
                unit_of_analysis_column="missing_unit_of_analysis",
                unit_of_treatment_column="unit_of_treatment",
                date_column="start_date",
            )

        # missing date column
        with self.assertRaises(ValueError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="outcome",
                unit_of_analysis_column="unit_of_analysis",
                unit_of_treatment_column="unit_of_treatment",
                date_column="missing_date",
            )

        # missing unit_of_treatment column
        with self.assertRaises(ValueError):
            validate_df(
                df=_DUMMY_DF,
                outcome_column="outcome",
                unit_of_analysis_column="unit_of_analysis",
                unit_of_treatment_column="missing_unit_of_treatment",
                date_column="start_date",
            )

    def test_validate_df_unique_columns(self) -> None:
        """Validate that error thrown if repeat columns in df"""
        with self.assertRaises(ValueError, msg="Column names in df must be unique"):
            validate_df(
                df=_DUMMY_DF.rename(
                    columns={
                        "other_column": "outcome",
                    }
                ),
                outcome_column="outcome",
                unit_of_analysis_column="unit_of_analysis",
                unit_of_treatment_column="unit_of_treatment",
                date_column="start_date",
            )

    def test_validate_df_returns_unique_columns(self) -> None:
        "Validate that validate_df returns unique columns"

        # get output of validate_df with repeat unit of analysis and treatment columns
        validated_df = validate_df(
            df=_DUMMY_DF,
            outcome_column="outcome",
            unit_of_analysis_column="unit_of_treatment",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
        )

        # verify unique columns in validated_df
        self.assertEqual(len(set(validated_df.columns)), len(validated_df.columns))


class TestEffectEstimationFunctions(unittest.TestCase):
    "Tests for effect estimation functions in estimate_effects.py"

    def test_get_panel_ols_result(self) -> None:
        "Verify that get_panel_ols_result() returns something wihtout error"

        # prep dummy df to include entity
        df = _PANEL_DF.copy().set_index(["unit_of_analysis", "start_date"])

        # init reg formula
        reg_formula = "outcome ~ 1 + EntityEffects + TimeEffects"

        # no cluster column
        get_panel_ols_result(
            reg_formula=reg_formula,
            df=df,
            weight_column="weights",
            cluster_column=None,
        )

        # with cluster column
        get_panel_ols_result(
            reg_formula=reg_formula,
            df=df,
            weight_column="weights",
            cluster_column="cluster_column",
        )

    @patch(
        "recidiviz.tools.analyst.estimate_effects.MIN_TREATED_CLUSTERS_FOR_CLUSTERED_SE",
        1,
    )
    def test_est_did_effect(self) -> None:
        """
        Verify that est_did_effect() returns something wihtout error for all
        combinations of inputs.
        """

        # The model needs residual degrees of freedom beyond the fixed effects, so
        # this uses a 6-facility, 3-date panel rather than _DUMMY_DF.
        df = _SIX_FACILITY_PANEL.rename(
            columns={"facility": "unit_of_analysis", "post": "post_treat"}
        )
        df["unit_of_treatment"] = df["unit_of_analysis"]
        df["cluster_column"] = ["north"] * 9 + ["south"] * 9

        # no optional params
        est_did_effect(
            df=df,
            outcome_column="outcome",
            interaction_column="post_treat",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
        )

        # with weights
        est_did_effect(
            df=df,
            outcome_column="outcome",
            interaction_column="post_treat",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
            weight_column="weights",
        )

        # with cluster column
        est_did_effect(
            df=df,
            outcome_column="outcome",
            interaction_column="post_treat",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
            cluster_column="cluster_column",
        )
        df["controls"] = [
            1.0, 0.0, 0.5, 1.0, 1.0, 0.0, 0.5, 0.5, 1.0,
            0.0, 1.0, 1.0, 0.5, 0.0, 1.0, 1.0, 0.5, 0.0,
        ]  # fmt: skip
        est_did_effect(
            df=df,
            outcome_column="outcome",
            interaction_column="post_treat",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
            control_columns=["controls"],
        )

    def test_est_did_effect_two_dates_matches_difference_in_differences(
        self,
    ) -> None:
        """Guards against linearmodels mistaking the interaction column for a
        constant when the panel has one more date than regressors."""
        panel = _make_facility_panel(
            [
                ("a", "2026-01-01", False, 10.0, 1.0),
                ("a", "2026-02-01", True, 13.0, 1.0),
                ("b", "2026-01-01", False, 8.0, 1.0),
                ("b", "2026-02-01", False, 8.5, 1.0),
                ("c", "2026-01-01", False, 9.0, 1.0),
                ("c", "2026-02-01", False, 10.5, 1.0),
                ("d", "2026-01-01", False, 12.0, 1.0),
                ("d", "2026-02-01", False, 12.0, 1.0),
            ]
        )

        with self.assertWarns(UserWarning):
            result = est_did_effect(
                df=panel,
                outcome_column="outcome",
                interaction_column="post",
                unit_of_analysis_column="facility",
                unit_of_treatment_column="facility",
                date_column="start_date",
                weight_column="weights",
            )

        # Facility a rose 3.0; b, c, and d rose 0.5, 1.5, and 0.0, an average of 2/3.
        self.assertAlmostEqual(3.0 - 2 / 3, result.params["post"])

    def test_est_es_effect(self) -> None:
        """
        Verify that est_es_effect() returns something without error for all
        combinations of inputs.
        """

        # prep dummy df
        df = _PANEL_DF.copy()
        df["treated"] = df.start_date == df.start_date.max()

        # no optional params
        est_es_effect(
            df=df,
            outcome_column="outcome",
            treated_column="treated",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
        )

        # with weights
        est_es_effect(
            df=df,
            outcome_column="outcome",
            treated_column="treated",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
            weight_column="weights",
        )

        # with cluster column
        est_es_effect(
            df=df,
            outcome_column="outcome",
            treated_column="treated",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
            cluster_column="cluster_column",
        )

        # with control column
        est_es_effect(
            df=df,
            outcome_column="outcome",
            treated_column="treated",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
            control_columns=["controls"],
        )


class TestPairwiseStratify(unittest.TestCase):
    """Tests for pairwise_stratify() method in estimate_effects.py"""

    def test_required_columns_present(self) -> None:
        "Verify that error thrown if required columns are not present in stratified df"

        # create dummy df
        df = _DUMMY_DF.copy()

        # verify error thrown if categorical_columns or continuous_column not provided
        with self.assertRaises(ValueError):
            pairwise_stratify(
                df=df,
                unit_of_treatment_column="unit_of_treatment",
            )

        # verify error thrown if unit_of_treatment_column not in df
        with self.assertRaises(ValueError):
            pairwise_stratify(
                df=df,
                unit_of_treatment_column="missing_unit_of_treatment",
                categorical_columns=["cluster_column"],
            )

        # verify error thrown if categorical_columns not in df
        with self.assertRaises(ValueError):
            pairwise_stratify(
                df=df,
                unit_of_treatment_column="unit_of_treatment",
                categorical_columns=["missing_categorical_column"],
            )

        # verify error thrown if continuous_column not in df
        with self.assertRaises(ValueError):
            pairwise_stratify(
                df=df,
                unit_of_treatment_column="unit_of_treatment",
                continuous_column="missing_continuous_column",
            )

        # no error thrown if all columns found
        pairwise_stratify(
            df=df,
            unit_of_treatment_column="unit_of_treatment",
            categorical_columns=["cluster_column"],
            continuous_column="weights",
        )

    def test_treated_column(self) -> None:
        """
        Verify that treated column present in returned stratified df and correctly
        valued.
        """

        # create dummy df
        df = pd.DataFrame(
            {
                "unit_of_treatment": ["a", "a", "a", "a"],
                "categorical_column": ["x", "x", "y", "y"],
                "continuous_column": [1, 98, 2, 99],
            }
        )

        # verify treated column present in stratified df
        stratified_df = pairwise_stratify(
            df=df,
            unit_of_treatment_column="unit_of_treatment",
            categorical_columns=["categorical_column"],
            continuous_column="continuous_column",
        )
        self.assertIn("treated", stratified_df.columns)
        for v in ["group_id", "pair_id"]:
            self.assertNotIn(v, stratified_df.columns)

        # verify group_id and pair_id returned if keep_block_and_pair_ids=True
        stratified_df = pairwise_stratify(
            df=df,
            unit_of_treatment_column="unit_of_treatment",
            categorical_columns=["categorical_column"],
            continuous_column="continuous_column",
            keep_block_and_pair_ids=True,
        )
        for v in ["treated", "group_id", "pair_id"]:
            self.assertIn(v, stratified_df.columns)

        # binary values only
        self.assertTrue(
            stratified_df.treated.isin([0, 1]).all(),
            "Treated column should only contain binary values.",
        )

        # equal number of treated = 0 or 1 for even number of units
        self.assertEqual(
            stratified_df.treated.value_counts()[0],
            stratified_df.treated.value_counts()[1],
            "Treated column should have equal number of 0s and 1s.",
        )

        # in the current case, the first and last two units should be paired,
        # that is, have the same paid_id and group_id
        for v in ["group_id", "pair_id"]:
            self.assertEqual(
                stratified_df.loc[0, v],
                stratified_df.loc[1, v],
                "First two units should be in the same group.",
            )
            self.assertEqual(
                stratified_df.loc[2, v],
                stratified_df.loc[3, v],
                "Last two units should be in the same group.",
            )

        # new case with contiuous column only
        # in this case, the first and third unit should be paired
        stratified_df = pairwise_stratify(
            df=df,
            unit_of_treatment_column="unit_of_treatment",
            continuous_column="continuous_column",
            keep_block_and_pair_ids=True,
        )
        for v in ["group_id", "pair_id"]:
            self.assertEqual(
                stratified_df.loc[0, v],
                stratified_df.loc[2, v],
                "First and third units should be in the same group.",
            )
            self.assertEqual(
                stratified_df.loc[1, v],
                stratified_df.loc[3, v],
                "Second and fourth units should be in the same group.",
            )


def _make_facility_panel(
    rows: list[tuple[str, str, bool, float, float]],
) -> pd.DataFrame:
    panel = pd.DataFrame.from_records(
        rows, columns=["facility", "start_date", "post", "outcome", "weights"]
    )
    panel["start_date"] = pd.to_datetime(panel["start_date"])
    return panel


# Facilities a and b launch on 2026-03-01.
_SIX_FACILITY_PANEL = _make_facility_panel(
    [
        ("a", "2026-01-01", False, 10.0, 20.0),
        ("a", "2026-02-01", False, 11.0, 22.0),
        ("a", "2026-03-01", True, 15.5, 21.0),
        ("b", "2026-01-01", False, 8.0, 35.0),
        ("b", "2026-02-01", False, 8.5, 33.0),
        ("b", "2026-03-01", True, 12.0, 36.0),
        ("c", "2026-01-01", False, 9.0, 10.0),
        ("c", "2026-02-01", False, 9.5, 12.0),
        ("c", "2026-03-01", False, 10.5, 11.0),
        ("d", "2026-01-01", False, 12.0, 25.0),
        ("d", "2026-02-01", False, 12.0, 24.0),
        ("d", "2026-03-01", False, 13.5, 26.0),
        ("e", "2026-01-01", False, 7.0, 15.0),
        ("e", "2026-02-01", False, 8.0, 14.0),
        ("e", "2026-03-01", False, 8.0, 16.0),
        ("f", "2026-01-01", False, 11.0, 30.0),
        ("f", "2026-02-01", False, 11.5, 31.0),
        ("f", "2026-03-01", False, 13.0, 29.0),
    ]
)

# Facility a launches on 2026-02-01 and facility b on 2026-03-01.
_STAGGERED_FOUR_FACILITY_PANEL = _make_facility_panel(
    [
        ("a", "2026-01-01", False, 10.0, 1.0),
        ("a", "2026-02-01", True, 13.0, 1.0),
        ("a", "2026-03-01", True, 14.5, 1.0),
        ("b", "2026-01-01", False, 8.0, 1.0),
        ("b", "2026-02-01", False, 8.5, 1.0),
        ("b", "2026-03-01", True, 11.0, 1.0),
        ("c", "2026-01-01", False, 9.0, 1.0),
        ("c", "2026-02-01", False, 9.5, 1.0),
        ("c", "2026-03-01", False, 10.5, 1.0),
        ("d", "2026-01-01", False, 12.0, 1.0),
        ("d", "2026-02-01", False, 12.0, 1.0),
        ("d", "2026-03-01", False, 13.5, 1.0),
    ]
)

# Facilities g01, g02, and g03 launch on 2026-03-01.
_TWELVE_FACILITY_PANEL = _make_facility_panel(
    [
        ("g01", "2026-01-01", False, 10.0, 1.0),
        ("g01", "2026-02-01", False, 10.5, 1.0),
        ("g01", "2026-03-01", True, 13.0, 1.0),
        ("g02", "2026-01-01", False, 8.0, 1.0),
        ("g02", "2026-02-01", False, 8.0, 1.0),
        ("g02", "2026-03-01", True, 10.5, 1.0),
        ("g03", "2026-01-01", False, 9.0, 1.0),
        ("g03", "2026-02-01", False, 9.5, 1.0),
        ("g03", "2026-03-01", True, 11.0, 1.0),
        ("g04", "2026-01-01", False, 12.0, 1.0),
        ("g04", "2026-02-01", False, 12.0, 1.0),
        ("g04", "2026-03-01", False, 12.5, 1.0),
        ("g05", "2026-01-01", False, 7.0, 1.0),
        ("g05", "2026-02-01", False, 7.5, 1.0),
        ("g05", "2026-03-01", False, 8.0, 1.0),
        ("g06", "2026-01-01", False, 11.0, 1.0),
        ("g06", "2026-02-01", False, 11.5, 1.0),
        ("g06", "2026-03-01", False, 11.0, 1.0),
        ("g07", "2026-01-01", False, 9.5, 1.0),
        ("g07", "2026-02-01", False, 9.5, 1.0),
        ("g07", "2026-03-01", False, 10.5, 1.0),
        ("g08", "2026-01-01", False, 10.5, 1.0),
        ("g08", "2026-02-01", False, 11.0, 1.0),
        ("g08", "2026-03-01", False, 10.0, 1.0),
        ("g09", "2026-01-01", False, 8.5, 1.0),
        ("g09", "2026-02-01", False, 8.5, 1.0),
        ("g09", "2026-03-01", False, 9.5, 1.0),
        ("g10", "2026-01-01", False, 13.0, 1.0),
        ("g10", "2026-02-01", False, 13.5, 1.0),
        ("g10", "2026-03-01", False, 13.5, 1.0),
        ("g11", "2026-01-01", False, 6.0, 1.0),
        ("g11", "2026-02-01", False, 6.5, 1.0),
        ("g11", "2026-03-01", False, 7.5, 1.0),
        ("g12", "2026-01-01", False, 11.5, 1.0),
        ("g12", "2026-02-01", False, 11.5, 1.0),
        ("g12", "2026-03-01", False, 12.0, 1.0),
    ]
)


# Three residents in each of six facilities; facilities a and b launch on
# 2026-02-01.
_RESIDENT_PANEL = pd.DataFrame.from_records(
    [
        ("a1", "a", "2026-01-01", False, 5.0),
        ("a1", "a", "2026-02-01", True, 8.0),
        ("a2", "a", "2026-01-01", False, 6.0),
        ("a2", "a", "2026-02-01", True, 10.0),
        ("a3", "a", "2026-01-01", False, 7.0),
        ("a3", "a", "2026-02-01", True, 9.5),
        ("b1", "b", "2026-01-01", False, 5.0),
        ("b1", "b", "2026-02-01", True, 7.0),
        ("b2", "b", "2026-01-01", False, 6.0),
        ("b2", "b", "2026-02-01", True, 9.5),
        ("b3", "b", "2026-01-01", False, 7.0),
        ("b3", "b", "2026-02-01", True, 10.0),
        ("c1", "c", "2026-01-01", False, 5.0),
        ("c1", "c", "2026-02-01", False, 5.5),
        ("c2", "c", "2026-01-01", False, 6.0),
        ("c2", "c", "2026-02-01", False, 7.0),
        ("c3", "c", "2026-01-01", False, 7.0),
        ("c3", "c", "2026-02-01", False, 7.0),
        ("d1", "d", "2026-01-01", False, 5.0),
        ("d1", "d", "2026-02-01", False, 6.5),
        ("d2", "d", "2026-01-01", False, 6.0),
        ("d2", "d", "2026-02-01", False, 6.5),
        ("d3", "d", "2026-01-01", False, 7.0),
        ("d3", "d", "2026-02-01", False, 8.0),
        ("e1", "e", "2026-01-01", False, 5.0),
        ("e1", "e", "2026-02-01", False, 5.0),
        ("e2", "e", "2026-01-01", False, 6.0),
        ("e2", "e", "2026-02-01", False, 7.0),
        ("e3", "e", "2026-01-01", False, 7.0),
        ("e3", "e", "2026-02-01", False, 6.5),
        ("f1", "f", "2026-01-01", False, 5.0),
        ("f1", "f", "2026-02-01", False, 6.0),
        ("f2", "f", "2026-01-01", False, 6.0),
        ("f2", "f", "2026-02-01", False, 8.0),
        ("f3", "f", "2026-01-01", False, 7.0),
        ("f3", "f", "2026-02-01", False, 7.5),
    ],
    columns=["resident", "facility", "start_date", "post", "outcome"],
).astype({"start_date": "datetime64[ns]"})


class TestEstDidEffectWithInference(unittest.TestCase):
    """Tests for est_did_effect_with_inference and the few-cluster warning in
    est_did_effect."""

    @staticmethod
    def _estimate(
        panel: pd.DataFrame,
        *,
        n_bootstrap_draws: int,
        max_randomization_assignments: int = 10_000,
        seed: int = 0,
    ) -> DidEffectResult:
        return est_did_effect_with_inference(
            panel.copy(),
            outcome_column="outcome",
            interaction_column="post",
            unit_of_analysis_column="facility",
            unit_of_treatment_column="facility",
            date_column="start_date",
            weight_column="weights",
            control_columns=None,
            n_bootstrap_draws=n_bootstrap_draws,
            max_randomization_assignments=max_randomization_assignments,
            seed=seed,
        )

    def test_counts_treated_and_total_clusters(self) -> None:
        result = self._estimate(_SIX_FACILITY_PANEL, n_bootstrap_draws=99)

        self.assertEqual(2, result.n_treated_clusters)
        self.assertEqual(6, result.n_clusters)

    def test_resident_level_panel_clusters_on_facility(self) -> None:
        result = est_did_effect_with_inference(
            _RESIDENT_PANEL.copy(),
            outcome_column="outcome",
            interaction_column="post",
            unit_of_analysis_column="resident",
            unit_of_treatment_column="facility",
            date_column="start_date",
            weight_column=None,
            control_columns=None,
            n_bootstrap_draws=99,
            seed=0,
        )

        self.assertEqual(2, result.n_treated_clusters)
        self.assertEqual(6, result.n_clusters)
        # Residents of a and b rose 18 / 6 = 3.0 on average; residents of c through
        # f rose 8.5 / 12.
        self.assertAlmostEqual(3.0 - 8.5 / 12, result.panel_ols_result.params["post"])

        panel = _RESIDENT_PANEL.astype({"post": int}).set_index(
            ["resident", "start_date"]
        )
        facility_clustered_fit = PanelOLS.from_formula(
            "outcome ~ -1 + post + EntityEffects + TimeEffects", data=panel
        ).fit(cov_type="clustered", clusters=panel["facility"])
        self.assertAlmostEqual(
            facility_clustered_fit.std_errors["post"],
            result.panel_ols_result.std_errors["post"],
        )

        # Randomization inference reassigns treatment across the 15 pairs of
        # facilities, not across residents.
        inference = result.randomization_inference
        self.assertTrue(inference.is_exhaustive)
        self.assertEqual(15, inference.n_assignments)
        self.assertAlmostEqual(
            facility_clustered_fit.tstats["post"], inference.observed_t_statistic
        )
        self.assertAlmostEqual(1 / 15, inference.p_value)
        self.assertAlmostEqual(1 / 99, result.wild_cluster_bootstrap.p_value)

    def test_randomization_inference_enumerates_every_pair_of_facilities(
        self,
    ) -> None:
        result = self._estimate(_SIX_FACILITY_PANEL, n_bootstrap_draws=99)

        # 15 ways to pick 2 of 6 facilities; only the real pair has a t-statistic
        # as extreme as the observed one.
        inference = result.randomization_inference
        self.assertTrue(inference.is_exhaustive)
        self.assertEqual(15, inference.n_assignments)
        self.assertAlmostEqual(4.333236141015302, inference.observed_t_statistic)
        self.assertAlmostEqual(
            result.panel_ols_result.tstats["post"], inference.observed_t_statistic
        )
        self.assertAlmostEqual(1 / 15, inference.p_value)

    def test_randomization_inference_samples_above_max_assignments(self) -> None:
        result = self._estimate(
            _SIX_FACILITY_PANEL, n_bootstrap_draws=99, max_randomization_assignments=5
        )

        inference = result.randomization_inference
        self.assertFalse(inference.is_exhaustive)
        self.assertEqual(5, inference.n_assignments)
        # None of the 5 sampled assignments is as extreme as the real one, so the
        # p-value counts only the real assignment: (0 + 1) / (5 + 1).
        self.assertAlmostEqual(1 / 6, inference.p_value)

    def test_randomization_inference_keeps_each_staggered_adoption_date(
        self,
    ) -> None:
        result = self._estimate(_STAGGERED_FOUR_FACILITY_PANEL, n_bootstrap_draws=99)

        # 12 ways to give one facility the 2026-02-01 date, another the 2026-03-01
        # date, and leave two untreated.
        inference = result.randomization_inference
        self.assertTrue(inference.is_exhaustive)
        self.assertEqual(12, inference.n_assignments)
        self.assertAlmostEqual(1 / 12, inference.p_value)

    def test_wild_bootstrap_uses_webb_weights_with_few_clusters(self) -> None:
        result = self._estimate(_SIX_FACILITY_PANEL, n_bootstrap_draws=99)

        bootstrap = result.wild_cluster_bootstrap
        self.assertEqual(
            WildBootstrapWeightDistribution.WEBB, bootstrap.weight_distribution
        )
        self.assertEqual(99, bootstrap.n_draws)
        self.assertFalse(bootstrap.is_exhaustive)
        self.assertAlmostEqual(2 / 99, bootstrap.p_value)
        self.assertAlmostEqual(1.990627611127581, bootstrap.confidence_interval_lower)
        self.assertAlmostEqual(4.3156310748709, bootstrap.confidence_interval_upper)

    def test_wild_bootstrap_enumerates_rademacher_weights_when_draws_allow(
        self,
    ) -> None:
        result = self._estimate(_TWELVE_FACILITY_PANEL, n_bootstrap_draws=4096)

        # 2 ** 12 = 4096 possible Rademacher draws, so every one is used.
        bootstrap = result.wild_cluster_bootstrap
        self.assertEqual(
            WildBootstrapWeightDistribution.RADEMACHER, bootstrap.weight_distribution
        )
        self.assertEqual(4096, bootstrap.n_draws)
        self.assertTrue(bootstrap.is_exhaustive)
        self.assertAlmostEqual(28 / 4096, bootstrap.p_value)

    def test_inference_summary(self) -> None:
        result = self._estimate(_SIX_FACILITY_PANEL, n_bootstrap_draws=99)

        expected = """                        Few-Cluster Inference for post
==============================================================================
Treated units of treatment:         2 of 6
Coefficient:                        2.8448
Clustered p-value:                  0.0019
Wild cluster bootstrap p-value:     0.0202 (99 random Webb draws)
Wild cluster bootstrap 95% CI:      1.9906 to 4.3156
Randomization inference p-value:    0.0667 (all 15 assignments)
=============================================================================="""
        self.assertEqual(expected, result.inference_summary)

    def test_same_seed_gives_same_p_values(self) -> None:
        first = self._estimate(
            _SIX_FACILITY_PANEL, n_bootstrap_draws=99, max_randomization_assignments=5
        )
        second = self._estimate(
            _SIX_FACILITY_PANEL, n_bootstrap_draws=99, max_randomization_assignments=5
        )

        self.assertEqual(
            first.wild_cluster_bootstrap.p_value, second.wild_cluster_bootstrap.p_value
        )
        self.assertEqual(
            first.randomization_inference.p_value,
            second.randomization_inference.p_value,
        )

    def test_rejects_interaction_column_that_turns_off(self) -> None:
        panel = _make_facility_panel(
            [
                ("a", "2026-01-01", False, 10.0, 1.0),
                ("a", "2026-02-01", True, 13.0, 1.0),
                ("a", "2026-03-01", False, 14.5, 1.0),
                ("b", "2026-01-01", False, 8.0, 1.0),
                ("b", "2026-02-01", False, 8.5, 1.0),
                ("b", "2026-03-01", False, 11.0, 1.0),
                ("c", "2026-01-01", False, 9.0, 1.0),
                ("c", "2026-02-01", False, 9.5, 1.0),
                ("c", "2026-03-01", False, 10.5, 1.0),
            ]
        )

        with self.assertRaisesRegex(
            ValueError, r"Units of treatment \[a\] have an interaction column"
        ):
            self._estimate(panel, n_bootstrap_draws=99)

    def test_est_did_effect_warns_with_few_treated_clusters(self) -> None:
        with self.assertWarnsRegex(
            UserWarning, r"Only \[2\] units of treatment are treated"
        ):
            est_did_effect(
                df=_SIX_FACILITY_PANEL.copy(),
                outcome_column="outcome",
                interaction_column="post",
                unit_of_analysis_column="facility",
                unit_of_treatment_column="facility",
                date_column="start_date",
                weight_column="weights",
            )

    @patch(
        "recidiviz.tools.analyst.estimate_effects.MIN_TREATED_CLUSTERS_FOR_CLUSTERED_SE",
        2,
    )
    def test_est_did_effect_does_not_warn_at_threshold(self) -> None:
        # Pytest turns warnings from recidiviz modules into errors, so a warning
        # here fails the test.
        est_did_effect(
            df=_SIX_FACILITY_PANEL.copy(),
            outcome_column="outcome",
            interaction_column="post",
            unit_of_analysis_column="facility",
            unit_of_treatment_column="facility",
            date_column="start_date",
            weight_column="weights",
        )
