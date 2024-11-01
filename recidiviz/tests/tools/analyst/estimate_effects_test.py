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

import pandas as pd

from recidiviz.tools.analyst.estimate_effects import (
    est_did_effect,
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
        df = _DUMMY_DF.copy().set_index(["unit_of_analysis", "start_date"])

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

    def test_est_did_effect(self) -> None:
        """
        Verify that est_did_effect() returns something wihtout error for all
        combinations of inputs.
        """

        # prep dummy df
        df = _DUMMY_DF.copy()
        df["post_treat"] = False
        df.loc[
            (df.unit_of_analysis == "a") & (df.start_date == df.start_date.max()),
            "post_treat",
        ] = True

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

        # with control column
        # need more observations because adding control column makes the matrix
        # indeterminate (cannot invert)
        df = pd.concat([df, df], axis=0)
        df["unit_of_analysis"] = ["a", "a", "b", "b", "c", "c", "d", "d"]
        df["controls"] = [1, 0, 0.5, 1, 1, 1, 1, 1]
        est_did_effect(
            df=df,
            outcome_column="outcome",
            interaction_column="post_treat",
            unit_of_analysis_column="unit_of_analysis",
            unit_of_treatment_column="unit_of_treatment",
            date_column="start_date",
            control_columns=["controls"],
        )

    def test_est_es_effect(self) -> None:
        """
        Verify that est_es_effect() returns something without error for all
        combinations of inputs.
        """

        # prep dummy df
        df = _DUMMY_DF.copy()
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
        # need more observations because adding control column makes the matrix
        # indeterminate (cannot invert)
        df = pd.concat([df, df], axis=0)
        df["unit_of_analysis"] = ["a", "a", "b", "b", "c", "c", "d", "d"]
        df["controls"] = [1, 0, 0.5, 1, 1, 1, 1, 1]
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
