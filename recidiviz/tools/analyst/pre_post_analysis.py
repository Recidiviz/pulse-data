# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Shared helpers for pre/post launch impact analysis notebooks."""

from datetime import date

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from scipy import stats

from recidiviz.tools.analyst.plots import RECIDIVIZ_COLORS


def get_incarceration_super_sessions_and_days_in_window(
    *,
    state_code: str,
    window_start: date,
    window_end: date,
    project_id: str,
) -> str:
    """Returns CTEs for (incarcerated_spans, incarcerated_days) clipped to window.

    `incarcerated_spans` selects each person's incarceration super-sessions in
    `state_code` that overlap [window_start, window_end). `incarcerated_days`
    sums per-person days within the clipped window and drops anyone with zero
    days. Intended to be embedded as a CTE chain inside a larger WITH clause.
    """
    return f"""
    incarcerated_spans AS (
      SELECT
        person_id,
        start_date AS incar_start,
        COALESCE(end_date_exclusive, '9999-12-31') AS incar_end
      FROM `{project_id}.sessions.compartment_level_1_super_sessions_materialized`
      WHERE state_code = '{state_code}'
        AND compartment_level_1 = 'INCARCERATION'
        AND start_date < DATE '{window_end}'
        AND COALESCE(end_date_exclusive, '9999-12-31') > DATE '{window_start}'
    ),
    incarcerated_days AS (
      SELECT
        person_id,
        SUM(GREATEST(DATE_DIFF(
          LEAST(incar_end, DATE '{window_end}'),
          GREATEST(incar_start, DATE '{window_start}'),
          DAY
        ), 0)) AS incarcerated_days
      FROM incarcerated_spans
      GROUP BY person_id
      HAVING incarcerated_days > 0
    )
    """


def trim_iqr(x: pd.Series, k: float = 1.5) -> pd.Series:
    """Drop values outside ±k×IQR."""
    q1, q3 = x.quantile([0.25, 0.75])
    iqr = q3 - q1
    return x[(x >= q1 - k * iqr) & (x <= q3 + k * iqr)]


def welch_post_vs_pre(
    df: pd.DataFrame, value_col: str, decimals: int = 2
) -> pd.DataFrame:
    """One-row Welch's two-sample t-test (post vs pre) summary."""
    pre = df.loc[~df["post"], value_col]
    post = df.loc[df["post"], value_col]
    diff = post.mean() - pre.mean()
    res = stats.ttest_ind(post, pre, equal_var=False)
    return pd.DataFrame(
        [
            {
                "Pre n": f"{len(pre):,}",
                "Post n": f"{len(post):,}",
                "Pre mean": round(pre.mean(), decimals),
                "Post mean": round(post.mean(), decimals),
                "Diff (post − pre)": round(diff, decimals),
                "Welch's t": round(res.statistic, 3),
                "p-value": f"{res.pvalue:.4g}",
                "Significant @ α=0.05": "Yes" if res.pvalue < 0.05 else "No",
            }
        ]
    )


def pre_post_xticklabels(
    df: pd.DataFrame,
    pre_label: str = "Pre launch",
    post_label: str = "Post launch",
) -> list[str]:
    """Two-element x-tick labels (pre / post) with row-count annotations."""
    n_pre = (~df["post"]).sum()
    n_post = df["post"].sum()
    return [f"{pre_label}\n(n = {n_pre:,})", f"{post_label}\n(n = {n_post:,})"]


def build_long_df(
    pre: pd.Series,
    post: pd.Series,
    value_col: str,
    trim: bool = True,
) -> pd.DataFrame:
    """Stack pre/post series into a long-form DataFrame for ``sns.barplot``.

    If ``trim`` is True, each period is IQR-trimmed before stacking and
    seaborn's CI computation runs on the trimmed distribution. Set
    ``trim=False`` for outcomes where the tail is the policy signal and
    shouldn't be dropped.
    """
    transform = trim_iqr if trim else (lambda x: x)
    return pd.concat(
        [
            pd.DataFrame(
                {"post": False, value_col: transform(pre.astype(float)).values}
            ),
            pd.DataFrame(
                {"post": True, value_col: transform(post.astype(float)).values}
            ),
        ],
        ignore_index=True,
    )


def plot_pre_post_bar(
    df: pd.DataFrame,
    value_col: str,
    ylabel: str,
    title: str,
    color: str | None = None,
    fmt: str = "%.2f",
    welch_decimals: int = 2,
) -> pd.DataFrame:
    """Render a pre/post bar with 95% CI; return the Welch's t-test summary."""
    _, ax = plt.subplots(figsize=(7, 4.5))
    sns.barplot(  # type: ignore[attr-defined]
        data=df,
        x="post",
        y=value_col,
        errorbar=("ci", 95),
        color=color or RECIDIVIZ_COLORS[0],
        ax=ax,
    )
    ax.set_xticklabels(pre_post_xticklabels(df))
    ax.set_xlabel("")
    ax.set_ylabel(ylabel)
    for container in ax.containers:
        ax.bar_label(container, fmt=fmt, padding=4, fontsize=8)  # type: ignore[arg-type]
    ax.set_ylim(0, ax.get_ylim()[1] * 1.20)
    plt.title(title)
    plt.show()
    return welch_post_vs_pre(df, value_col, decimals=welch_decimals)
