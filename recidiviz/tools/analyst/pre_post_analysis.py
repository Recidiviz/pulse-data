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
import numpy as np
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


def per_person_credit_activity_monthly(
    window_start: date,
    window_end: date,
    *,
    state_code: str,
    project_id: str,
    credit_filter_sql: str,
) -> pd.DataFrame:
    """Per-resident-per-month credit-activity rates over the window.

    Queries `analyst_data.earned_credit_activity_materialized` for events in
    [window_start, window_end] matching ``credit_filter_sql``. Cohort is every
    resident incarcerated in ``state_code`` during the window.

    Returns one row per resident with:
      - ``incarcerated_days``: days incarcerated within the window
      - ``n_events``: matching event count
      - ``total_credits``: sum of ``credits_earned`` across matching events
      - ``events_per_resident_per_month``: ``n_events / (incarcerated_days / 30.4375)``
      - ``credits_per_resident_per_month``: ``total_credits / (incarcerated_days / 30.4375)``
    """
    return pd.read_gbq(
        f"""
    WITH
    {get_incarceration_super_sessions_and_days_in_window(state_code=state_code, window_start=window_start, window_end=window_end, project_id=project_id)},
    person_credits AS (
      SELECT
        e.person_id,
        COUNT(*) AS n_events,
        SUM(e.credits_earned) AS total_credits
      FROM `{project_id}.analyst_data.earned_credit_activity_materialized` e
      WHERE e.state_code = '{state_code}'
        AND ({credit_filter_sql})
        AND e.credit_date >= DATE '{window_start}'
        AND e.credit_date <  DATE '{window_end}'
      GROUP BY e.person_id
    )
    SELECT
      i.person_id,
      i.incarcerated_days,
      CAST(COALESCE(pc.n_events, 0) AS INT64) AS n_events,
      CAST(COALESCE(pc.total_credits, 0) AS FLOAT64) AS total_credits,
      CAST(
        COALESCE(pc.n_events, 0)
          / NULLIF(i.incarcerated_days / 30.4375, 0)
        AS FLOAT64
      ) AS events_per_resident_per_month,
      CAST(
        COALESCE(pc.total_credits, 0)
          / NULLIF(i.incarcerated_days / 30.4375, 0)
        AS FLOAT64
      ) AS credits_per_resident_per_month
    FROM incarcerated_days i
    LEFT JOIN person_credits pc USING (person_id)
    """,
        project_id=project_id,
        progress_bar_type=None,
    )


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


def t_ci_half_width(mean: pd.Series, sd: pd.Series, n: pd.Series) -> pd.Series:
    """95% t-CI half-width on the mean"""
    _, ci_high = stats.t.interval(
        0.95,
        df=(n - 1).clip(lower=1),
        loc=mean,
        scale=sd.fillna(0) / np.sqrt(n),
    )
    return ci_high - mean


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
    ymin, ymax = ax.get_ylim()
    if ymin >= 0:
        ax.set_ylim(0, ymax * 1.20)
    elif ymax <= 0:
        ax.set_ylim(ymin * 1.20, 0)
    else:
        pad = (ymax - ymin) * 0.10
        ax.set_ylim(ymin - pad, ymax + pad)
    plt.title(title)
    plt.show()
    return welch_post_vs_pre(df, value_col, decimals=welch_decimals)


def plot_pre_post_histograms(
    pre: pd.Series,
    post: pd.Series,
    *,
    xlabel: str,
    title: str,
    bins: int | list[float] = 30,
    color: str | None = None,
    xticks: list[float] | None = None,
) -> None:
    """Side-by-side pre/post histograms on shared axes for distribution comparison."""
    bin_edges = (
        np.histogram_bin_edges(np.concatenate([pre, post]), bins=bins)
        if isinstance(bins, int)
        else bins
    )
    _, axes = plt.subplots(1, 2, figsize=(11, 4), sharex=True, sharey=True)
    for ax, data, label in zip(axes, [pre, post], ["Pre launch", "Post launch"]):
        ax.hist(
            data, bins=bin_edges, color=color or RECIDIVIZ_COLORS[0], edgecolor="white"
        )
        ax.set_title(f"{label} (n = {len(data):,})")
        ax.set_xlabel(xlabel)
        ax.set_ylabel("Number of observations")
        if xticks is not None:
            ax.set_xticks(xticks)
    plt.suptitle(title)
    plt.tight_layout()
    plt.show()
