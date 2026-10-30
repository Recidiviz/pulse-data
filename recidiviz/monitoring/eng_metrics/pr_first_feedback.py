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
"""One row per pull request with when it became ready for review and when the
first human feedback arrived, computed from the archived GitHub webhook events.

For example, if the archive holds these events for two PRs in one repo (payload
fields abridged):

    event_timestamp_ts   github_event_type     action            payload
    2026-10-01 10:00:00  pull_request          opened            number=1, draft=true, user=alice
    2026-10-01 13:00:00  pull_request          ready_for_review  number=1
    2026-10-01 13:05:00  pull_request_review   submitted         number=1, user=claude[bot] (Bot)
    2026-10-01 15:00:00  pull_request_review   submitted         number=1, user=bob
    2026-10-02 09:00:00  pull_request          opened            number=2, draft=false, user=bob

...this view returns:

    pr_number  ready_at             ready_at_source         first_feedback_at    first_feedback_login  pickup_hours
    1          2026-10-01 13:00:00  ready_for_review_event  2026-10-01 15:00:00  bob                   2.0
    2          2026-10-02 09:00:00  opened_ready            NULL                 NULL                  NULL

PR 1 opened as a draft, so its review clock starts at the ready_for_review event,
and the bot review five minutes later does not count. PR 2 opened ready and has
no feedback yet, so its feedback columns are null until someone reviews it.
"""
from recidiviz.big_query.big_query_view import SimpleBigQueryViewBuilder
from recidiviz.big_query.big_query_view_column import (
    BigQueryViewColumn,
    Float,
    Integer,
    String,
    Timestamp,
)
from recidiviz.monitoring.eng_metrics.dataset_config import (
    ENG_METRICS_DATASET,
    GITHUB_EVENTS_ARCHIVE_ADDRESS,
)
from recidiviz.monitoring.eng_metrics.exclusions import (
    ENG_METRICS_REPO_ALLOWLIST,
    EXCLUDED_FEEDBACK_LOGINS,
)
from recidiviz.utils.environment import GCP_PROJECT_STAGING
from recidiviz.utils.metadata import local_project_id_override
from recidiviz.utils.string_formatting import fix_indent

PR_FIRST_FEEDBACK_VIEW_ID = "pr_first_feedback"

REPOSITORY_FULL_NAME_COLUMN = "repository_full_name"
PR_NUMBER_COLUMN = "pr_number"
PR_URL_COLUMN = "pr_url"
PR_AUTHOR_LOGIN_COLUMN = "pr_author_login"
PR_CREATED_AT_COLUMN = "pr_created_at"
READY_AT_COLUMN = "ready_at"
READY_AT_SOURCE_COLUMN = "ready_at_source"
FIRST_FEEDBACK_AT_COLUMN = "first_feedback_at"
FIRST_FEEDBACK_LOGIN_COLUMN = "first_feedback_login"
FIRST_FEEDBACK_KIND_COLUMN = "first_feedback_kind"
PICKUP_HOURS_COLUMN = "pickup_hours"

READY_AT_SOURCE_OPENED_READY = "opened_ready"
READY_AT_SOURCE_READY_FOR_REVIEW_EVENT = "ready_for_review_event"

FEEDBACK_KIND_REVIEW = "review"
FEEDBACK_KIND_REVIEW_COMMENT = "review_comment"
FEEDBACK_KIND_ISSUE_COMMENT = "issue_comment"

_VIEW_DESCRIPTION = """One row per pull request in the measured repos, with when
the PR became ready for review and when the first human feedback arrived. Ready
is the first ready_for_review event for a PR that opened as a draft, else the
PR's created_at. First feedback is the earliest submitted review, inline review
comment, or PR issue comment at or after ready, from an account that is not a
bot, not the automation account, and not the PR author. pickup_hours is the gap
between the two.

Rows with a null first_feedback_at are PRs nobody has picked up yet, so a
percentile over recent weeks always reads faster than it will once those PRs
complete. The view leaves out bot-authored PRs and repos outside the allowlist.
"""


def _sql_string_list(values: list[str]) -> str:
    """Returns the values as a comma-separated list of SQL string literals."""
    return ", ".join(f"'{value}'" for value in values)


def _build_query_template() -> str:
    """Returns the view query template."""
    repo_allowlist = _sql_string_list(ENG_METRICS_REPO_ALLOWLIST)
    excluded_logins = _sql_string_list(EXCLUDED_FEEDBACK_LOGINS)
    query_template = f"""
WITH events AS (
    SELECT
        delivery_id,
        repository_full_name,
        github_event_type,
        action,
        event_timestamp_ts,
        webhook_payload
    FROM `{{project_id}}.{GITHUB_EVENTS_ARCHIVE_ADDRESS.to_str()}`
    WHERE repository_full_name IN ({repo_allowlist})
),
pr_events AS (
    SELECT
        repository_full_name,
        CAST(JSON_VALUE(webhook_payload, '$.pull_request.number') AS INT64) AS pr_number,
        TIMESTAMP(JSON_VALUE(webhook_payload, '$.pull_request.created_at')) AS pr_created_at,
        JSON_VALUE(webhook_payload, '$.pull_request.user.login') AS pr_author_login,
        JSON_VALUE(webhook_payload, '$.pull_request.user.type') AS pr_author_type,
        IFNULL(JSON_VALUE(webhook_payload, '$.pull_request.draft') = 'true', FALSE) AS is_draft,
        action,
        event_timestamp_ts,
        delivery_id
    FROM events
    WHERE github_event_type = 'pull_request'
),
-- The earliest archived event for a PR is usually its opened event. When it is
-- not (the PR predates the archive), that event's draft flag says whether the
-- PR was still a draft at that point, and a ready_for_review event as the
-- first one seen proves the PR was a draft just before it.
prs AS (
    SELECT
        repository_full_name,
        pr_number,
        pr_created_at,
        pr_author_login,
        pr_author_type,
        is_draft OR action = 'ready_for_review' AS opened_as_draft
    FROM pr_events
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY repository_full_name, pr_number
        ORDER BY event_timestamp_ts, delivery_id
    ) = 1
),
ready_events AS (
    SELECT
        repository_full_name,
        pr_number,
        MIN(event_timestamp_ts) AS first_ready_event_at
    FROM pr_events
    WHERE action = 'ready_for_review'
    GROUP BY 1, 2
),
pr_ready AS (
    SELECT
        prs.repository_full_name,
        prs.pr_number,
        prs.pr_created_at,
        prs.pr_author_login,
        IF(prs.opened_as_draft, ready_events.first_ready_event_at, prs.pr_created_at) AS ready_at,
        CASE
            WHEN NOT prs.opened_as_draft THEN '{READY_AT_SOURCE_OPENED_READY}'
            WHEN ready_events.first_ready_event_at IS NOT NULL THEN '{READY_AT_SOURCE_READY_FOR_REVIEW_EVENT}'
        END AS ready_at_source
    FROM prs
    LEFT JOIN ready_events
    USING (repository_full_name, pr_number)
    WHERE prs.pr_author_type IS DISTINCT FROM 'Bot'
        AND prs.pr_author_login NOT IN ({excluded_logins})
),
feedback AS (
    SELECT
        repository_full_name,
        CAST(JSON_VALUE(webhook_payload, '$.pull_request.number') AS INT64) AS pr_number,
        TIMESTAMP(JSON_VALUE(webhook_payload, '$.review.submitted_at')) AS feedback_at,
        JSON_VALUE(webhook_payload, '$.review.user.login') AS feedback_login,
        JSON_VALUE(webhook_payload, '$.review.user.type') AS feedback_user_type,
        '{FEEDBACK_KIND_REVIEW}' AS feedback_kind,
        1 AS kind_rank
    FROM events
    WHERE github_event_type = 'pull_request_review' AND action = 'submitted'

    UNION ALL

    SELECT
        repository_full_name,
        CAST(JSON_VALUE(webhook_payload, '$.pull_request.number') AS INT64) AS pr_number,
        TIMESTAMP(JSON_VALUE(webhook_payload, '$.comment.created_at')) AS feedback_at,
        JSON_VALUE(webhook_payload, '$.comment.user.login') AS feedback_login,
        JSON_VALUE(webhook_payload, '$.comment.user.type') AS feedback_user_type,
        '{FEEDBACK_KIND_REVIEW_COMMENT}' AS feedback_kind,
        2 AS kind_rank
    FROM events
    WHERE github_event_type = 'pull_request_review_comment' AND action = 'created'

    UNION ALL

    -- An issue_comment lands on a PR only when the issue carries a pull_request
    -- object; comments on plain issues have none.
    SELECT
        repository_full_name,
        CAST(JSON_VALUE(webhook_payload, '$.issue.number') AS INT64) AS pr_number,
        TIMESTAMP(JSON_VALUE(webhook_payload, '$.comment.created_at')) AS feedback_at,
        JSON_VALUE(webhook_payload, '$.comment.user.login') AS feedback_login,
        JSON_VALUE(webhook_payload, '$.comment.user.type') AS feedback_user_type,
        '{FEEDBACK_KIND_ISSUE_COMMENT}' AS feedback_kind,
        3 AS kind_rank
    FROM events
    WHERE github_event_type = 'issue_comment'
        AND action = 'created'
        AND JSON_QUERY(webhook_payload, '$.issue.pull_request') IS NOT NULL
),
first_feedback AS (
    SELECT
        pr_ready.repository_full_name,
        pr_ready.pr_number,
        feedback.feedback_at,
        feedback.feedback_login,
        feedback.feedback_kind
    FROM pr_ready
    JOIN feedback
    USING (repository_full_name, pr_number)
    WHERE feedback.feedback_at >= pr_ready.ready_at
        AND feedback.feedback_user_type IS DISTINCT FROM 'Bot'
        AND feedback.feedback_login NOT IN ({excluded_logins})
        AND feedback.feedback_login IS DISTINCT FROM pr_ready.pr_author_login
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY repository_full_name, pr_number
        ORDER BY feedback.feedback_at, feedback.kind_rank, feedback.feedback_login
    ) = 1
)
SELECT
    pr_ready.repository_full_name AS {REPOSITORY_FULL_NAME_COLUMN},
    pr_ready.pr_number AS {PR_NUMBER_COLUMN},
    CONCAT(
        'https://github.com/', pr_ready.repository_full_name, '/pull/', CAST(pr_ready.pr_number AS STRING)
    ) AS {PR_URL_COLUMN},
    pr_ready.pr_author_login AS {PR_AUTHOR_LOGIN_COLUMN},
    pr_ready.pr_created_at AS {PR_CREATED_AT_COLUMN},
    pr_ready.ready_at AS {READY_AT_COLUMN},
    pr_ready.ready_at_source AS {READY_AT_SOURCE_COLUMN},
    first_feedback.feedback_at AS {FIRST_FEEDBACK_AT_COLUMN},
    first_feedback.feedback_login AS {FIRST_FEEDBACK_LOGIN_COLUMN},
    first_feedback.feedback_kind AS {FIRST_FEEDBACK_KIND_COLUMN},
    TIMESTAMP_DIFF(first_feedback.feedback_at, pr_ready.ready_at, SECOND) / 3600.0 AS {PICKUP_HOURS_COLUMN}
FROM pr_ready
LEFT JOIN first_feedback
USING (repository_full_name, pr_number)
"""
    return fix_indent(query_template, indent_level=0)


def _schema() -> list[BigQueryViewColumn]:
    """Returns the view's schema."""
    return [
        String(
            name=REPOSITORY_FULL_NAME_COLUMN,
            description="The repo as owner/name, always one of the allowlisted repos.",
            mode="REQUIRED",
        ),
        Integer(
            name=PR_NUMBER_COLUMN,
            description="The PR number within the repo. (repository_full_name, pr_number) identifies a row.",
            mode="REQUIRED",
        ),
        String(
            name=PR_URL_COLUMN,
            description="Link to the PR on GitHub, for checking a row by hand.",
            mode="REQUIRED",
        ),
        String(
            name=PR_AUTHOR_LOGIN_COLUMN,
            description="GitHub login of the PR author.",
            mode="NULLABLE",
        ),
        Timestamp(
            name=PR_CREATED_AT_COLUMN,
            description="When the PR was opened, from the webhook payload.",
            mode="NULLABLE",
        ),
        Timestamp(
            name=READY_AT_COLUMN,
            description=(
                "When the review clock started: the first ready_for_review event for a "
                "PR that opened as a draft, else pr_created_at. Null when the PR opened "
                "as a draft and no ready_for_review event was archived."
            ),
            mode="NULLABLE",
        ),
        String(
            name=READY_AT_SOURCE_COLUMN,
            description=(
                "Which rule set ready_at: opened_ready or ready_for_review_event. "
                "Null when ready_at is null."
            ),
            mode="NULLABLE",
        ),
        Timestamp(
            name=FIRST_FEEDBACK_AT_COLUMN,
            description=(
                "The earliest submitted review, inline review comment, or PR issue "
                "comment at or after ready_at from an account that is not a bot, not "
                "the automation account, and not the PR author. Null until someone "
                "gives feedback."
            ),
            mode="NULLABLE",
        ),
        String(
            name=FIRST_FEEDBACK_LOGIN_COLUMN,
            description="GitHub login of whoever gave the first feedback. Null if none yet.",
            mode="NULLABLE",
        ),
        String(
            name=FIRST_FEEDBACK_KIND_COLUMN,
            description=(
                "What the first feedback was: review, review_comment, or issue_comment. "
                "Null if none yet."
            ),
            mode="NULLABLE",
        ),
        Float(
            name=PICKUP_HOURS_COLUMN,
            description=(
                "Hours from ready_at to first_feedback_at. Null when either is null."
            ),
            mode="NULLABLE",
        ),
    ]


PR_FIRST_FEEDBACK_VIEW_BUILDER = SimpleBigQueryViewBuilder(
    dataset_id=ENG_METRICS_DATASET,
    view_id=PR_FIRST_FEEDBACK_VIEW_ID,
    description=_VIEW_DESCRIPTION,
    view_query_template=_build_query_template(),
    schema=_schema(),
    should_materialize=True,
    clustering_fields=[REPOSITORY_FULL_NAME_COLUMN],
)

if __name__ == "__main__":
    with local_project_id_override(GCP_PROJECT_STAGING):
        PR_FIRST_FEEDBACK_VIEW_BUILDER.build_and_print()
