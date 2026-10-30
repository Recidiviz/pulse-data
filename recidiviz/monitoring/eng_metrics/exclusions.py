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
"""Which repos and which accounts the engineering metrics views count.

The GitHub org webhook covers every repo in the org, including forks, prototypes,
and the public mirrors, so the views only count repos listed here. The views
exclude bot accounts by their GitHub account type; this module lists the one
bot that GitHub types as a User.
"""

# Repos whose PRs the engineering metrics views measure. Edit this list to add
# or drop a repo; nothing else needs to change.
ENG_METRICS_REPO_ALLOWLIST: list[str] = [
    "Recidiviz/pulse-data",
    "Recidiviz/recidiviz-dashboards",
    "Recidiviz/looker",
    "Recidiviz/recidiviz-ops",
    "Recidiviz/ops-portal",
    "Recidiviz/recidiviz-iam",
    "Recidiviz/claude-skills",
    "Recidiviz/web-libraries",
]

# Accounts whose reviews and comments never count as human feedback, and whose
# PRs the views do not measure, even though GitHub types them as User rather
# than Bot.
# helperbot-recidiviz is the automation account behind the TBR auto-approval
# workflow and the deploy scripts.
EXCLUDED_FEEDBACK_LOGINS: list[str] = ["helperbot-recidiviz"]
