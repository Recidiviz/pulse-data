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
"""Registry of eOMIS writeback flows that run on the scheduled entrypoint.

Central, data-only wiring: each entry names a flow, the eOMIS instance URLs it
targets, and how to build it for an unattended run. Registering a new flow is a
single entry here — the entrypoint and the per-state flow modules stay
untouched. Kept out of the flow modules on purpose so per-state flow code
carries no scheduling/hosting concern.
"""
from __future__ import annotations

from typing import Callable

import attr

from recidiviz.common import attr_validators
from recidiviz.eomis.flow import EomisWritebackFlow
from recidiviz.eomis.us_ar import program_referral_flow as ar
from recidiviz.eomis.us_co import sentence_credit_flow as co
from recidiviz.utils.environment import GCP_PROJECT_PRODUCTION

ScheduledFlowBuilder = Callable[..., EomisWritebackFlow]


@attr.define(frozen=True, kw_only=True)
class ScheduledFlow:
    """How the scheduled entrypoint runs one eOMIS writeback flow."""

    test_base_url: str = attr.ib(validator=attr_validators.is_non_empty_str)
    prod_base_url: str = attr.ib(validator=attr_validators.is_non_empty_str)

    # build(*, bq_view: str | None, project_id: str) -> flow, for an unattended run.
    build: ScheduledFlowBuilder = attr.ib(validator=attr.validators.is_callable())

    def base_url_for_project(self, project_id: str) -> str:
        """Returns the prod instance from the production project, else test."""
        if project_id == GCP_PROJECT_PRODUCTION:
            return self.prod_base_url
        return self.test_base_url


def _build_ar_ged(*, bq_view: str | None, project_id: str) -> EomisWritebackFlow:
    return ar.ArProgramReferralFlow(
        bq_view=bq_view or ar.default_view(project_id),
        project_id=project_id,
        limit=None,
        comment="",
        add_comment="",
    )


def _build_co_edovo(*, bq_view: str | None, project_id: str) -> EomisWritebackFlow:
    return co.CoSentenceCreditFlow(
        bq_view=bq_view or co.default_view(project_id),
        project_id=project_id,
        limit=None,
    )


SCHEDULED_FLOWS: dict[str, ScheduledFlow] = {
    "ar_ged": ScheduledFlow(
        test_base_url=ar.TEST_BASE_URL,
        prod_base_url=ar.PROD_BASE_URL,
        build=_build_ar_ged,
    ),
    "co_edovo": ScheduledFlow(
        test_base_url=co.TEST_BASE_URL,
        prod_base_url=co.PROD_BASE_URL,
        build=_build_co_edovo,
    ),
}
