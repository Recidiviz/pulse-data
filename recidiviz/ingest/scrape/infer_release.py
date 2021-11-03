# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Exposes API to infer release of people."""

import logging
from http import HTTPStatus

from flask import Blueprint, request

from recidiviz.common.constants.county.booking import CustodyStatus
from recidiviz.ingest.scrape import scrape_phase, sessions
from recidiviz.ingest.scrape.ingest_utils import validate_regions
from recidiviz.persistence import persistence
from recidiviz.utils import monitoring
from recidiviz.utils.auth.gae import requires_gae_auth
from recidiviz.utils.params import get_str_param_values
from recidiviz.utils.regions import Region, RemovedFromWebsite, get_region

infer_release_blueprint = Blueprint("infer_release", __name__)


@infer_release_blueprint.route("/release")
@requires_gae_auth
def infer_release():
    """Runs infer release for the given regions."""
    region_codes = validate_regions(get_str_param_values("region", request.args))
    regions = [get_region(region_code) for region_code in region_codes]

    for region in regions:
        with monitoring.push_tags({monitoring.TagKey.REGION: region.region_code}):
            if region.agency_type != "jail":
                continue

            session = sessions.get_most_recent_completed_session(region.region_code)
            if session:
                logging.info(
                    "Got most recent completed session for [%s] with "
                    "start time [%s]",
                    region.region_code,
                    session.start,
                )
                persistence.infer_release_on_open_bookings(
                    region.region_code, session.start, _get_custody_status(region)
                )
                sessions.update_phase(session, scrape_phase.ScrapePhase.DONE)

    return "", HTTPStatus.OK


def _get_custody_status(region: Region):
    removed_from_website = region.removed_from_website
    if removed_from_website == RemovedFromWebsite.RELEASED:
        return CustodyStatus.INFERRED_RELEASE
    if removed_from_website == RemovedFromWebsite.UNKNOWN_SIGNIFICANCE:
        return CustodyStatus.REMOVED_WITHOUT_INFO
    raise ValueError(
        f"RemovedFromWebsite value {removed_from_website} not mapped to a ReleaseReason"
    )
