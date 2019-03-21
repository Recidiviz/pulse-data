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

from http import HTTPStatus
import logging

from flask import Blueprint, request, url_for

from recidiviz.common import queues
from recidiviz.common.constants.booking import CustodyStatus
from recidiviz.ingest.scrape import scrape_phase, sessions
from recidiviz.ingest.scrape.ingest_utils import validate_regions
from recidiviz.utils.auth import authenticate_request
from recidiviz.persistence import persistence
from recidiviz.utils import monitoring
from recidiviz.utils.params import get_values
from recidiviz.utils.regions import Region, RemovedFromWebsite, get_region

infer_release_blueprint = Blueprint('infer_release', __name__)


@infer_release_blueprint.route('/release')
@authenticate_request
def infer_release():
    """Runs infer release for the given regions."""
    region_codes = validate_regions(get_values('region', request.args))
    regions = [get_region(region_code) for region_code in region_codes]

    next_phase = scrape_phase.next_phase(request.endpoint)
    next_phase_url = url_for(next_phase) if next_phase else None

    for region in regions:
        with monitoring.push_tags(
                {monitoring.TagKey.REGION: region.region_code}):
            if region.agency_type != 'jail':
                continue

            session = sessions.get_most_recent_completed_session(
                region.region_code)
            if session:
                logging.info('Got most recent completed session for %s with '
                             'start time %s', region.region_code, session.start)
                persistence.infer_release_on_open_bookings(
                    region.region_code, session.start,
                    _get_custody_status(region))

            if next_phase:
                logging.info('Enqueueing %s for region %s.',
                             next_phase, region.region_code)
                queues.enqueue_scraper_phase(
                    region_code=region.region_code, url=next_phase_url)

    return '', HTTPStatus.OK


def _get_custody_status(region: Region):
    removed_from_website = region.removed_from_website
    if removed_from_website == RemovedFromWebsite.RELEASED:
        return CustodyStatus.INFERRED_RELEASE
    if removed_from_website == RemovedFromWebsite.UNKNOWN_SIGNIFICANCE:
        return CustodyStatus.REMOVED_WITHOUT_INFO
    raise ValueError(
        "RemovedFromWebsite value {} not mapped to a ReleaseReason".format(
            removed_from_website))
