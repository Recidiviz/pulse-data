# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2022 Recidiviz, Inc.
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
"""
Utils for more sophisticated agency logic.
"""

import logging
from typing import Dict

from sqlalchemy.orm import Session

from recidiviz.justice_counts.agency import AgencyInterface
from recidiviz.persistence.database.schema.justice_counts import schema

logger = logging.getLogger(__name__)


def delete_agency(session: Session, agency_id: int, dry_run: bool) -> Dict:
    """Delete the given agency and all its data."""
    agency = AgencyInterface.get_agency_by_id(
        session=session,
        agency_id=agency_id,
        with_users=True,
        with_settings=True,
    )

    logger.info("DRY RUN: %s", dry_run)
    logger.info("Will delete %s", agency.name)

    objects_to_delete = []
    associations = (
        session.query(schema.AgencyUserAccountAssociation)
        .filter_by(agency_id=agency_id)
        .all()
    )
    objects_to_delete.extend(associations)
    logger.info("Will delete %d UserAccountAssociations", len(associations))

    reports = session.query(schema.Report).filter_by(source_id=agency_id).all()
    objects_to_delete.extend(reports)
    logger.info("Will delete %d reports", len(reports))

    datapoints = session.query(schema.Datapoint).filter_by(source_id=agency_id).all()
    # Delete the Datapoint History entries associated with each deleted datapoint.
    for datapoint in datapoints:
        datapoint_histories = (
            session.query(schema.DatapointHistory)
            .filter_by(datapoint_id=datapoint.id)
            .all()
        )
        objects_to_delete.extend(datapoint_histories)

    objects_to_delete.extend(datapoints)
    logger.info("Will delete %d datapoints", len(datapoints))

    settings = session.query(schema.AgencySetting).filter_by(source_id=agency_id).all()
    objects_to_delete.extend(settings)
    logger.info("Will delete %d agency settings", len(settings))

    metric_settings = (
        session.query(schema.MetricSetting).filter_by(agency_id=agency_id).all()
    )
    objects_to_delete.extend(metric_settings)
    logger.info("Will delete %d metric settings", len(metric_settings))

    jurisdictions = (
        session.query(schema.AgencyJurisdiction).filter_by(source_id=agency_id).all()
    )
    objects_to_delete.extend(jurisdictions)
    logger.info("Will delete %d jurisdictions", len(jurisdictions))

    spreadsheets = (
        session.query(schema.Spreadsheet).filter_by(agency_id=agency_id).all()
    )
    objects_to_delete.extend(spreadsheets)
    logger.info("Will delete %d spreadsheets", len(spreadsheets))

    # If the agency is a superagency, nullify the super_agency_id field from all of its
    # children. Nullifying a child agency's super_agency_id field causes the child
    # agency to no longer be considered a child agency.
    agencies = session.query(schema.Agency).filter_by(id=agency_id).all()
    for agency in agencies:
        if not agency.is_superagency:
            continue
        children = (
            session.query(schema.Agency).filter_by(super_agency_id=agency.id).all()
        )
        # Set all children's super_agency_ids to None.
        for child_agency in children:
            child_agency.super_agency_id = None

    objects_to_delete.extend(agencies)
    logger.info("Will delete %d agencies", len(agencies))

    if dry_run is False:
        for obj in objects_to_delete:
            session.delete(obj)
        session.commit()
        logger.info("%s deleted", agency.name)

    # Return the agency as a JSON object for frontend purposes.
    return agency.to_json(with_team=False, with_settings=False)
