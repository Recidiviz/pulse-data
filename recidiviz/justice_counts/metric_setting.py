# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Interface for working with the MetricSetting model."""

from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.metrics.metric_interface import MetricInterface
from recidiviz.persistence.database.schema.justice_counts import schema


class MetricSettingInterface:
    """Contains methods for working with the MetricSetting table."""

    @staticmethod
    def add_or_update_agency_metric_setting(
        session: Session,
        agency: schema.Agency,
        agency_metric: MetricInterface,
        user_account: Optional[schema.UserAccount] = None,
    ) -> None:
        """Add the agency metric interface to the MetricSetting table as a lightweight
        json representation with report datapoints removed.
        Note: Until we start the migration process, this method will write
        agency metrics to the Datapoint table."""
        DatapointInterface.add_or_update_agency_datapoints(
            session=session,
            agency=agency,
            agency_metric=agency_metric,
            user_account=user_account,
        )

    # TODO(#27956): Replace these DatapointInterface methods.
    #   get_agency_datapoints
    #   get_agency_datapoints_for_multiple_agencies
    #   get_agency_datapoints_for_multiple_agencies
    #   get_datapoints_for_agency_dashboard
    @staticmethod
    def get_agency_metric_interfaces(
        session: Session,
        agency: schema.Agency,
    ) -> List[MetricInterface]:
        """Gets an agency metric interface from the MetricSetting table.
        Note: Until we start the migration process, this method will read agency metrics
        from the Datapoint table."""

        return DatapointInterface.get_metric_settings_by_agency(
            session=session, agency=agency, agency_datapoints=None
        )

    # TODO(#27956): Replace these DatapointInterface methods.
    #   get_metric_key_to_agency_datapoints.
    @staticmethod
    def get_metric_key_to_metric_interface(
        session: Session,
        agency: schema.Agency,
    ) -> Dict[str, MetricInterface]:
        """Create a map of all metric keys to MetricInterfaces that
        belong to an agency."""

        metric_interfaces = MetricSettingInterface.get_agency_metric_interfaces(
            session=session, agency=agency
        )
        metric_key_to_metric_interface = {
            metric_interface.key: metric_interface
            for metric_interface in metric_interfaces
        }
        return metric_key_to_metric_interface
