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

from typing import Dict, List, Optional, Set

from sqlalchemy.orm import Session

from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.metrics.metric_interface import (
    MetricDefinition,
    MetricInterface,
)
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

    @staticmethod
    def get_agency_metric_interfaces(
        session: Session,
        agency: schema.Agency,
        agency_datapoints: Optional[List[schema.Datapoint]] = None,
    ) -> List[MetricInterface]:
        """Gets an agency metric interface from the MetricSetting table.
        Note: Until we start the migration process, this method will read agency metrics
        from the Datapoint table."""

        return DatapointInterface.get_metric_settings_by_agency(
            session=session, agency=agency, agency_datapoints=agency_datapoints
        )

    @staticmethod
    def get_metric_key_to_metric_interface(
        session: Session,
        agency: schema.Agency,
        agency_datapoints: Optional[List[schema.Datapoint]] = None,
    ) -> Dict[str, MetricInterface]:
        """Create a map of all metric keys to MetricInterfaces that
        belong to an agency."""

        metric_interfaces = MetricSettingInterface.get_agency_metric_interfaces(
            session=session,
            agency=agency,
            agency_datapoints=agency_datapoints,
        )
        metric_key_to_metric_interface = {
            metric_interface.key: metric_interface
            for metric_interface in metric_interfaces
        }
        return metric_key_to_metric_interface

    @staticmethod
    def get_agency_id_to_metric_key_to_metric_interface(
        session: Session,
        agencies: List[schema.Agency],
    ) -> Dict[int, Dict[str, MetricInterface]]:
        agency_id_to_metric_key_to_metric_interface: Dict[
            int, Dict[str, MetricInterface]
        ] = {}
        for agency in agencies:
            agency_id_to_metric_key_to_metric_interface[
                agency.id
            ] = MetricSettingInterface.get_metric_key_to_metric_interface(
                session=session, agency=agency
            )
        return agency_id_to_metric_key_to_metric_interface

    @staticmethod
    def should_add_metric_definition_to_report(
        report_frequency: str,
        metric_frequency: str,
        report_starting_month: int,
        metric_starting_month: int,
    ) -> bool:
        """Returns True if the reporting frequency of the metric
        matches that of the report."""
        if metric_frequency == report_frequency:
            if metric_frequency == "MONTHLY":
                return True
            if metric_frequency == "ANNUAL":
                return report_starting_month == metric_starting_month

        return False

    @staticmethod
    def get_metric_definitions_for_report(
        systems: Set[schema.System],
        metric_key_to_metric_interface: Dict[str, MetricInterface],
        report_frequency: str,
        starting_month: int,
    ) -> List[MetricDefinition]:
        """Returns the metric definitions on a report based upon the reports
        custom reporting frequencies."""
        metrics = MetricInterface.get_metric_definitions_by_systems(
            systems=systems,
        )
        metric_definitions = []
        for metric in metrics:
            metric_interface = metric_key_to_metric_interface.get(metric.key)
            # If there is no metric interface for this metric, create an empty one.
            if metric_interface is None:
                metric_interface = MetricInterface(key=metric.key)
                metric_key_to_metric_interface[metric.key] = metric_interface
            if metric_interface.has_report_datapoints:
                # If there are report datapoints for this metric already,
                # add metric to metric_definitions. We do this to make sure
                # that even if you changed this metric's reporting frequency
                # after this report was created, it should still show up in
                # the report.
                metric_definitions.append(metric)
                continue

            # If there are no report datapoints yet for this metric, add it
            # to the report if the custom reporting frequency matches report_frequency.
            # If there is no custom reporting frequency, add the metric if the default
            # reporting frequency matches report_frequency.
            if MetricSettingInterface.should_add_metric_definition_to_report(
                report_starting_month=starting_month,
                report_frequency=report_frequency,
                metric_starting_month=metric_interface.custom_reporting_frequency.starting_month
                if metric_interface.custom_reporting_frequency.starting_month
                is not None
                else 1,
                metric_frequency=metric_interface.custom_reporting_frequency.frequency.value
                if metric_interface.custom_reporting_frequency.frequency is not None
                else metric.reporting_frequency.value,
            ):
                metric_definitions.append(metric)

        return metric_definitions
