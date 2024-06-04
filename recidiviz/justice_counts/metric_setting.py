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

from copy import deepcopy
from typing import Any, Dict, List, Optional, Set

from sqlalchemy.orm import Session

from recidiviz.justice_counts.datapoint import DatapointInterface
from recidiviz.justice_counts.metrics.metric_interface import (
    MetricDefinition,
    MetricInterface,
)
from recidiviz.persistence.database.schema.justice_counts import schema


def apply_updates(original: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively updates the 'original' with the values from 'updates'. Does not
    overwrite if the value of 'updates' is None.

    This approach works as a way to modify metric interface storage JSONs because all
    fields in the storage JSONs are either dictionaries or values. And in the case that
    they are values, we always want to overwrite the original value with the update
    value.

    Arguments:
    - original (json): The original storage json to be updated.
    - updates (json): The updates to apply. Must also be formatted as a storage json.
    """
    for key, value in updates.items():
        if isinstance(value, dict) and key in original:
            apply_updates(original[key], value)
        # If `key` is not in `original`, we add the full key-value pair from `updates`
        # to `original` as-is. This applies regardless of whether `value` is a
        # dictionary or a primitive value.
        elif value is not None:
            original[key] = value
    return original


class MetricSettingInterface:
    """Contains methods for working with the MetricSetting table."""

    @staticmethod
    def get_metric_setting_by_agency_id_and_metric_key(
        session: Session,
        agency_id: int,
        metric_definition_key: str,
    ) -> Optional[schema.MetricSetting]:
        """Get a metric setting according to agency id and metric definition key."""
        return (
            session.query(schema.MetricSetting)
            .filter(
                schema.MetricSetting.agency_id == agency_id,
                schema.MetricSetting.metric_definition_key == metric_definition_key,
            )
            .one_or_none()
        )  # Unique by agency_id and metric_definition_key.

    @staticmethod
    def update_metric_setting(
        session: Session,
        agency: schema.Agency,
        agency_metric: MetricInterface,
    ) -> None:
        """Overwrite an agency's metric setting with the given metric interface."""

        existing_setting = (
            MetricSettingInterface.get_metric_setting_by_agency_id_and_metric_key(
                session=session,
                agency_id=agency.id,
                metric_definition_key=agency_metric.key,
            )
        )

        if existing_setting is not None:
            if existing_setting.metric_interface is None:
                raise ValueError(
                    f"metric_interface column in MetricSetting table should never be null, but is null for agency {agency.name} and metric definition key {agency_metric.key}."
                )

            updates = agency_metric.to_storage_json()
            # Using deepcopy so that the existing metric interface is only modified once
            # apply_updates returns.
            existing_setting.metric_interface = apply_updates(
                deepcopy(existing_setting.metric_interface), updates
            )
        else:
            session.add(
                schema.MetricSetting(
                    agency_id=agency.id,
                    metric_definition_key=agency_metric.key,
                    metric_interface=agency_metric.to_storage_json(),
                )
            )
        session.commit()

    # TODO(#28469): Remove "new" from this method once we deprecate the existing
    # add_or_update_agency_metric_setting method.
    @staticmethod
    def new_add_or_update_agency_metric_setting(
        session: Session,
        agency: schema.Agency,
        agency_metric: MetricInterface,
    ) -> None:
        """Add or update an agency's metric setting with the given metric interface.

        If disaggregated_by_supervision_subsystem is modified, we will also modify the
        disaggregated_by_supervision_subsystem and is_metric_enabled fields accordingly
        for all supervision systems and subsystems for the metric.
        See the test_set_disaggregated_by_supervision_subsystem_to_true test as an
        example of this behavior.
        """
        agency_systems = {schema.System[s] for s in agency.systems}
        current_system = agency_metric.metric_definition.system.value
        # First, modify the disaggregated_by_supervision_subsystems and is_metric_enabled
        # fields for all supervision subsystems for this agency if
        # disaggregated_by_supervision_subsystems is modified.
        if (
            agency_metric.disaggregated_by_supervision_subsystems is not None
            and agency_systems.intersection(schema.System.supervision_subsystems())
        ):
            for system in agency.systems:
                # Skip systems which are neither supervision systems nor subsystems.
                if (
                    schema.System[system] != schema.System.SUPERVISION
                    and schema.System[system]
                    not in schema.System.supervision_subsystems()
                ):
                    continue

                metric_definition_key = (
                    agency_metric.key
                    if system == current_system
                    else agency_metric.key.replace(current_system, system, 1)
                )
                MetricSettingInterface.update_metric_setting(
                    session=session,
                    agency=agency,
                    agency_metric=MetricInterface(
                        key=metric_definition_key,
                        disaggregated_by_supervision_subsystems=agency_metric.disaggregated_by_supervision_subsystems,
                        is_metric_enabled=agency_metric.disaggregated_by_supervision_subsystems
                        if schema.System[system]
                        in schema.System.supervision_subsystems()
                        else not agency_metric.disaggregated_by_supervision_subsystems,
                    ),
                )
        # Update the metric interface as-given if disaggregated_by_supervision_subsystems
        # is not being updated, or if the agency does not have supervision subsystems.
        MetricSettingInterface.update_metric_setting(
            session=session, agency=agency, agency_metric=agency_metric
        )

    @staticmethod
    def add_or_update_agency_metric_setting(
        session: Session,
        agency: schema.Agency,
        agency_metric: MetricInterface,
        user_account: Optional[schema.UserAccount] = None,
    ) -> None:
        """Add the agency metric interface to the MetricSetting table as a lightweight
        json representation with report datapoints removed.
        Note: Until we start the migration process, this method will continue to write
        agency metrics to the Datapoint table. The `new_add_or_update_agency_metric_setting`
        method (which writes to the MetricSetting table instead of writing agency
        datapoints) will be used instead of this method after the migration.
        """
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
