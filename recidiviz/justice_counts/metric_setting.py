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

import datetime
from copy import deepcopy
from typing import Any, Dict, List, Optional, Set

from sqlalchemy.orm import Query, Session

from recidiviz.justice_counts.metrics.metric_interface import (
    MetricDefinition,
    MetricInterface,
)
from recidiviz.justice_counts.metrics.metric_registry import METRIC_KEY_TO_METRIC
from recidiviz.justice_counts.utils.constants import REPLACE_FIELDS
from recidiviz.persistence.database.schema.justice_counts import schema
from recidiviz.persistence.database.schema.justice_counts.schema import (
    ReportingFrequency,
)


def handle_invariants(metric_interface_json: Dict[str, Any]) -> Dict[str, Any]:
    """This function maintains any relationships between metric interface fields which
    are not handled by the apply_updates step.

    For example,
        existing = {custom_reporting_frequency: {custom_frequency:'ANNUAL', starting_month: 7}}
        update = {custom_reporting_frequency: {custom_frequency:'MONTHLY', starting_month: None}}

    Without this function, the apply_updates return value would just be
        result = {custom_reporting_frequency: {custom_frequency:'MONTHLY', starting_month: 7}}

    But we need the result to be
        result = {custom_reporting_frequency: {custom_frequency:'MONTHLY', starting_month: None}}
    """
    # If custom reporting frequency is set to MONTHLY, the starting month should be None.
    if (
        metric_interface_json["custom_reporting_frequency"]["custom_frequency"]
        == ReportingFrequency.MONTHLY.value
    ):
        metric_interface_json["custom_reporting_frequency"]["starting_month"] = None
    return metric_interface_json


def apply_updates(
    original: Dict[str, Any],
    updates: Dict[str, Any],
) -> Dict[str, Any]:
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
        if key in REPLACE_FIELDS:
            # If `key` is not in `original`, we add the full key-value pair from `updates`
            # to `original` as-is. This applies regardless of whether `value` is a
            # dictionary or a primitive value.
            original[key] = value
        elif isinstance(value, dict) and key in original:
            apply_updates(original[key], value)
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
        agency_metric_updates: MetricInterface,
        user_account: Optional[schema.UserAccount] = None,
    ) -> None:
        """Overwrite an agency's metric setting with the given metric interface."""

        existing_setting = (
            MetricSettingInterface.get_metric_setting_by_agency_id_and_metric_key(
                session=session,
                agency_id=agency.id,
                metric_definition_key=agency_metric_updates.key,
            )
        )
        if existing_setting is not None:
            if existing_setting.metric_interface is None:
                raise ValueError(
                    f"metric_interface column in MetricSetting table should never be null, but is null for agency {agency.name} and metric definition key {agency_metric_updates.key}."
                )

            updates = agency_metric_updates.to_storage_json()
            # Using deepcopy so that the existing metric interface is only modified once
            # apply_updates returns.
            existing_setting.metric_interface = handle_invariants(
                apply_updates(
                    deepcopy(existing_setting.metric_interface),
                    updates,
                )
            )
            existing_setting.last_updated = datetime.datetime.now(
                tz=datetime.timezone.utc
            )

        else:
            session.add(
                schema.MetricSetting(
                    agency_id=agency.id,
                    metric_definition_key=agency_metric_updates.key,
                    metric_interface=agency_metric_updates.to_storage_json(),
                    last_updated=datetime.datetime.now(tz=datetime.timezone.utc),
                    created_at=datetime.datetime.now(tz=datetime.timezone.utc),
                )
            )
        # Add metric setting history entry.
        session.add(
            schema.MetricSettingHistory(
                agency_id=agency.id,
                metric_definition_key=agency_metric_updates.key,
                updates=handle_invariants(agency_metric_updates.to_storage_json()),
                user_account_id=(user_account.id if user_account is not None else None),
                timestamp=datetime.datetime.now(tz=datetime.timezone.utc),
            )
        )
        session.commit()

    @staticmethod
    def add_or_update_agency_metric_setting(
        session: Session,
        agency: schema.Agency,
        agency_metric_updates: MetricInterface,
        user_account: Optional[schema.UserAccount] = None,
    ) -> None:
        """Add or update an agency's metric setting with the given metric interface.

        If disaggregated_by_supervision_subsystem is modified, we will also modify the
        disaggregated_by_supervision_subsystem and is_metric_enabled fields accordingly
        for all supervision systems and subsystems for the metric.
        See the test_set_disaggregated_by_supervision_subsystem_to_true test as an
        example of this behavior.
        """
        agency_systems = {schema.System[s] for s in agency.systems}
        current_system = agency_metric_updates.metric_definition.system.value
        # First, modify the disaggregated_by_supervision_subsystems and is_metric_enabled
        # fields for all supervision subsystems for this agency if
        # disaggregated_by_supervision_subsystems is modified.
        if (
            agency_metric_updates.disaggregated_by_supervision_subsystems is not None
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
                    agency_metric_updates.key
                    if system == current_system
                    else agency_metric_updates.key.replace(current_system, system, 1)
                )
                MetricSettingInterface.update_metric_setting(
                    session=session,
                    agency=agency,
                    agency_metric_updates=MetricInterface(
                        key=metric_definition_key,
                        disaggregated_by_supervision_subsystems=agency_metric_updates.disaggregated_by_supervision_subsystems,
                        is_metric_enabled=(
                            agency_metric_updates.disaggregated_by_supervision_subsystems
                            if schema.System[system]
                            in schema.System.supervision_subsystems()
                            else not agency_metric_updates.disaggregated_by_supervision_subsystems
                        ),
                    ),
                    user_account=user_account,
                )
        # Update the metric interface as-given if disaggregated_by_supervision_subsystems
        # is not being updated, or if the agency does not have supervision subsystems.
        MetricSettingInterface.update_metric_setting(
            session=session,
            agency=agency,
            agency_metric_updates=agency_metric_updates,
            user_account=user_account,
        )

    @staticmethod
    def get_agency_metric_interfaces(
        session: Session,
        agency: schema.Agency,
        agency_metric_settings: Optional[List[schema.MetricSetting]] = None,
        expunge_metric_settings: Optional[bool] = True,
    ) -> List[MetricInterface]:
        """
        Gets an agency's metric interfaces from the MetricSetting table.

        If a metric applies to an agency but does not exist in the MetricSetting table,
        return an empty MetricInterface for that metric.

        Performs post-processing on the stored_metric_interfaces. See post_process_storage_json()
        for post-processing details.

        Parameters:
        - session (Session): The SQLAlchemy session.
        - agency (Agency): The agency for which to get metric interfaces.
        - agency_metric_settings (List[MetricSetting]): If provided, this set of
            MetricSettings will be used instead of querying the database.
            Post-processing steps are then applied to the provided metric settings.
        """
        if agency_metric_settings is None:
            agency_metric_settings = (
                session.query(schema.MetricSetting)
                .filter(schema.MetricSetting.agency_id == agency.id)
                .all()
            )

        # Filter out metric settings for deprecated metrics
        filtered_agency_metric_settings = [
            metric_setting
            for metric_setting in agency_metric_settings
            if metric_setting.metric_definition_key in METRIC_KEY_TO_METRIC
        ]

        key_to_metric_interfaces = {
            item.metric_definition_key: MetricInterface.from_storage_json(
                item.metric_interface
            )
            for item in filtered_agency_metric_settings
        }
        # Return a metric interface for every metric definition, even if no metric
        # setting exists in storage.
        metric_definitions = MetricInterface.get_metric_definitions_by_systems(
            systems={schema.System[system] for system in agency.systems or []},
        )
        result: List[MetricInterface] = []
        for metric_definition in metric_definitions:
            metric_interface = key_to_metric_interfaces.get(
                metric_definition.key,
                MetricInterface(key=metric_definition.key),
            )
            # Ensure the metric interface obeys MetricInterface invariants.
            metric_interface.post_process_storage_json()
            result.append(metric_interface)

        if expunge_metric_settings is True:
            for metric_setting in agency_metric_settings or []:
                session.expunge(metric_setting)
        return result

    @staticmethod
    def get_metric_key_to_metric_interface(
        session: Session,
        agency: schema.Agency,
        metric_interfaces: Optional[List[MetricInterface]] = None,
        expunge_metric_settings: Optional[bool] = False,
    ) -> Dict[str, MetricInterface]:
        """Create a map of all metric keys to MetricInterfaces that
        belong to an agency.

        Parameters:
        - session: The SQLAlchemy session.
        - agency: The agency for which to get metric interfaces.
        - metric_interfaces: If provided, this list of MetricInterfaces will be used
            instead of querying the database. The list of metric_interfaces MUST be
            obtained from a call to MetricSettingInterface.get_agency_metric_interfaces()
            since this method applies important post-processing steps to the interfaces.
        """
        if metric_interfaces is None:
            metric_interfaces = MetricSettingInterface.get_agency_metric_interfaces(
                session=session,
                agency=agency,
                expunge_metric_settings=expunge_metric_settings,
            )
        metric_key_to_metric_interface = {
            metric_interface.key: metric_interface
            for metric_interface in metric_interfaces
        }
        return metric_key_to_metric_interface

    @staticmethod
    def get_agency_id_to_metric_key_to_metric_interface(
        session: Session,
        agency_query: Query,
        expunge_metric_settings: Optional[bool] = True,
    ) -> Dict[int, Dict[str, MetricInterface]]:
        agency_id_to_metric_key_to_metric_interface: Dict[
            int, Dict[str, MetricInterface]
        ] = {}
        for agency in agency_query:
            agency_id_to_metric_key_to_metric_interface[
                agency.id
            ] = MetricSettingInterface.get_metric_key_to_metric_interface(
                session=session,
                agency=agency,
                expunge_metric_settings=expunge_metric_settings,
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
                metric_starting_month=(
                    metric_interface.custom_reporting_frequency.starting_month
                    if metric_interface.custom_reporting_frequency.starting_month
                    is not None
                    else 1
                ),
                metric_frequency=(
                    metric_interface.custom_reporting_frequency.frequency.value
                    if metric_interface.custom_reporting_frequency.frequency is not None
                    else metric.reporting_frequency.value
                ),
            ):
                metric_definitions.append(metric)

        return metric_definitions
