# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2025 Recidiviz, Inc.
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
"""Provides custom LookML fields for an entity."""
import attr

from recidiviz.looker.lookml_dashboard_element import (
    FULL_SCREEN_WIDTH,
    SMALL_ELEMENT_HEIGHT,
    LookMLColorApplication,
    LookMLDashboardElement,
    LookMLElementType,
    LookMLListen,
    LookMLSort,
)


@attr.define
class EntityDashboardElementFactory:
    """
    Class to provide LookML dashboard elements that may be reused across multiple entity dashboards.
    """

    @staticmethod
    def info_element() -> LookMLDashboardElement:
        """Element to display relevant information for the dashboard."""
        return LookMLDashboardElement(
            title="Info",
            name="info",
            type=LookMLElementType.TEXT,
            body_text="__Info:__ ❇️ - Indicates an open incarceration/supervision period in respective tables."
            " __Note:__ Person_id is not consistent between production and staging environments,"
            " make sure to filter by external id and state if you want to compare a person between the two environments.",
            height=SMALL_ELEMENT_HEIGHT,
            width=FULL_SCREEN_WIDTH,
        )

    @staticmethod
    def multiparent_disclaimer_element() -> LookMLDashboardElement:
        """Element to display a disclaimer regarding multiparent tables."""
        return LookMLDashboardElement(
            title="Disclaimer",
            name="disclaimer",
            type=LookMLElementType.TEXT,
            body_text="__Note:__ Multi-parent tables state_charge and state_early_discharge may be missing results."
            " Please migrate to sentences v2 and charges v2 for accurate results.",
            height=SMALL_ELEMENT_HEIGHT,
            width=FULL_SCREEN_WIDTH,
        )

    @staticmethod
    def actions_element(
        explore: str, listen: LookMLListen, model: str
    ) -> LookMLDashboardElement:
        """Element to display the 'actions' button to switch between different person dashboards.
        Assumes that the explore has an 'actions' field defining the button."""
        return LookMLDashboardElement(
            title="Actions",
            name="actions",
            explore=explore,
            model=model,
            type=LookMLElementType.SINGLE_VALUE,
            fields=[f"{explore}.actions"],
            listen=listen,
            height=SMALL_ELEMENT_HEIGHT,
            width=FULL_SCREEN_WIDTH,
        )

    @staticmethod
    def person_periods_timeline_element(
        explore: str, person_periods_view_name: str, listen: LookMLListen, model: str
    ) -> LookMLDashboardElement:
        """Element to display the periods timeline for a person. Assumes that the explore has a person_id field."""
        return LookMLDashboardElement(
            title="Periods Timeline",
            name="periods_timeline",
            explore=explore,
            model=model,
            type=LookMLElementType.LOOKER_TIMELINE,
            fields=[
                f"{explore}.person_id",
                f"{person_periods_view_name}.period_type",
                f"{person_periods_view_name}.start_date",
                f"{person_periods_view_name}.end_date",
            ],
            sorts=[LookMLSort(f"{person_periods_view_name}.start_date", desc=True)],
            group_bars=False,
            show_legend=True,
            color_application=LookMLColorApplication(
                collection_id="recidiviz-color-collection",
                palette_id="recidiviz-color-collection-categorical-0",
            ),
            listen=listen,
            width=FULL_SCREEN_WIDTH,
        )
