# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2023 Recidiviz, Inc.
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
"""Tests for find_unused_bq_views.py."""
import unittest

from recidiviz.big_query.big_query_address import BigQueryAddress
from recidiviz.tools.find_unused_bq_views import (
    TEMPORARY_UNREFERENCED_ADDRESSES_TO_KEEP,
    UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON,
    get_unused_across_all_projects_addresses_from_all_views_dag,
)
from recidiviz.tools.lint.check_todo_format import VALID_TO_DO_RE
from recidiviz.tools.unreferenced_bq_view_exemptions import (
    PERMANENT_UNREFERENCED_ADDRESSES_TO_KEEP,
    TEMPORARY_GRANDFATHERED_UNREFERENCED_ADDRESSES_TO_KEEP,
)
from recidiviz.view_registry.deployed_view_graphs import (
    builders_for_all_view_graphs_across_projects,
)

_MINIMUM_REASON_LENGTH = 40


class TestFindUnusedBQViews(unittest.TestCase):
    """Tests for enforcing no untracked unreferenced views and testing the
    find_unused_bq_views script.
    """

    def test_no_unused_views(self) -> None:
        unused_addresses = get_unused_across_all_projects_addresses_from_all_views_dag()
        self.assertEqual(
            0,
            len(unused_addresses),
            f"""
                Found the following views that are not used by any known downstream product: \n{BigQueryAddress.addresses_to_str(unused_addresses, indent_level=18)}.

                In order to address this failure, you can do the following:
                1) If this view is no longer needed, delete it in this PR.
                2) If this view is temporarily unreferenced, that is, it will gain a downstream
                  consumer or be deleted in a follow-up PR, add the address to
                  TEMPORARY_UNREFERENCED_ADDRESSES_TO_KEEP in find_unused_bq_views.py,
                  with your name, date, and a reason that cites the tracking ticket as a
                  TODO (e.g. TODO(#12345) or TODO(OBT-12345)). Remove it once the view
                  has a downstream product use.
                3) If this view is unreferenced by design and will never be removed, add
                  the address to PERMANENT_UNREFERENCED_ADDRESSES_TO_KEEP in
                  unreferenced_bq_view_exemptions.py, with your name, date, and a reason.
                4) If this view is still in use in LOOKER and cannot be deleted, add the address to
                  LOOKER_REFERENCED_ADDRESSES.
                5) If you do not think your failure falls under any of these cases, please reach out to
                  Doppler in the #platform-team channel.
                """,
        )

    def test_addresses_to_keep_are_unused_or_invalid(self) -> None:
        all_unused_addresses = (
            get_unused_across_all_projects_addresses_from_all_views_dag(
                ignore_exemptions=True
            )
        )

        all_views: set[BigQueryAddress] = set(
            vb.address for vb in builders_for_all_view_graphs_across_projects()
        )

        # Get views that are marked as unused in the exemption list, but aren't actually
        # unused.
        delete_from_keep_list_addresses = (
            set(UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON) - all_unused_addresses
        )

        not_included_in_view_graph = delete_from_keep_list_addresses - all_views

        self.assertEqual(
            0,
            len(not_included_in_view_graph),
            f"""
            Found the following views that are marked as unused but are not in the view graph: \n{BigQueryAddress.addresses_to_str(not_included_in_view_graph, indent_level=14)}
             
            Please remove the view(s) from UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON if you are deleting the view or add them to the view graph if you have not yet already.
            """,
        )

        self.assertEqual(
            0,
            len(delete_from_keep_list_addresses),
            f"""
            Found the following views that are marked as unused but are actually used: \n{BigQueryAddress.addresses_to_str(delete_from_keep_list_addresses, indent_level=14)}

            Please remove the view(s) from UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON.
            """,
        )

    def test_exemption_maps_are_disjoint(self) -> None:
        temporary = set(TEMPORARY_UNREFERENCED_ADDRESSES_TO_KEEP)
        grandfathered = set(TEMPORARY_GRANDFATHERED_UNREFERENCED_ADDRESSES_TO_KEEP)
        permanent = set(PERMANENT_UNREFERENCED_ADDRESSES_TO_KEEP)

        self.assertEqual(
            set(),
            temporary & grandfathered,
            "Addresses appear in both the temporary and grandfathered exemption maps: "
            f"\n{BigQueryAddress.addresses_to_str(temporary & grandfathered, indent_level=14)}",
        )
        self.assertEqual(
            set(),
            temporary & permanent,
            "Addresses appear in both the temporary and permanent exemption maps: "
            f"\n{BigQueryAddress.addresses_to_str(temporary & permanent, indent_level=14)}",
        )
        self.assertEqual(
            set(),
            grandfathered & permanent,
            "Addresses appear in both the grandfathered and permanent exemption maps: "
            f"\n{BigQueryAddress.addresses_to_str(grandfathered & permanent, indent_level=14)}",
        )

    def test_every_exemption_reason_meets_minimum_length(self) -> None:
        too_short = {
            address: reason
            for address, reason in UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON.items()
            if len(reason.strip()) < _MINIMUM_REASON_LENGTH
        }
        self.assertEqual(
            {},
            too_short,
            f"The following unreferenced-view exemptions have a reason shorter than "
            f"{_MINIMUM_REASON_LENGTH} characters. Explain why the view must be kept, "
            f"including a point of contact and date: \n"
            f"{self._exemptions_to_str(too_short)}",
        )

    def test_every_temporary_exemption_cites_a_todo(self) -> None:
        missing_todo = {
            address: reason
            for address, reason in TEMPORARY_UNREFERENCED_ADDRESSES_TO_KEEP.items()
            if not VALID_TO_DO_RE.search(reason)
        }
        self.assertEqual(
            {},
            missing_todo,
            f"The following temporary exemptions do not cite a tracking issue as a "
            f"TODO (e.g. TODO(#12345) or TODO(OBT-12345)), which is required as the "
            f"burn-down tracker. Add one, or - if the view is unreferenced by design "
            f"and will never be removed - move it to "
            f"PERMANENT_UNREFERENCED_ADDRESSES_TO_KEEP in "
            f"unreferenced_bq_view_exemptions.py: \n"
            f"{self._exemptions_to_str(missing_todo)}",
        )

    @staticmethod
    def _exemptions_to_str(exemptions: dict[BigQueryAddress, str]) -> str:
        return "\n".join(
            f"    `{address.to_str()}`: {reason}"
            for address, reason in sorted(
                exemptions.items(), key=lambda item: item[0].to_str()
            )
        )
