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

from recidiviz.tools.find_unused_bq_views import (
    UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON,
    get_unused_across_all_projects_addresses_from_all_views_dag,
)


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
                Found the following views that are not used by any known downstream product: {unused_addresses}.

                In order to address this failure, you can do the following:
                1) If this view is no longer needed, delete it in this PR.
                2) If this view is no longer needed but there is a reason why you want to delete it in 
                  a later follow-up PR (e.g. because there are a lot of views to delete, you want to 
                  validate your change first, etc), add the address to 
                  OTHER_ADDRESSES_TO_KEEP_WITH_REASON with your name, date, and a comment that includes
                  a linked cleanup task.
                3) If this is a new view that will soon be used in a product view, add the address to 
                  OTHER_ADDRESSES_TO_KEEP_WITH_REASON with your name, date, and a comment that includes
                  a linked task that encompasses the work to use this new view downstream. This view
                  should be removed from OTHER_ADDRESSES_TO_KEEP_WITH_REASON once it does have a
                  downstream product use.
                4) If this view is still in use in LOOKER and cannot be deleted, add the address to 
                  LOOKER_REFERENCED_ADDRESSES.
                5) If you do not think your failure falls under any of these cases, please reach out to
                  Doppler in the #platform-team channel.
                """,
        )

    def test_addresses_to_keep_are_unused(self) -> None:
        all_unused_addresses = (
            get_unused_across_all_projects_addresses_from_all_views_dag(
                ignore_exemptions=True
            )
        )
        # Get views that are marked as unused in the exemption list, but aren't actually
        # unused.
        delete_from_keep_list_addresses = (
            set(UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON) - all_unused_addresses
        )

        self.assertEqual(
            0,
            len(delete_from_keep_list_addresses),
            f"""
             
             Found the following views that are marked as unused but are actually used: {delete_from_keep_list_addresses}
             
             Please remove the view(s) from UNREFERENCED_ADDRESSES_TO_KEEP_WITH_REASON.
            """,
        )
