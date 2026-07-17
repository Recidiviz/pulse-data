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
"""Tests for product_type.py"""
import unittest

from recidiviz.segment.product_type import ProductType


class ProductTypeTest(unittest.TestCase):
    """Tests ProductType functions"""

    def test_context_page_filter_query_fragment(self) -> None:
        """Check that context_page_filter_query_fragment is defined for every ProductType enum"""
        for product_type in ProductType:
            self.assertIsNotNone(
                product_type.context_page_filter_query_fragment(
                    context_page_url_col_name="context_page_path"
                )
            )

    def test_product_roster_filter_checks_feature_variant_presence(self) -> None:
        """Feature variants are stored as presence-keyed JSON objects, not the string
        'true', so the product roster filter must check key presence (and not an
        explicit `false` value) rather than `JSON_EXTRACT_SCALAR(...) = 'true'`, which
        never matches and silently zeroes out provisioning for these products."""
        feature_variant_gated = [
            product_type
            for product_type in ProductType
            if product_type.product_roster_feature_variants
        ]
        # Pin the exact set of feature-variant-gated products. This both keeps
        # the loop below from passing vacuously and forces any change to a
        # product's gating to be deliberate (see the failure message below).
        expected_feature_variant_gated = {
            ProductType.CASE_NOTE_SEARCH,
            ProductType.ROUTE_PLANNER,
            ProductType.SUPERVISOR_HOMEPAGE_LAST_LOGIN_MODULE,
            ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
            ProductType.SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE,
        }
        self.assertEqual(
            expected_feature_variant_gated,
            set(feature_variant_gated),
            "The set of feature-variant-gated products changed. This set defines "
            "the provisioned-user population that downstream aggregated metrics "
            "(the provisioning funnels in `insights_impact_metrics`, etc.) are "
            "computed from, so adding or removing a product here shifts those "
            "metrics. If the change is intentional, update this set AND confirm "
            "the affected provisioning / aggregated_metrics views are re-derived; "
            "if it is not, a product likely lost (or gained) its gating by "
            "accident.",
        )

        for product_type in feature_variant_gated:
            fragment = product_type.get_product_roster_filter_query_fragment()
            for fv in product_type.product_roster_feature_variants:
                self.assertIn(
                    f"JSON_QUERY(default_feature_variants, '$.{fv}') IS NOT NULL",
                    fragment,
                )
                self.assertIn(
                    f"JSON_QUERY(default_feature_variants, '$.{fv}') != 'false'",
                    fragment,
                )
                self.assertNotIn(
                    f"JSON_EXTRACT_SCALAR(default_feature_variants, '$.{fv}') = 'true'",
                    fragment,
                )
