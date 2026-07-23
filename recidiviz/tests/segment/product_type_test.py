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

from recidiviz.segment.product_type import REENTRY_ASSESSMENT_PATH, ProductType


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

    def test_url_bases_defined_for_every_product_type(self) -> None:
        """Every product type must resolve to at least one base URL."""
        for product_type in ProductType:
            self.assertTrue(product_type.url_bases)

    def test_context_page_filter_matches_all_jii_hosts(self) -> None:
        """The JII product filter must match every JII host, not just the web app."""
        fragment = (
            ProductType.JII_OPPORTUNITIES_APP.context_page_filter_query_fragment()
        )
        for url_base in ProductType.JII_OPPORTUNITIES_APP.url_bases:
            self.assertIn(f"{url_base}/%", fragment)

    def test_url_base_filter_matches_all_hosts(self) -> None:
        """url_base_filter matches every host a product is served from."""
        jii_host_filter = ProductType.JII_OPPORTUNITIES_APP.url_base_filter()
        for url_base in ProductType.JII_OPPORTUNITIES_APP.url_bases:
            self.assertIn(f"{url_base}/%", jii_host_filter)

    def test_url_base_filter_omits_path_filter_that_product_filter_applies(
        self,
    ) -> None:
        """url_base_filter applies no path narrowing: for a product whose attribution
        needs a path filter, the host filter omits it while
        context_page_filter_query_fragment includes it."""
        self.assertNotIn("/workflows", ProductType.WORKFLOWS.url_base_filter())
        self.assertIn(
            "/workflows",
            ProductType.WORKFLOWS.context_page_filter_query_fragment(),
        )

    def test_url_base_filter_uses_provided_column_name(self) -> None:
        """The column name argument is threaded into the generated predicate."""
        self.assertIn(
            "context_page_path",
            ProductType.WORKFLOWS.url_base_filter("context_page_path"),
        )

    def test_reentry_assessment_belongs_to_cpa_not_jii_opportunities_app(self) -> None:
        """The reentry-assessment surface embedded in the opportunities app is claimed by
        CPA and explicitly excluded from the JII opportunities app product."""
        cpa_fragment = (
            ProductType.CASE_PLANNING_ASSISTANT.context_page_filter_query_fragment()
        )
        jii_fragment = (
            ProductType.JII_OPPORTUNITIES_APP.context_page_filter_query_fragment()
        )
        # CPA claims the reentry-assessment surface...
        self.assertIn(
            f"REGEXP_CONTAINS(context_page_url, r'{REENTRY_ASSESSMENT_PATH}')",
            cpa_fragment,
        )
        # ...and the JII opportunities app excludes it.
        self.assertIn(
            f"NOT REGEXP_CONTAINS(context_page_url, r'{REENTRY_ASSESSMENT_PATH}')",
            jii_fragment,
        )
