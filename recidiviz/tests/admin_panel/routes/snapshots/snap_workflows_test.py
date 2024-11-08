#  Recidiviz - a data platform for criminal justice reform
#  Copyright (C) 2024 Recidiviz, Inc.
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#  =============================================================================
"""
Snapshots for recidiviz/tests/workflows/querier/querier_test.py
Update by running `pytest recidiviz/tests/admin_panel/routes/workflows_test.py --snapshot-update`
You will need to replace this header afterward.
"""

# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots[
    "WorkflowsAdminPanelEndpointTests.WorkflowsAdminPanelEndpointTests test_get_configs_for_opportunity"
] = [
    {
        "callToAction": "do something",
        "compareBy": [
            {
                "field": "eligibilityDate",
                "sortDirection": "asc",
                "undefinedBehavior": "undefinedFirst",
            }
        ],
        "createdAt": "2024-05-12 00:00:00",
        "createdBy": "Mary",
        "denialReasons": {"DENY": "Denied"},
        "denialText": "Deny",
        "description": "A config",
        "displayName": "display",
        "dynamicEligibilityText": "text",
        "eligibilityDateText": "date text",
        "eligibleCriteriaCopy": {},
        "featureVariant": "feature_variant",
        "hideDenialRevert": True,
        "id": 1,
        "ineligibleCriteriaCopy": {},
        "initialHeader": "header",
        "isAlert": False,
        "methodologyUrl": "url",
        "notifications": [],
        "priority": "NORMAL",
        "sidebarComponents": ["someComponent"],
        "snooze": {"defaultSnoozeDays": 30, "maxSnoozeDays": 180},
        "stagingId": None,
        "stateCode": "US_ID",
        "status": "ACTIVE",
        "subheading": "this is what the policy does",
        "tabGroups": {},
        "tooltipEligibilityText": "Eligible",
        "zeroGrantsTooltip": "example tooltip",
    },
    {
        "callToAction": "do something",
        "compareBy": [
            {
                "field": "eligibilityDate",
                "sortDirection": "asc",
                "undefinedBehavior": "undefinedFirst",
            }
        ],
        "createdAt": "2024-05-13 00:00:00",
        "createdBy": "Mary",
        "denialReasons": {"DENY": "Denied"},
        "denialText": "Deny",
        "description": "A config",
        "displayName": "display",
        "dynamicEligibilityText": "text",
        "eligibilityDateText": "date text",
        "eligibleCriteriaCopy": {},
        "featureVariant": "feature_variant",
        "hideDenialRevert": True,
        "id": 2,
        "ineligibleCriteriaCopy": {},
        "initialHeader": "header",
        "isAlert": False,
        "methodologyUrl": "url",
        "notifications": [],
        "priority": "NORMAL",
        "sidebarComponents": ["someComponent"],
        "snooze": {"defaultSnoozeDays": 30, "maxSnoozeDays": 180},
        "stagingId": None,
        "stateCode": "US_ID",
        "status": "INACTIVE",
        "subheading": "this is what the policy does",
        "tabGroups": {},
        "tooltipEligibilityText": "Eligible",
        "zeroGrantsTooltip": "example tooltip",
    },
    {
        "callToAction": "do something",
        "compareBy": [
            {
                "field": "eligibilityDate",
                "sortDirection": "asc",
                "undefinedBehavior": "undefinedFirst",
            }
        ],
        "createdAt": "2024-05-15 00:00:00",
        "createdBy": "Mary",
        "denialReasons": {"DENY": "Denied"},
        "denialText": "Deny",
        "description": "A config",
        "displayName": "display",
        "dynamicEligibilityText": "text",
        "eligibilityDateText": "date text",
        "eligibleCriteriaCopy": {},
        "featureVariant": "feature_variant",
        "hideDenialRevert": True,
        "id": 3,
        "ineligibleCriteriaCopy": {},
        "initialHeader": "header",
        "isAlert": False,
        "methodologyUrl": "url",
        "notifications": [],
        "priority": "NORMAL",
        "sidebarComponents": ["someComponent"],
        "snooze": {"defaultSnoozeDays": 30, "maxSnoozeDays": 180},
        "stagingId": None,
        "stateCode": "US_ID",
        "status": "ACTIVE",
        "subheading": "this is what the policy does",
        "tabGroups": {},
        "tooltipEligibilityText": "Eligible",
        "zeroGrantsTooltip": "example tooltip",
    },
]
