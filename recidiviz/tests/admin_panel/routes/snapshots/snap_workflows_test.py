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
Snapshots for recidiviz/tests/admin_panel/routes/workflows_test.py
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
        "denialAdjective": "Ineligible",
        "denialNoun": "Ineligibility",
        "denialReasons": [{"key": "DENY", "text": "Denied"}],
        "denialText": "Deny",
        "deniedTabTitle": "Marked Ineligible",
        "displayName": "display",
        "dynamicEligibilityText": "text",
        "eligibilityDateText": "date text",
        "eligibleCriteriaCopy": [],
        "emptyTabCopy": [{"tab": "Eligible Now", "text": "No people are eligible"}],
        "featureVariant": "feature_variant",
        "hideDenialRevert": True,
        "highlightCasesOnHomepage": False,
        "highlightedCaseCtaCopy": "Opportunity name",
        "id": 1,
        "ineligibleCriteriaCopy": [],
        "initialHeader": "header",
        "isAlert": False,
        "markSubmittedOptionsByTab": [
            {"tab": "Pending", "texts": ["PENDING_1", "PENDING_2", "PENDING_3"]},
            {"tab": "Eligible Now", "texts": ["PENDING_1"]},
        ],
        "methodologyUrl": "url",
        "nonOmsCriteria": [
            {"text": "test text"},
            {"text": "test criteria with tooltip", "tooltip": "test tooltip"},
        ],
        "nonOmsCriteriaHeader": "Requirements to check",
        "notifications": [],
        "omsCriteriaHeader": "Validated by data from OMS",
        "overdueOpportunityCalloutCopy": "overdue for opportunity",
        "priority": "NORMAL",
        "revisionDescription": "for testing",
        "sidebarComponents": ["someComponent"],
        "snooze": {"defaultSnoozeDays": 30, "maxSnoozeDays": 180},
        "stagingId": None,
        "stateCode": "US_ID",
        "status": "ACTIVE",
        "subcategoryHeadings": [
            {"subcategory": "PENDING_1", "text": "Pending type 1"},
            {"subcategory": "PENDING_2", "text": "Pending type 2"},
        ],
        "subcategoryOrderings": [
            {"tab": "Pending", "texts": ["PENDING_1", "PENDING_2"]},
            {"tab": "Eligible Now", "texts": ["ELIGIBLE_1", "ELIGIBLE_2"]},
        ],
        "subheading": "this is what the policy does",
        "submittedTabTitle": "Submitted",
        "supportsSubmitted": True,
        "tabGroups": [],
        "tabPrefaceCopy": [{"tab": "Pending", "text": "Pending people"}],
        "tooltipEligibilityText": "Eligible",
        "variantDescription": "A config",
        "zeroGrantsTooltip": "example tooltip",
        "snoozeCompanionOpportunityTypes": ["usNdOppType1", "usNdOppType2"],
        "caseNotesTitle": "Case notes title",
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
        "denialAdjective": "Ineligible",
        "denialNoun": "Ineligibility",
        "denialReasons": [{"key": "DENY", "text": "Denied"}],
        "denialText": "Deny",
        "deniedTabTitle": "Marked Ineligible",
        "displayName": "display",
        "dynamicEligibilityText": "text",
        "eligibilityDateText": "date text",
        "eligibleCriteriaCopy": [],
        "emptyTabCopy": [{"tab": "Eligible Now", "text": "No people are eligible"}],
        "featureVariant": "feature_variant",
        "hideDenialRevert": True,
        "highlightCasesOnHomepage": False,
        "highlightedCaseCtaCopy": "Opportunity name",
        "id": 2,
        "ineligibleCriteriaCopy": [],
        "initialHeader": "header",
        "isAlert": False,
        "markSubmittedOptionsByTab": [
            {"tab": "Pending", "texts": ["PENDING_1", "PENDING_2", "PENDING_3"]},
            {"tab": "Eligible Now", "texts": ["PENDING_1"]},
        ],
        "methodologyUrl": "url",
        "nonOmsCriteria": [
            {"text": "test text"},
            {"text": "test criteria with tooltip", "tooltip": "test tooltip"},
        ],
        "nonOmsCriteriaHeader": "Requirements to check",
        "notifications": [],
        "omsCriteriaHeader": "Validated by data from OMS",
        "overdueOpportunityCalloutCopy": "overdue for opportunity",
        "priority": "NORMAL",
        "revisionDescription": "for testing",
        "sidebarComponents": ["someComponent"],
        "snooze": {"defaultSnoozeDays": 30, "maxSnoozeDays": 180},
        "stagingId": None,
        "stateCode": "US_ID",
        "status": "INACTIVE",
        "subcategoryHeadings": [
            {"subcategory": "PENDING_1", "text": "Pending type 1"},
            {"subcategory": "PENDING_2", "text": "Pending type 2"},
        ],
        "subcategoryOrderings": [
            {"tab": "Pending", "texts": ["PENDING_1", "PENDING_2"]},
            {"tab": "Eligible Now", "texts": ["ELIGIBLE_1", "ELIGIBLE_2"]},
        ],
        "subheading": "this is what the policy does",
        "submittedTabTitle": "Submitted",
        "supportsSubmitted": True,
        "tabGroups": [],
        "tabPrefaceCopy": [{"tab": "Pending", "text": "Pending people"}],
        "tooltipEligibilityText": "Eligible",
        "variantDescription": "A config",
        "zeroGrantsTooltip": "example tooltip",
        "snoozeCompanionOpportunityTypes": ["usNdOppType1", "usNdOppType2"],
        "caseNotesTitle": "Case notes title",
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
        "denialAdjective": "Ineligible",
        "denialNoun": "Ineligibility",
        "denialReasons": [{"key": "DENY", "text": "Denied"}],
        "denialText": "Deny",
        "deniedTabTitle": "Marked Ineligible",
        "displayName": "display",
        "dynamicEligibilityText": "text",
        "eligibilityDateText": "date text",
        "eligibleCriteriaCopy": [],
        "emptyTabCopy": [{"tab": "Eligible Now", "text": "No people are eligible"}],
        "featureVariant": "feature_variant",
        "hideDenialRevert": True,
        "highlightCasesOnHomepage": False,
        "highlightedCaseCtaCopy": "Opportunity name",
        "id": 3,
        "ineligibleCriteriaCopy": [],
        "initialHeader": "header",
        "isAlert": False,
        "markSubmittedOptionsByTab": [
            {"tab": "Pending", "texts": ["PENDING_1", "PENDING_2", "PENDING_3"]},
            {"tab": "Eligible Now", "texts": ["PENDING_1"]},
        ],
        "methodologyUrl": "url",
        "nonOmsCriteria": [
            {"text": "test text"},
            {"text": "test criteria with tooltip", "tooltip": "test tooltip"},
        ],
        "nonOmsCriteriaHeader": "Requirements to check",
        "notifications": [],
        "omsCriteriaHeader": "Validated by data from OMS",
        "overdueOpportunityCalloutCopy": "overdue for opportunity",
        "priority": "NORMAL",
        "revisionDescription": "for testing",
        "sidebarComponents": ["someComponent"],
        "snooze": {"defaultSnoozeDays": 30, "maxSnoozeDays": 180},
        "stagingId": None,
        "stateCode": "US_ID",
        "status": "ACTIVE",
        "subcategoryHeadings": [
            {"subcategory": "PENDING_1", "text": "Pending type 1"},
            {"subcategory": "PENDING_2", "text": "Pending type 2"},
        ],
        "subcategoryOrderings": [
            {"tab": "Pending", "texts": ["PENDING_1", "PENDING_2"]},
            {"tab": "Eligible Now", "texts": ["ELIGIBLE_1", "ELIGIBLE_2"]},
        ],
        "subheading": "this is what the policy does",
        "submittedTabTitle": "Submitted",
        "supportsSubmitted": True,
        "tabGroups": [],
        "tabPrefaceCopy": [{"tab": "Pending", "text": "Pending people"}],
        "tooltipEligibilityText": "Eligible",
        "variantDescription": "A config",
        "zeroGrantsTooltip": "example tooltip",
        "snoozeCompanionOpportunityTypes": ["usNdOppType1", "usNdOppType2"],
        "caseNotesTitle": "Case notes title",
    },
]
