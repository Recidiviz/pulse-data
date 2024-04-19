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
Update by running `pytest recidiviz/tests/workflows/querier/querier_test.py --snapshot-update`
You will need to replace this header afterward.

"""
# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import GenericRepr, Snapshot

snapshots = Snapshot()

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_configs_for_type_returns_newest_first"
] = [
    GenericRepr(
        "FullOpportunityConfig(state_code='US_ID', opportunity_type='usIdSupervisionLevelDowngrade', display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrade all of them', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 2}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched', id=4, created_by='otherBarry@revivizens.org', created_at=datetime.datetime(2023, 5, 17, 0, 0), description='shorter snooze', status=<OpportunityStatus.ACTIVE: 1>, feature_variant='otherFeatureVariant')"
    ),
    GenericRepr(
        "FullOpportunityConfig(state_code='US_ID', opportunity_type='usIdSupervisionLevelDowngrade', display_name='Supervision Level Downgrade', methodology_url='http://fake.com', initial_header='Search for mismatches', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrades all around', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 10}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched', id=3, created_by='barry@revivizens.org', created_at=datetime.datetime(2023, 5, 8, 0, 0), description='base config', status=<OpportunityStatus.ACTIVE: 1>, feature_variant=None)"
    ),
]

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_for_single_type_irrelevant_fvs"
] = {
    "usIdCRCWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCRCWorkRelease', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None)"
    )
}

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_for_single_type_no_fv"
] = {
    "usIdCRCWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCRCWorkRelease', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None)"
    )
}

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_from_multiple_available"
] = {
    "usIdSupervisionLevelDowngrade": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdSupervisionLevelDowngrade', display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrade all of them', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 2}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched')"
    )
}

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_from_multiple_available_no_relevant_fv_set"
] = {
    "usIdSupervisionLevelDowngrade": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdSupervisionLevelDowngrade', display_name='Supervision Level Downgrade', methodology_url='http://fake.com', initial_header='Search for mismatches', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrades all around', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 10}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched')"
    )
}

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_ignores_inactive_configs_even_if_fv_matches"
] = {
    "usIdCRCWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCRCWorkRelease', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None)"
    )
}

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_returns_config_for_each_type"
] = {
    "usIdCRCWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCRCWorkRelease', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None)"
    ),
    "usIdSupervisionLevelDowngrade": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdSupervisionLevelDowngrade', display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrade all of them', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 2}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched')"
    ),
}
