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
    "TestOutliersQuerier.TestOutliersQuerier test_get_active_configs_for_multiple_opportunity_types"
] = [
    GenericRepr(
        "FullOpportunityConfig(state_code='US_ID', opportunity_type='usIdCrcWorkRelease', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'copy': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None, id=2, created_by='bob@recivizens.org', created_at=datetime.datetime(2023, 5, 3, 0, 0), description='base config', status=<OpportunityStatus.ACTIVE: 1>, feature_variant=None)"
    ),
    GenericRepr(
        "FullOpportunityConfig(state_code='US_ID', opportunity_type='usIdSLD', display_name='Supervision Level Downgrade', methodology_url='http://fake.com', initial_header='Search for mismatches', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'copy': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrades all around', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 10}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched', id=3, created_by='barry@revivizens.org', created_at=datetime.datetime(2023, 5, 8, 0, 0), description='base config', status=<OpportunityStatus.ACTIVE: 1>, feature_variant=None)"
    ),
    GenericRepr(
        "FullOpportunityConfig(state_code='US_ID', opportunity_type='usIdSLD', display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'copy': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrade all of them', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 2}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched', id=4, created_by='otherBarry@revivizens.org', created_at=datetime.datetime(2023, 5, 17, 0, 0), description='shorter snooze', status=<OpportunityStatus.ACTIVE: 1>, feature_variant='otherFeatureVariant')"
    ),
]

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_for_single_type_irrelevant_fvs"
] = {
    "usIdCrcWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCrcWorkRelease', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'copy': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None)"
    )
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_for_single_type_no_fv"
] = {
    "usIdCrcWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCrcWorkRelease', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'copy': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None)"
    )
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_from_multiple_available"
] = {
    "usIdSLD": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdSLD', display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'copy': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrade all of them', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 2}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched')"
    )
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_from_multiple_available_no_relevant_fv_set"
] = {
    "usIdSLD": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdSLD', display_name='Supervision Level Downgrade', methodology_url='http://fake.com', initial_header='Search for mismatches', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'copy': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrades all around', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 10}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched')"
    )
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_ignores_inactive_configs_even_if_fv_matches"
] = {
    "usIdCrcWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCrcWorkRelease', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'copy': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None)"
    )
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_returns_config_for_each_type"
] = {
    "usIdCrcWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCrcWorkRelease', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'copy': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None)"
    ),
    "usIdSLD": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdSLD', display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', denial_reasons={'SOME REASON': 'Some reason text'}, eligible_criteria_copy={'criterion': {'copy': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}}, ineligible_criteria_copy={}, dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrade all of them', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 2}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched')"
    ),
}
