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
        "OpportunityConfig(id=2, state_code='US_ID', opportunity_type='usIdCrcWorkRelease', created_by='bob@recivizens.org', created_at=datetime.datetime(2023, 5, 3, 0, 0), description='base config', status=<OpportunityStatus.ACTIVE: 1>, display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, feature_variant=None, is_alert=False, denial_text=None)"
    ),
    GenericRepr(
        "OpportunityConfig(id=3, state_code='US_ID', opportunity_type='usIdSLD', created_by='barry@revivizens.org', created_at=datetime.datetime(2023, 5, 8, 0, 0), description='base config', status=<OpportunityStatus.ACTIVE: 1>, display_name='Supervision Level Downgrade', methodology_url='http://fake.com', initial_header='Search for mismatches', dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrades all around', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 10}, feature_variant=None, is_alert=False, denial_text='Leave mismatched')"
    ),
    GenericRepr(
        "OpportunityConfig(id=4, state_code='US_ID', opportunity_type='usIdSLD', created_by='otherBarry@revivizens.org', created_at=datetime.datetime(2023, 5, 17, 0, 0), description='shorter snooze', status=<OpportunityStatus.ACTIVE: 1>, display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrade all of them', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 2}, feature_variant='otherFeatureVariant', is_alert=False, denial_text='Leave mismatched')"
    ),
]

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_for_single_type_irrelevant_fvs"
] = {
    "usIdCrcWorkRelease": GenericRepr(
        "OpportunityConfig(id=2, state_code='US_ID', opportunity_type='usIdCrcWorkRelease', created_by='bob@recivizens.org', created_at=datetime.datetime(2023, 5, 3, 0, 0), description='base config', status=<OpportunityStatus.ACTIVE: 1>, display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, feature_variant=None, is_alert=False, denial_text=None)"
    )
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_for_single_type_no_fv"
] = {
    "usIdCrcWorkRelease": GenericRepr(
        "OpportunityConfig(id=2, state_code='US_ID', opportunity_type='usIdCrcWorkRelease', created_by='bob@recivizens.org', created_at=datetime.datetime(2023, 5, 3, 0, 0), description='base config', status=<OpportunityStatus.ACTIVE: 1>, display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, feature_variant=None, is_alert=False, denial_text=None)"
    )
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_from_multiple_available"
] = {
    "usIdSLD": GenericRepr(
        "OpportunityConfig(id=4, state_code='US_ID', opportunity_type='usIdSLD', created_by='otherBarry@revivizens.org', created_at=datetime.datetime(2023, 5, 17, 0, 0), description='shorter snooze', status=<OpportunityStatus.ACTIVE: 1>, display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrade all of them', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 2}, feature_variant='otherFeatureVariant', is_alert=False, denial_text='Leave mismatched')"
    )
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_from_multiple_available_no_relevant_fv_set"
] = {
    "usIdSLD": GenericRepr(
        "OpportunityConfig(id=3, state_code='US_ID', opportunity_type='usIdSLD', created_by='barry@revivizens.org', created_at=datetime.datetime(2023, 5, 8, 0, 0), description='base config', status=<OpportunityStatus.ACTIVE: 1>, display_name='Supervision Level Downgrade', methodology_url='http://fake.com', initial_header='Search for mismatches', dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrades all around', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 10}, feature_variant=None, is_alert=False, denial_text='Leave mismatched')"
    )
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_ignores_inactive_configs_even_if_fv_matches"
] = {
    "usIdCrcWorkRelease": GenericRepr(
        "OpportunityConfig(id=2, state_code='US_ID', opportunity_type='usIdCrcWorkRelease', created_by='bob@recivizens.org', created_at=datetime.datetime(2023, 5, 3, 0, 0), description='base config', status=<OpportunityStatus.ACTIVE: 1>, display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, feature_variant=None, is_alert=False, denial_text=None)"
    )
}

snapshots[
    "TestOutliersQuerier.TestOutliersQuerier test_get_top_config_returns_config_for_each_type"
] = {
    "usIdCrcWorkRelease": GenericRepr(
        "OpportunityConfig(id=2, state_code='US_ID', opportunity_type='usIdCrcWorkRelease', created_by='bob@recivizens.org', created_at=datetime.datetime(2023, 5, 3, 0, 0), description='base config', status=<OpportunityStatus.ACTIVE: 1>, display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', dynamic_eligibility_text='client[|s] eligible for WR', call_to_action='Approve them all', snooze=None, feature_variant=None, is_alert=False, denial_text=None)"
    ),
    "usIdSLD": GenericRepr(
        "OpportunityConfig(id=4, state_code='US_ID', opportunity_type='usIdSLD', created_by='otherBarry@revivizens.org', created_at=datetime.datetime(2023, 5, 17, 0, 0), description='shorter snooze', status=<OpportunityStatus.ACTIVE: 1>, display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', dynamic_eligibility_text='client[|s] eligible for SLD', call_to_action='Downgrade all of them', snooze={'maxSnoozeDays': 30, 'defaultSnoozeDays': 2}, feature_variant='otherFeatureVariant', is_alert=False, denial_text='Leave mismatched')"
    ),
}
