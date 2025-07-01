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
        "FullOpportunityConfig(state_code='US_ID', opportunity_type='usIdSupervisionLevelDowngrade', priority='NORMAL', display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', denial_reasons=[{'SOME REASON': 'Some reason text'}], eligible_criteria_copy=[{'key': 'criterion', 'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}], ineligible_criteria_copy=[], dynamic_eligibility_text='client[|s] eligible for SLD', eligibility_date_text='date text', hide_denial_revert=True, tooltip_eligibility_text='tooltip text', call_to_action='Downgrade all of them', subheading='A subheading', snooze={'default_snooze_days': 2, 'max_snooze_days': 30}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched', tab_groups=None, compare_by=[{'field': 'eligibleDate'}], notifications=[], zero_grants_tooltip=None, denied_tab_title=None, denial_adjective=None, denial_noun=None, supports_submitted=True, submitted_tab_title=None, empty_tab_copy={}, tab_preface_copy={}, subcategory_headings={}, subcategory_orderings={}, mark_submitted_options_by_tab={}, oms_criteria_header=None, non_oms_criteria_header=None, non_oms_criteria={}, highlight_cases_on_homepage=False, highlighted_case_cta_copy=None, overdue_opportunity_callout_copy=None, snooze_companion_opportunity_types=[], case_notes_title=None, id=4, created_by='otherBarry@recidiviz.org', created_at=datetime.datetime(2023, 5, 17, 0, 0, tzinfo=datetime.timezone.utc), variant_description='shorter snooze', revision_description='rev', status=<OpportunityStatus.ACTIVE: 1>, feature_variant='otherFeatureVariant', staging_id=None)"
    ),
    GenericRepr(
        "FullOpportunityConfig(state_code='US_ID', opportunity_type='usIdSupervisionLevelDowngrade', priority='NORMAL', display_name='Supervision Level Downgrade', methodology_url='http://fake.com', initial_header='Search for mismatches', denial_reasons=[{'key': 'SOME REASON', 'text': 'Some reason text'}], eligible_criteria_copy=[{'key': 'criterion', 'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}], ineligible_criteria_copy=[], dynamic_eligibility_text='client[|s] eligible for SLD', eligibility_date_text='date text', hide_denial_revert=True, tooltip_eligibility_text='tooltip text', call_to_action='Downgrades all around', subheading='A subheading', snooze={'default_snooze_days': 10, 'max_snooze_days': 30}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched', tab_groups=None, compare_by=[{'field': 'eligibleDate'}], notifications=[], zero_grants_tooltip=None, denied_tab_title=None, denial_adjective=None, denial_noun=None, supports_submitted=True, submitted_tab_title=None, empty_tab_copy={}, tab_preface_copy={}, subcategory_headings={}, subcategory_orderings={}, mark_submitted_options_by_tab={}, oms_criteria_header=None, non_oms_criteria_header=None, non_oms_criteria={}, highlight_cases_on_homepage=False, highlighted_case_cta_copy=None, overdue_opportunity_callout_copy=None, snooze_companion_opportunity_types=[], case_notes_title='Case notes title', id=3, created_by='barry@recidiviz.org', created_at=datetime.datetime(2023, 5, 8, 0, 0, tzinfo=datetime.timezone.utc), variant_description='base config', revision_description='rev', status=<OpportunityStatus.ACTIVE: 1>, feature_variant=None, staging_id=None)"
    ),
]

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_for_single_type_irrelevant_fvs"
] = {
    "usIdCRCWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCRCWorkRelease', priority='NORMAL', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons=[{'key': 'SOME REASON', 'text': 'Some reason text'}], eligible_criteria_copy=[{'key': 'criterion', 'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}], ineligible_criteria_copy=[], dynamic_eligibility_text='client[|s] eligible for WR', eligibility_date_text='date text', hide_denial_revert=False, tooltip_eligibility_text='tooltip text', call_to_action='Approve them all', subheading='A subheading', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None, tab_groups=None, compare_by=[{'field': 'eligibleDate'}], notifications=[], zero_grants_tooltip=None, denied_tab_title=None, denial_adjective=None, denial_noun=None, supports_submitted=True, submitted_tab_title=None, empty_tab_copy={}, tab_preface_copy={}, subcategory_headings={}, subcategory_orderings={}, mark_submitted_options_by_tab={}, oms_criteria_header=None, non_oms_criteria_header=None, non_oms_criteria={}, highlight_cases_on_homepage=False, highlighted_case_cta_copy=None, overdue_opportunity_callout_copy=None, snooze_companion_opportunity_types=[], case_notes_title=None)"
    )
}

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_for_single_type_no_fv"
] = {
    "usIdCRCWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCRCWorkRelease', priority='NORMAL', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons=[{'key': 'SOME REASON', 'text': 'Some reason text'}], eligible_criteria_copy=[{'key': 'criterion', 'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}], ineligible_criteria_copy=[], dynamic_eligibility_text='client[|s] eligible for WR', eligibility_date_text='date text', hide_denial_revert=False, tooltip_eligibility_text='tooltip text', call_to_action='Approve them all', subheading='A subheading', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None, tab_groups=None, compare_by=[{'field': 'eligibleDate'}], notifications=[], zero_grants_tooltip=None, denied_tab_title=None, denial_adjective=None, denial_noun=None, supports_submitted=True, submitted_tab_title=None, empty_tab_copy={}, tab_preface_copy={}, subcategory_headings={}, subcategory_orderings={}, mark_submitted_options_by_tab={}, oms_criteria_header=None, non_oms_criteria_header=None, non_oms_criteria={}, highlight_cases_on_homepage=False, highlighted_case_cta_copy=None, overdue_opportunity_callout_copy=None, snooze_companion_opportunity_types=[], case_notes_title=None)"
    )
}

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_from_multiple_available"
] = {
    "usIdSupervisionLevelDowngrade": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdSupervisionLevelDowngrade', priority='NORMAL', display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', denial_reasons=[{'SOME REASON': 'Some reason text'}], eligible_criteria_copy=[{'key': 'criterion', 'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}], ineligible_criteria_copy=[], dynamic_eligibility_text='client[|s] eligible for SLD', eligibility_date_text='date text', hide_denial_revert=True, tooltip_eligibility_text='tooltip text', call_to_action='Downgrade all of them', subheading='A subheading', snooze={'default_snooze_days': 2, 'max_snooze_days': 30}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched', tab_groups=None, compare_by=[{'field': 'eligibleDate'}], notifications=[], zero_grants_tooltip=None, denied_tab_title=None, denial_adjective=None, denial_noun=None, supports_submitted=True, submitted_tab_title=None, empty_tab_copy={}, tab_preface_copy={}, subcategory_headings={}, subcategory_orderings={}, mark_submitted_options_by_tab={}, oms_criteria_header=None, non_oms_criteria_header=None, non_oms_criteria={}, highlight_cases_on_homepage=False, highlighted_case_cta_copy=None, overdue_opportunity_callout_copy=None, snooze_companion_opportunity_types=[], case_notes_title=None)"
    )
}

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_from_multiple_available_no_relevant_fv_set"
] = {
    "usIdSupervisionLevelDowngrade": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdSupervisionLevelDowngrade', priority='NORMAL', display_name='Supervision Level Downgrade', methodology_url='http://fake.com', initial_header='Search for mismatches', denial_reasons=[{'key': 'SOME REASON', 'text': 'Some reason text'}], eligible_criteria_copy=[{'key': 'criterion', 'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}], ineligible_criteria_copy=[], dynamic_eligibility_text='client[|s] eligible for SLD', eligibility_date_text='date text', hide_denial_revert=True, tooltip_eligibility_text='tooltip text', call_to_action='Downgrades all around', subheading='A subheading', snooze={'default_snooze_days': 10, 'max_snooze_days': 30}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched', tab_groups=None, compare_by=[{'field': 'eligibleDate'}], notifications=[], zero_grants_tooltip=None, denied_tab_title=None, denial_adjective=None, denial_noun=None, supports_submitted=True, submitted_tab_title=None, empty_tab_copy={}, tab_preface_copy={}, subcategory_headings={}, subcategory_orderings={}, mark_submitted_options_by_tab={}, oms_criteria_header=None, non_oms_criteria_header=None, non_oms_criteria={}, highlight_cases_on_homepage=False, highlighted_case_cta_copy=None, overdue_opportunity_callout_copy=None, snooze_companion_opportunity_types=[], case_notes_title='Case notes title')"
    )
}

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_ignores_inactive_configs_even_if_fv_matches"
] = {
    "usIdCRCWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCRCWorkRelease', priority='NORMAL', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons=[{'key': 'SOME REASON', 'text': 'Some reason text'}], eligible_criteria_copy=[{'key': 'criterion', 'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}], ineligible_criteria_copy=[], dynamic_eligibility_text='client[|s] eligible for WR', eligibility_date_text='date text', hide_denial_revert=False, tooltip_eligibility_text='tooltip text', call_to_action='Approve them all', subheading='A subheading', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None, tab_groups=None, compare_by=[{'field': 'eligibleDate'}], notifications=[], zero_grants_tooltip=None, denied_tab_title=None, denial_adjective=None, denial_noun=None, supports_submitted=True, submitted_tab_title=None, empty_tab_copy={}, tab_preface_copy={}, subcategory_headings={}, subcategory_orderings={}, mark_submitted_options_by_tab={}, oms_criteria_header=None, non_oms_criteria_header=None, non_oms_criteria={}, highlight_cases_on_homepage=False, highlighted_case_cta_copy=None, overdue_opportunity_callout_copy=None, snooze_companion_opportunity_types=[], case_notes_title=None)"
    )
}

snapshots[
    "TestWorkflowsQuerier.TestWorkflowsQuerier test_get_top_config_returns_config_for_each_type"
] = {
    "usIdCRCWorkRelease": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdCRCWorkRelease', priority='NORMAL', display_name='Work Release', methodology_url='http://example.com', initial_header='Search for work realease', denial_reasons=[{'key': 'SOME REASON', 'text': 'Some reason text'}], eligible_criteria_copy=[{'key': 'criterion', 'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}], ineligible_criteria_copy=[], dynamic_eligibility_text='client[|s] eligible for WR', eligibility_date_text='date text', hide_denial_revert=False, tooltip_eligibility_text='tooltip text', call_to_action='Approve them all', subheading='A subheading', snooze=None, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text=None, tab_groups=None, compare_by=[{'field': 'eligibleDate'}], notifications=[], zero_grants_tooltip=None, denied_tab_title=None, denial_adjective=None, denial_noun=None, supports_submitted=True, submitted_tab_title=None, empty_tab_copy={}, tab_preface_copy={}, subcategory_headings={}, subcategory_orderings={}, mark_submitted_options_by_tab={}, oms_criteria_header=None, non_oms_criteria_header=None, non_oms_criteria={}, highlight_cases_on_homepage=False, highlighted_case_cta_copy=None, overdue_opportunity_callout_copy=None, snooze_companion_opportunity_types=[], case_notes_title=None)"
    ),
    "usIdSupervisionLevelDowngrade": GenericRepr(
        "OpportunityConfig(state_code='US_ID', opportunity_type='usIdSupervisionLevelDowngrade', priority='NORMAL', display_name='Supervision Level Downgrade', methodology_url='http://other-fake.com', initial_header='Search for mismatches', denial_reasons=[{'SOME REASON': 'Some reason text'}], eligible_criteria_copy=[{'key': 'criterion', 'text': 'Criteria copy: {{someVar}}', 'tooltip': 'Extra copy:{{opportunity.someField.deepField}}'}], ineligible_criteria_copy=[], dynamic_eligibility_text='client[|s] eligible for SLD', eligibility_date_text='date text', hide_denial_revert=True, tooltip_eligibility_text='tooltip text', call_to_action='Downgrade all of them', subheading='A subheading', snooze={'default_snooze_days': 2, 'max_snooze_days': 30}, is_alert=False, sidebar_components=['aComponent', 'anotherComponent'], denial_text='Leave mismatched', tab_groups=None, compare_by=[{'field': 'eligibleDate'}], notifications=[], zero_grants_tooltip=None, denied_tab_title=None, denial_adjective=None, denial_noun=None, supports_submitted=True, submitted_tab_title=None, empty_tab_copy={}, tab_preface_copy={}, subcategory_headings={}, subcategory_orderings={}, mark_submitted_options_by_tab={}, oms_criteria_header=None, non_oms_criteria_header=None, non_oms_criteria={}, highlight_cases_on_homepage=False, highlighted_case_cta_copy=None, overdue_opportunity_callout_copy=None, snooze_companion_opportunity_types=[], case_notes_title=None)"
    ),
}
