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
"""Defines segment event names and their associated product types."""
from collections import defaultdict

from recidiviz.segment.product_type import ProductType


def get_segment_event_types_by_product() -> dict[ProductType, list[str]]:
    """Collects event types by product type."""
    event_types_by_product = defaultdict(list)
    for event, product_types in SEGMENT_EVENT_NAME_TO_RELEVANT_PRODUCTS.items():
        for product_type in product_types:
            event_types_by_product[product_type].append(event)

    return event_types_by_product


SEGMENT_EVENT_NAME_TO_RELEVANT_PRODUCTS = {
    "frontend_almost_eligible_copy_cta_clicked": [
        ProductType.CLIENT_PAGE,
        ProductType.WORKFLOWS,
    ],
    "frontend_almost_eligible_copy_cta_viewed": [
        ProductType.CLIENT_PAGE,
        ProductType.WORKFLOWS,
    ],
    "frontend_case_note_search": [ProductType.CASE_NOTE_SEARCH],
    "frontend_case_note_search_note_clicked": [ProductType.CASE_NOTE_SEARCH],
    "frontend_caseload_search": [
        ProductType.CLIENT_PAGE,
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.TASKS,
        ProductType.WORKFLOWS,
    ],
    "frontend_direct_download_form_downloaded": [ProductType.WORKFLOWS],
    "frontend_methodology_link_clicked": [
        ProductType.SUPERVISOR_HOMEPAGE_OPERATIONS_MODULE,
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE,
        ProductType.TASKS,
        ProductType.WORKFLOWS,
    ],
    "frontend_milestones_congratulations_sent": [ProductType.MILESTONES],
    "frontend_milestones_message_declined": [ProductType.MILESTONES],
    "frontend_milestones_tab_clicked": [ProductType.MILESTONES],
    "frontend_opportunity_marked_eligible": [
        ProductType.CLIENT_PAGE,
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_opportunity_marked_submitted": [
        ProductType.CLIENT_PAGE,
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_opportunity_previewed": [
        ProductType.CLIENT_PAGE,
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.TASKS,
        ProductType.WORKFLOWS,
    ],
    "frontend_opportunity_snooze_case_note_copied": [ProductType.WORKFLOWS],
    "frontend_opportunity_snoozed": [
        ProductType.CLIENT_PAGE,
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_opportunity_status_updated": [
        ProductType.CLIENT_PAGE,
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_opportunity_tab_clicked": [
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_opportunity_tab_order_changed": [
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_opportunity_table_view_preference_changed": [
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_opportunity_unsubmitted": [
        ProductType.CLIENT_PAGE,
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_outliers_action_strategy_list_viewed": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_action_strategy_popup_viewed": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_action_strategy_popup_viewed_10_seconds": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_action_strategy_popup_viewed_from_list": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_action_strategy_surfaced": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_client_page_viewed": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_page_viewed_30_seconds": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_roster_change_request_form_submitted": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_staff_metric_viewed": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_staff_page_viewed": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_supervisor_page_viewed": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_supervisor_roster_modal_viewed": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_outliers_supervisors_list_page_viewed": [
        ProductType.SUPERVISOR_HOMEPAGE_OUTCOMES_MODULE
    ],
    "frontend_person_id_copied_to_clipboard": [
        ProductType.CLIENT_PAGE,
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.TASKS,
        ProductType.WORKFLOWS,
    ],
    "frontend_profile_opportunity_link_clicked": [ProductType.WORKFLOWS],
    "frontend_profile_viewed": [ProductType.CLIENT_PAGE],
    "frontend_referral_form_copied_to_clipboard": [
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_referral_form_downloaded": [
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_referral_form_edited": [
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_referral_form_first_edited": [
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_referral_form_printed": [
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_referral_form_submitted": [
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_referral_form_viewed": [
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_sentencing_add_opportunity_to_recommendation_clicked": [
        ProductType.PSI_CASE_INSIGHTS
    ],
    "frontend_sentencing_case_details_page_viewed": [ProductType.PSI_CASE_INSIGHTS],
    "frontend_sentencing_case_status_complete_clicked": [ProductType.PSI_CASE_INSIGHTS],
    "frontend_sentencing_copy_summary_to_clipboard_clicked": [
        ProductType.PSI_CASE_INSIGHTS
    ],
    "frontend_sentencing_create_or_update_recommendation_clicked": [
        ProductType.PSI_CASE_INSIGHTS
    ],
    "frontend_sentencing_dashboard_page_viewed": [ProductType.PSI_CASE_INSIGHTS],
    "frontend_sentencing_dashboard_sort_order_changed": [ProductType.PSI_CASE_INSIGHTS],
    "frontend_sentencing_download_report_clicked": [ProductType.PSI_CASE_INSIGHTS],
    "frontend_sentencing_edit_case_details_clicked": [ProductType.PSI_CASE_INSIGHTS],
    "frontend_sentencing_individual_case_clicked": [ProductType.PSI_CASE_INSIGHTS],
    "frontend_sentencing_onboarding_page_viewed": [ProductType.PSI_CASE_INSIGHTS],
    "frontend_sentencing_opportunity_modal_opened": [ProductType.PSI_CASE_INSIGHTS],
    "frontend_sentencing_recommendation_status_filter_changed": [
        ProductType.PSI_CASE_INSIGHTS
    ],
    "frontend_sentencing_recommended_disposition_changed": [
        ProductType.PSI_CASE_INSIGHTS
    ],
    "frontend_sentencing_remove_opportunity_from_recommendation_clicked": [
        ProductType.PSI_CASE_INSIGHTS
    ],
    "frontend_surfaced_in_list": [
        ProductType.CLIENT_PAGE,
        ProductType.SUPERVISOR_HOMEPAGE_OPPORTUNITIES_MODULE,
        ProductType.WORKFLOWS,
    ],
    "frontend_task_filter_dropdown_opened": [ProductType.TASKS],
    "frontend_task_filter_selected": [ProductType.TASKS],
    "frontend_task_header_toggled": [ProductType.TASKS],
    "frontend_task_table_category_selected": [ProductType.TASKS],
    "frontend_tasks_filter_changed": [ProductType.TASKS],
    "frontend_tasks_filters_cleared": [ProductType.TASKS],
    "frontend_tasks_filters_reset": [ProductType.TASKS],
    "frontend_tasks_previewed": [ProductType.TASKS],
    "frontend_tasks_view_changed": [ProductType.TASKS],
    "frontend_us_ia_early_discharge_opportunity_actions": [
        ProductType.CLIENT_PAGE,
        ProductType.WORKFLOWS,
    ],
}
