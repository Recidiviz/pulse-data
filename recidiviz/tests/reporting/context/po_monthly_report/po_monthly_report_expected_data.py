# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2021 Recidiviz, Inc.
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
"""Snapshots of expected po_monthly_report context data based on fixture input"""

from recidiviz.common.constants.states import StateCode

expected_us_id = {
    "state_code": StateCode.US_ID,
    "email_address": "letter@kenny.ca",
    "review_month": "May",
    "review_year": "2021",
    "officer_given_name": "CHRISTOPHER",
    "officer_external_id": "CMADEUPNAME",
    "district": "DISTRICT OFFICE 3, CALDWELL",
    # successful completions
    "pos_discharges_clients": None,
    "pos_discharges": {
        "heading": "Successful Completions",
        "icon": "ic_case-completions-v2.png",
        "main_text": "{} people completed supervision in Idaho this month.",
        "total": 273,
        "supplemental_text": "5 from your caseload",
        "action_text": "These clients are within 30 days of their full-term release date:",
        "action_table": [
            ("Hansen, Linet (105)", "June 7"),
            ("Cortes, Rebekah (142)", "June 18"),
        ],
    },
    "pos_discharges_last_month": "3",
    "pos_discharges_district_average": "1.14765",
    "pos_discharges_state_average": "0.945637",
    "pos_discharges_district_total": 38,
    "pos_discharges_state_total": 273,
    # early releases
    "earned_discharges_clients": None,
    "earned_discharges": {
        "heading": "Early Releases",
        "icon": "ic_early-discharges-v2.png",
        "main_text": "{} early discharge requests were filed across Idaho.",
        "total": 106,
        "supplemental_text": "1 from your caseload",
        "action_text": None,
        "action_table": None,
    },
    "earned_discharges_last_month": "3",
    "earned_discharges_district_average": "0.8575756",
    "earned_discharges_state_average": "1.6567567",
    "earned_discharges_district_total": 18,
    "earned_discharges_state_total": 106,
    # supervision downgrades
    "supervision_downgrades_clients": None,
    "supervision_downgrades": {
        "heading": "Supervision Downgrades",
        "icon": "ic_supervision-downgrades-v2.png",
        "main_text": "{} clients had their supervision downgraded this month.",
        "total": 314,
        "supplemental_text": "5 from your caseload",
        "action_text": "These clients may be downgraded based on their latest assessment:",
        "action_table": [
            ("Tonye Thompson (189472)", "Medium &rarr; Low"),
            ("Linet Hansen (47228)", "Medium &rarr; Low"),
            ("Rebekah Cortes (132878)", "High &rarr; Medium"),
            ("Taryn Berry (147872)", "High &rarr; Low"),
        ],
    },
    "supervision_downgrades_last_month": "3",
    "supervision_downgrades_district_average": "2.3456789",
    "supervision_downgrades_state_average": "1.765432",
    "supervision_downgrades_district_total": 51,
    "supervision_downgrades_state_total": 314,
    # revocations
    "revocations_clients": None,
    "technical_revocations": {"count": 0, "label": "Technical Revocations"},
    "technical_revocations_zero_streak": 1,
    "technical_revocations_last_month": "1",
    "technical_revocations_district_average": "2.022",
    "technical_revocations_state_average": "2.095",
    "crime_revocations": {"count": 2, "label": "New Crime Revocations"},
    "crime_revocations_zero_streak": 0,
    "crime_revocations_last_month": "3",
    "crime_revocations_district_average": "3.353",
    "crime_revocations_state_average": "3.542",
    # absconsions
    "absconsions_clients": None,
    "absconsions": {
        "amount_above_average": 1.77654655,
        "count": 2,
        "label": "Absconsions",
    },
    "absconsions_zero_streak": 0,
    "absconsions_last_month": "1",
    "absconsions_district_average": "0.22",
    "absconsions_state_average": "0.14",
    # compliance
    "assessments_out_of_date_clients": None,
    "assessments": "15",
    "assessments_percent": "73",
    "overdue_assessments_goal": "3",
    "overdue_assessments_goal_percent": "81",
    "assessments_goal_enabled": True,
    "assessments_goal_met": False,
    "facetoface_out_of_date_clients": None,
    "facetoface": "0",
    "facetoface_percent": "N/A",
    "overdue_facetoface_goal": "9",
    "overdue_facetoface_goal_percent": "N/A",
    "facetoface_goal_enabled": False,
    "facetoface_goal_met": False,
    # other data
    "batch_id": "20201105123033",
    "static_image_path": "http://123.456.7.8/US_ID/po_monthly_report/static",
    "greeting": "Hey there, Christopher!",
    "learn_more_link": "https://docs.google.com/document/d/1kgG5LiIrFQaBupHYfoIwo59TCmYH5f_aIpRzGrtOkhU/edit#heading=h.r6s5tyc7ut6c",
    "message_body": "We’re here to make your life a bit easier by helping you keep track of your caseload's health. You'll receive this email once a month as a customized, personal check-in just for you.",
    "display_congratulations": "inherit",
    "congratulations_text": "You improved from last month across 4 metrics and out-performed other officers like you across 5 metrics.",
    "attachment_content": None,
    "mismatches": [
        {
            "name": "Tonye Thompson",
            "person_external_id": "189472",
            "last_score": 14,
            "last_assessment_date": "10/12/20",
            "current_supervision_level": "Medium",
            "recommended_level": "Low",
        },
        {
            "name": "Linet Hansen",
            "person_external_id": "47228",
            "last_assessment_date": "1/12/21",
            "last_score": 8,
            "current_supervision_level": "Medium",
            "recommended_level": "Low",
        },
        {
            "name": "Rebekah Cortes",
            "person_external_id": "132878",
            "last_assessment_date": "3/14/20",
            "last_score": 10,
            "current_supervision_level": "High",
            "recommended_level": "Medium",
        },
        {
            "name": "Taryn Berry",
            "person_external_id": "147872",
            "last_assessment_date": "3/13/20",
            "last_score": 4,
            "current_supervision_level": "High",
            "recommended_level": "Low",
        },
    ],
    "upcoming_release_date_clients": [
        {
            "full_name": "Hansen, Linet",
            "person_external_id": "105",
            "projected_end_date": "2021-06-07",
        },
        {
            "full_name": "Cortes, Rebekah",
            "person_external_id": "142",
            "projected_end_date": "2021-06-18",
        },
    ],
}

expected_us_pa = {
    "state_code": StateCode.US_PA,
    "email_address": "letter@kenny.ca",
    "review_month": "May",
    "review_year": "2021",
    "officer_given_name": "CHRISTOPHER",
    "officer_external_id": "CMADEUPNAME",
    "district": "DISTRICT OFFICE 3, CALDWELL",
    # successful completions
    "pos_discharges_clients": None,
    "pos_discharges": {
        "heading": "Successful Completions",
        "icon": "ic_case-completions-v2.png",
        "main_text": "{} people completed supervision in Pennsylvania this month.",
        "total": 273,
        "supplemental_text": "5 from your caseload",
        "action_text": "These clients are within 30 days of their max date:",
        "action_table": [
            ("Hansen, Linet (105)", "June 7"),
            ("Cortes, Rebekah (142)", "June 18"),
        ],
    },
    "pos_discharges_last_month": "3",
    "pos_discharges_district_average": "1.14765",
    "pos_discharges_state_average": "0.945637",
    "pos_discharges_district_total": 38,
    "pos_discharges_state_total": 273,
    # early releases
    "earned_discharges_clients": None,
    "earned_discharges": "1",
    "earned_discharges_last_month": "3",
    "earned_discharges_district_average": "0.8575756",
    "earned_discharges_state_average": "1.6567567",
    "earned_discharges_district_total": 18,
    "earned_discharges_state_total": 106,
    # supervision downgrades
    "supervision_downgrades_clients": None,
    "supervision_downgrades": {
        "heading": "Supervision Downgrades",
        "icon": "ic_supervision-downgrades-v2.png",
        "main_text": "{} clients had their supervision downgraded this month.",
        "total": 314,
        "supplemental_text": "5 from your caseload",
        "action_text": "These clients may be downgraded based on their latest assessment:",
        "action_table": [
            ("Tonye Thompson (189472)", "Medium &rarr; Low"),
            ("Linet Hansen (47228)", "Medium &rarr; Low"),
            ("Rebekah Cortes (132878)", "High &rarr; Medium"),
            ("Taryn Berry (147872)", "High &rarr; Low"),
        ],
    },
    "supervision_downgrades_last_month": "3",
    "supervision_downgrades_district_average": "2.3456789",
    "supervision_downgrades_state_average": "1.765432",
    "supervision_downgrades_district_total": 51,
    "supervision_downgrades_state_total": 314,
    # revocations
    "revocations_clients": None,
    "technical_revocations": {"count": 0, "label": "Technical Revocations"},
    "technical_revocations_last_month": "1",
    "technical_revocations_district_average": "2.022",
    "technical_revocations_state_average": "2.095",
    "technical_revocations_zero_streak": 1,
    "crime_revocations": {"count": 2, "label": "New Crime Revocations"},
    "crime_revocations_last_month": "3",
    "crime_revocations_district_average": "3.353",
    "crime_revocations_state_average": "3.542",
    "crime_revocations_zero_streak": 0,
    # absconsions
    "absconsions_clients": None,
    "absconsions": {
        "amount_above_average": 1.77654655,
        "count": 2,
        "label": "Absconsions",
    },
    "absconsions_last_month": "1",
    "absconsions_district_average": "0.22",
    "absconsions_state_average": "0.14",
    "absconsions_zero_streak": 0,
    # compliance
    "assessments_out_of_date_clients": None,
    "assessments": "15",
    "assessments_percent": "73",
    "overdue_assessments_goal": "3",
    "overdue_assessments_goal_percent": "81",
    "assessments_goal_enabled": True,
    "assessments_goal_met": False,
    "facetoface_out_of_date_clients": None,
    "facetoface": "0",
    "facetoface_percent": "N/A",
    "overdue_facetoface_goal": "9",
    "overdue_facetoface_goal_percent": "N/A",
    "facetoface_goal_enabled": False,
    "facetoface_goal_met": False,
    # other data
    "batch_id": "20201105123033",
    "static_image_path": "http://123.456.7.8/US_PA/po_monthly_report/static",
    "greeting": "Hey there, Christopher!",
    "learn_more_link": "https://docs.google.com/document/d/1kgG5LiIrFQaBupHYfoIwo59TCmYH5f_aIpRzGrtOkhU/edit#heading=h.r6s5tyc7ut6c",
    "message_body": "We’re here to make your life a bit easier by helping you keep track of your caseload's health. You'll receive this email once a month as a customized, personal check-in just for you.",
    "display_congratulations": "inherit",
    "congratulations_text": "You improved from last month across 4 metrics and out-performed other officers like you across 4 metrics.",
    "attachment_content": None,
    "mismatches": [
        {
            "name": "Tonye Thompson",
            "person_external_id": "189472",
            "last_score": 14,
            "last_assessment_date": "10/12/20",
            "current_supervision_level": "Medium",
            "recommended_level": "Low",
        },
        {
            "name": "Linet Hansen",
            "person_external_id": "47228",
            "last_assessment_date": "1/12/21",
            "last_score": 8,
            "current_supervision_level": "Medium",
            "recommended_level": "Low",
        },
        {
            "name": "Rebekah Cortes",
            "person_external_id": "132878",
            "last_assessment_date": "3/14/20",
            "last_score": 10,
            "current_supervision_level": "High",
            "recommended_level": "Medium",
        },
        {
            "name": "Taryn Berry",
            "person_external_id": "147872",
            "last_assessment_date": "3/13/20",
            "last_score": 4,
            "current_supervision_level": "High",
            "recommended_level": "Low",
        },
    ],
    "upcoming_release_date_clients": [
        {
            "full_name": "Hansen, Linet",
            "person_external_id": "105",
            "projected_end_date": "2021-06-07",
        },
        {
            "full_name": "Cortes, Rebekah",
            "person_external_id": "142",
            "projected_end_date": "2021-06-18",
        },
    ],
}
