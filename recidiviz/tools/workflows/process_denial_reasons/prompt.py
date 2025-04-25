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
"""
Prompt provided to the Vertex AI to process the denial reasons in process_denial_reasons.py
"""

# Define the possible denial reasons per opportunity type
possible_denial_reasons_dict = {
    "usMiEarlyDischarge": [
        "CHILD ABUSE ORDER",
        "SUSPECTED OFFENSE",
        "FELONY/STATE PROBATION",
        "NEEDS",
        "NONCOMPLIANT",
        "PROGRAMMING",
        "PRO-SOCIAL",
        "RESTITUTION",
        "FINES & FEES",
        "PENDING CHARGES",
        "ORDERED TREATMENT",
        "EXCLUDED OFFENSE",
        "COURT",
        "OTHER",
    ],
    "usMiClassificationReview": [
        "VIOLATIONS",
        "EMPLOYMENT",
        "FINES & FEES",
        "CASE PLAN",
        "NONCOMPLIANT",
        "ABSCONSION",
        "OTHER",
    ],
    "usMiMinimumTelephoneReporting": [
        "FIREARM",
        "SPEC COURT",
        "RPOSN",
        "HIGH PROFILE",
        "COURT",
        "OTHER",
    ],
    "usMiSupervisionLevelDowngrade": ["OVERRIDE", "EXCLUDED CHARGE", "OTHER"],
}

# Define the prompt template
prompt_template = """
Someone was denied an opportunity on community supervision (like supervision level downgrade, or earned discharge, etc.).

Given the following information:

- Opportunity Type: "{opportunity_type}"
- Plain text reason why the case worker denied the person this opportunity: "{free_text_reason}"
- The case worker's selected denial reasons, from a multi-select: {selected_denial_reasons}

The possible denial reasons for this opportunity are:
{possible_denial_reasons}

The clusters for this opportunity are:
{clusters_formatted}

Please perform the following tasks:

1. Based on the free text response and selected denial reasons, enumerate all denial reasons for this opportunity that apply to this person.

2. Based on the free text response, assign this case to one or more of the clusters listed above (however many apply), or 'Other' if none apply.

3. Determine if the user's typed response is fully encapsulated by the denial reasons they selected or if it adds new reasons that weren't in the denial reasons they selected (True/False).

4. Determine if the free text reason(s) are fully addressed by available denial reasons for this opportunity (ignoring the denial reason 'Other') (True/False).

If the information suggests multiple reasons (e.g., they both had a recent violation and are getting discharged next week anyways) be sure to select denial reasons and clusters for each reason, even if they're 'Other' for either one. Select as many denial reasons and clusters as apply.

**Instructions**:

- Return your answer only in valid JSON format.
- Do not include any explanations or additional text.
- Use double quotes ("") for all JSON keys and string values.
- Ensure the JSON is properly formatted.

**Example Output**:

```json
{{
  "assigned_denial_reasons": ["NONCOMPLIANT", "SUSPECTED OFFENSE", "NEEDS"],
  "assigned_clusters": [{{"Compliance Issues": "Recent/Pending Violations"}}, {{"Compliance Issues": "New Criminal Activity/Charges"}}],
  "is_fully_encapsulated_by_selected_denial_reasons": false,
  "is_encapsulated_by_known_denial_reasons": true
}}

Ensure the following:
- Clusters must be chosen from the predefined list of valid clusters. All clusters must have a top-level cluster, and a sub-cluster (even if that sub-cluster is 'Other').
- Denial reasons must be selected from the predefined list for the opportunity type.

"""
