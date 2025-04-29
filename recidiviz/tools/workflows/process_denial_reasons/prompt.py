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
        "JUDGE",
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
        "JUDGE",
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

1. Based on the free text response and selected denial reasons, enumerate all denial reasons from the possible denial reasons for this opportunity that apply to this person.

2. Based on the free text response, assign this case to one or more of the clusters listed above (however many apply), or 'Other' if none apply.

3. Determine if the user's typed response is fully encapsulated by the denial reasons they selected or if it adds new reasons that weren't in the denial reasons they selected (True/False).

4. Determine if the free text reason(s) are fully addressed by available denial reasons for this opportunity (ignoring the denial reason 'Other') (True/False).

If the information suggests multiple reasons (e.g., they both had a recent violation and are getting discharged next week anyways) be sure to select denial reasons and clusters for each reason, even if they're 'Other' for either one. Select as many denial reasons and clusters as apply.

5. Assign a confidence score for each output on a scale of 1 - 10, with 1 being not confident at all and 10 being extremely confident that the output is correct

**Instructions**:

- Return your answer only in valid JSON format.
- Do not include any explanations or additional text.
- Use double quotes ("") for all JSON keys and string values.
- Ensure the JSON is properly formatted.

**Example Output**:

```json
{{
  "assigned_denial_reasons": ["NONCOMPLIANT", "SUSPECTED OFFENSE", "NEEDS"],
  "assigned_denial_reasons_confidence_score": 7
  "assigned_clusters": [{{"Compliance Issues": "Recent/Pending Violations"}}, {{"Compliance Issues": "New Criminal Activity/Charges"}}],
  "assigned_clusters_confidence_score": 6
  "is_fully_encapsulated_by_selected_denial_reasons": false,
  "is_fully_encapsulated_by_selected_denial_reasons_confidence_score": 8,
  "is_encapsulated_by_known_denial_reasons": true
  "is_encapsulated_by_known_denial_reasons_confidence_score": 9
}}
```

Ensure the following:
- Clusters must be chosen from the predefined list of valid clusters. All clusters must have a top-level cluster, and a sub-cluster (even if that sub-cluster is 'Other').
- Denial reasons must be selected from the predefined list for the opportunity type.
- Any reference to "county" or "courts" in the free text should result in assigning the "JUDGE" denial reason, and always use one of the Judge/Court Decisions clusters.

Terminology clarification:
- When assigning denial reasons and clusters, references to violations mean the person is not in compliance with the conditions of their supervision. New criminal activity, new charges, or new offenses all count as a violation, but a violation is not always a new crime or offense and should not be assumed as such.
- If an offense occurred prior to the supervision period, it is not considered a violation. This offense may make a person noncompliant because it is an excluded crime/offense type.
- VOP is violation of parole
- SCRAM is often a requirement for programming/treatment

Example input/output pairs (partial objects):
- Input 1:
```json
{{
"opportunity_type": "usMiMinimumTelephoneReporting",
"free_text_reason": "Bay County case, Bay County forbids telephone reporting",
"selected_denial_reasons": ["Other"],
}}
```
Output 1: 
```json
{{
"assigned_denial_reasons": ["JUDGE","OTHER"],
"assigned_clusters": [{{"Judge/Court Decisions":"Judge's Decision/Policy"}}],
}}
```

- Input 2:
```json
{{
"opportunity_type": "usMiSupervisionLevelDowngrade",
"free_text_reason": "Parolee had a case where violence and shooting victims were involved.",
"selected_denial_reasons": ["Other"],
}}
```
Output 2: 
```json
{{
"assigned_denial_reasons": ["OTHER", "EXCLUDED CHARGE"],
"assigned_clusters": [{{"Compliance Issues":"Excluded Crime/Offense Type"}}],
}}
```

- Input 3:
```json
{{
"opportunity_type": "usMiEarlyDischarge",
"free_text_reason": "Parolee on SCRAM for drinking violations ",
"selected_denial_reasons": ["Other"],
}}
```
Output 3: 
```json
{{
"assigned_denial_reasons": ["OTHER", "NONCOMPLIANT", "ORDERED TREATMENT"],
"assigned_clusters": [{{"Compliance Issues":"Recent/Pending Violations"}}, {{"Compliance Issues": "Incomplete Treatment/Programming"}}],
}}
```

- Input 4:
```json
{{
"opportunity_type": "usMiClassificationReview",
"free_text_reason": "reduced to minimum",
"selected_denial_reasons": ["Other"],
}}
```
Output 4: 
```json
{{
"assigned_denial_reasons": ["OTHER"],
"assigned_clusters": [{{"Time Constraints":"Classification Review Date Set/Near Review Date/Already Done"}}],
}}
```

"""
