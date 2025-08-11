"""	
Snapshots for recidiviz/tests/admin_panel/routes/outliers_test.py	
Update snapshots automatically by running `pytest recidiviz/tests/admin_panel/routes/outliers_test.py --snapshot-update	
Remember to include a docstring like this after updating the snapshots for Pylint purposes	
"""

# -*- coding: utf-8 -*-
# snapshottest: v1 - https://goo.gl/zC4yUc
from __future__ import unicode_literals

from snapshottest import Snapshot

snapshots = Snapshot()

snapshots[
    "OutliersAdminPanelEndpointTests.OutliersAdminPanelEndpointTests test_add_configuration"
] = [
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because x",
        "featureVariant": None,
        "id": 1,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "agent",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-26T13:30:00",
        "updatedBy": "alexa@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because a",
        "featureVariant": "fv2",
        "id": 4,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:03",
        "updatedBy": "dana2@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because z",
        "featureVariant": "fv1",
        "id": 3,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:02",
        "updatedBy": "dana1@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because y",
        "featureVariant": None,
        "id": 2,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:01",
        "updatedBy": "fake@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because b",
        "featureVariant": "fv2",
        "id": 5,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer-fv2",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:00:03",
        "updatedBy": "dana2@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
]

snapshots[
    "OutliersAdminPanelEndpointTests.OutliersAdminPanelEndpointTests test_add_configuration_with_updated_by"
] = [
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because x",
        "featureVariant": None,
        "id": 1,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "agent",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-26T13:30:00",
        "updatedBy": "alexa@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because a",
        "featureVariant": "fv2",
        "id": 4,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:03",
        "updatedBy": "dana2@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because z",
        "featureVariant": "fv1",
        "id": 3,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:02",
        "updatedBy": "dana1@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because y",
        "featureVariant": None,
        "id": 2,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:01",
        "updatedBy": "fake@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because b",
        "featureVariant": "fv2",
        "id": 5,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer-fv2",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:00:03",
        "updatedBy": "dana2@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
]

snapshots[
    "OutliersAdminPanelEndpointTests.OutliersAdminPanelEndpointTests test_get_configurations"
] = [
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because x",
        "featureVariant": None,
        "id": 1,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "agent",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-26T13:30:00",
        "updatedBy": "alexa@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because a",
        "featureVariant": "fv2",
        "id": 4,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:03",
        "updatedBy": "dana2@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because z",
        "featureVariant": "fv1",
        "id": 3,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:02",
        "updatedBy": "dana1@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because y",
        "featureVariant": None,
        "id": 2,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:01",
        "updatedBy": "fake@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because b",
        "featureVariant": "fv2",
        "id": 5,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer-fv2",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:00:03",
        "updatedBy": "dana2@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
]

snapshots[
    "OutliersAdminPanelEndpointTests.OutliersAdminPanelEndpointTests test_promote_default_configuration_success"
] = [
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because z",
        "featureVariant": None,
        "id": 6,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-02-01T00:00:00",
        "updatedBy": "test-user@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because x",
        "featureVariant": None,
        "id": 1,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "agent",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-26T13:30:00",
        "updatedBy": "alexa@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because a",
        "featureVariant": "fv2",
        "id": 4,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:03",
        "updatedBy": "dana2@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because z",
        "featureVariant": "fv1",
        "id": 3,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:02",
        "updatedBy": "dana1@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because y",
        "featureVariant": None,
        "id": 2,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:01",
        "updatedBy": "fake@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because b",
        "featureVariant": "fv2",
        "id": 5,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer-fv2",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:00:03",
        "updatedBy": "dana2@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
]

snapshots[
    "OutliersAdminPanelEndpointTests.OutliersAdminPanelEndpointTests test_reactivate_configuration_success"
] = [
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because b",
        "featureVariant": "fv2",
        "id": 6,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer-fv2",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-02-01T00:00:00",
        "updatedBy": "test-user@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because x",
        "featureVariant": None,
        "id": 1,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "agent",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-26T13:30:00",
        "updatedBy": "alexa@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because a",
        "featureVariant": "fv2",
        "id": 4,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:03",
        "updatedBy": "dana2@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because z",
        "featureVariant": "fv1",
        "id": 3,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "ACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:02",
        "updatedBy": "dana1@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because y",
        "featureVariant": None,
        "id": 2,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:30:01",
        "updatedBy": "fake@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
    {
        "abscondersLabel": "absconders",
        "actionStrategyCopy": {
            "ACTION_STRATEGY_60_PERC_OUTLIERS": {
                "body": """Try setting positive, collective goals with your team:
1. After some investigation, arrange a meeting with your team to engage in a comprehensive discussion about their strengths, challenges, and metrics.
2. Prepare a well-structured agenda and establish clear objectives for the meeting. Additionally, come prepared with inquiries for your staff, as well as be open to addressing any questions they may have.
3. Collaborate as a team to brainstorm innovative approaches for overcoming challenges and improving upon any outliers in the metrics.
4. Establish SMART (Specific, Measurable, Achievable, Relevant, Time-bound) goals together with your team for future endeavors and devise a plan to effectively monitor their progress. Ensure that these goals are communicated and easily accessible to all team members.
5. Foster an environment of open communication and actively encourage the implementation of the strategies and plans that have been established for moving forward.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I work with my team to improve these metrics?",
            },
            "ACTION_STRATEGY_OUTLIER": {
                "body": """Try conducting case reviews and direct observations:
1. Gather additional information on how agents do their work to inform how you approach the staff member, where there are gaps in client or staff resources, and where additional agent training could help.
2. Conduct case reviews to ascertain whether outlying agents are consistently following agency policy and practice expectations; using the strategies and techniques that have been identified as most effective in meeting agency goals (e.g., evidence-based practices); and delivering services in the manner intended. Consider using cases listed in the tool for the agent's 3 self-assessments/case management reviews this quarter.
4. Conduct direct observations of in-person staff/client meetings to determine the quality of interactions with clients and how agents are building rapport and using strategies that promote behavior change.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I investigate what is driving this metric?",
            },
            "ACTION_STRATEGY_OUTLIER_3_MONTHS": {
                "body": """First, investigate: Conduct further case reviews or direct observations along with using the Lantern Insights tool to make sure that you understand the agent's caseload, trends, and approach. Other strategies to better investigate behind the metrics are here.
After investigating, try having a positive meeting 1:1 with the agent:
1. Establish a meeting atmosphere that fosters open communication. Ensure that your agent comprehends the purpose behind this coaching conversation - improving future client outcomes.
2. Customize the discussion to cater to the individual needs and growth of the agent you are engaging with.
3. Utilize positive reinforcement and subtle prompts to demonstrate attentive listening.
4. Collaborate on generating ideas to reduce outlier metrics and improve overall performance of the officer.
5. If needed, schedule regular meetings and formulate objectives with clear timeframe expectations to track the progress of the agent or tackle persistent challenges and issues. Consider using cases listed in the tool for the outlying agent's 3 self-assessments/case management reviews this quarter.

See this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I discuss this with the agent in a constructive way?",
            },
            "ACTION_STRATEGY_OUTLIER_ABSCONSION": {
                "body": """Try prioritizing rapport-building activities between the agent and the client:
1. Suggest to this agent that they should prioritize:
    - accommodating client work schedules for meetings
    - building rapport with clients early-on
    - building relationships with community-based providers to connect with struggling clients.
 2. Implement unit-wide strategies to encourage client engagement, such as:
    - early meaningful contact with all new clients
    - clear explanations of absconding and reengagement to new clients during their orientation and beyond
    - rewarding agents building positive rapport (supportive communication, some amounts of small talk) with clients.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "What strategies could an agent take to reduce their absconder warrant rate?",
            },
            "ACTION_STRATEGY_OUTLIER_NEW_OFFICER": {
                "body": """Try pairing agents up to shadow each other on a regular basis:
1. Identify agents who have a track record of following agency policy, have a growth mindset for their clients, and have a positive rapport with clients.
 2. Offer outlying agents and/or new agents the opportunity for on-the-job shadowing to learn different approaches, skills, and response techniques when interacting with clients.
 3. Reinforce the notion among your staff that this presents a valuable opportunity for learning and growth.

See more details on this and other action strategies [here](https://www.recidiviz.org).""",
                "prompt": "How might I help an outlying or new agent learn from other agents on my team?",
            },
        },
        "atOrAboveRateLabel": "At or above statewide rate",
        "atOrBelowRateLabel": "At or below statewide rate",
        "exclusionReasonDescription": "excluded because b",
        "featureVariant": "fv2",
        "id": 5,
        "learnMoreUrl": "fake.com",
        "noneAreOutliersLabel": "are outliers",
        "officerHasNoEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "officerHasNoOutlierMetricsLabel": "Nice! No outlying metrics this month.",
        "outliersHover": "Has a rate on any metric significantly higher than peers - over 1 Interquartile Range above the statewide rate.",
        "slightlyWorseThanRateLabel": "slightly worse than statewide rate",
        "status": "INACTIVE",
        "supervisionDistrictLabel": "district",
        "supervisionDistrictManagerLabel": "district manager",
        "supervisionJiiLabel": "client",
        "supervisionOfficerLabel": "officer-fv2",
        "supervisionSupervisorLabel": "supervisor",
        "supervisionUnitLabel": "unit",
        "supervisorHasNoOfficersWithEligibleClientsLabel": "Nice! No outstanding opportunities for now.",
        "supervisorHasNoOutlierOfficersLabel": "Nice! No officers are outliers on any metrics this month.",
        "updatedAt": "2024-01-01T13:00:03",
        "updatedBy": "dana2@recidiviz.org",
        "vitalsMetricsMethodologyUrl": "http://example.com/methodology",
        "worseThanRateLabel": "Far worse than statewide rate",
    },
]
