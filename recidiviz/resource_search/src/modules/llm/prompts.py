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
"""Prompts for validation"""

from typing import Optional

from pydantic import BaseModel

from recidiviz.resource_search.src.models.resource_enums import (
    ResourceCategory,
    ResourceSubcategory,
)


class ValidationPromptSet(BaseModel):
    criteria: str
    ranking: str


########################
### CATEGORY PROMPTS ###
########################

BASIC_NEEDS_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide essential basic needs services. This includes:
    - Housing assistance and shelter
    - Food and meal programs
    - Clothing assistance
    - Emergency supplies
    - Basic hygiene resources
    """,
    ranking="""
    5: Provides comprehensive basic needs assistance including housing
    4: Offers multiple basic needs services with regular availability
    3: Provides basic food, clothing or shelter assistance
    2: Offers basic needs referrals or limited direct assistance
    1: Limited basic needs support or resources
    """,
)

EMPLOYMENT_CAREER_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide employment and career services. This includes:
    - Job training programs
    - Career counseling
    - Job placement assistance
    - Resume writing help
    - Interview preparation
    - Professional certification programs
    """,
    ranking="""
    5: Provides comprehensive job training and guaranteed placement
    4: Offers structured job training with placement assistance
    3: Provides basic career services and job search help
    2: Offers employment information and referrals
    1: Limited career guidance or resources
    """,
)

EDUCATION_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide educational services. This includes:
    - High school equivalency programs
    - Post-secondary education support
    - Literacy programs
    - Digital literacy training
    - Academic counseling
    - Educational resources
    """,
    ranking="""
    5: Provides comprehensive educational programs and support
    4: Offers structured educational services and resources
    3: Provides basic educational assistance
    2: Offers educational information and referrals
    1: Limited educational support or resources
    """,
)


BEHAVIORAL_HEALTH_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide behavioral health services. This includes:
    - Mental health counseling
    - Substance abuse treatment
    - Trauma-informed care
    - Group therapy
    - Crisis intervention
    """,
    ranking="""
    5: Provides comprehensive behavioral health treatment
    4: Offers specialized mental health or addiction services
    3: Provides basic counseling or support groups
    2: Offers behavioral health referrals or resources
    1: Limited mental health support services
    """,
)

MEDICAL_AND_HEALTH_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide medical and health services. This includes:
    - Primary care services
    - Specialized medical care
    - Addiction medicine
    - HIV/AIDS and hepatitis services
    - Health screenings
    - Medication assistance
    """,
    ranking="""
    5: Provides comprehensive medical and health services
    4: Offers multiple specialized health services
    3: Provides basic medical care
    2: Offers medical referrals and resources
    1: Limited health services available
    """,
)

LEGAL_AND_FINANCIAL_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide legal and financial assistance services. This includes:
    - Legal aid services
    - Financial counseling
    - ID/documentation assistance
    - Emergency financial help
    - Benefits assistance
    - Tax preparation help
    """,
    ranking="""
    5: Provides comprehensive legal and financial services
    4: Offers multiple specialized legal/financial programs
    3: Provides basic legal or financial assistance
    2: Offers legal/financial referrals and resources
    1: Limited legal/financial support available
    """,
)

FAMILY_COMMUNITY_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide family and community support services. This includes:
    - Family reunification programs
    - Mentorship services
    - Support groups
    - Faith-based support
    - Community integration services
    """,
    ranking="""
    5: Provides comprehensive family support programs
    4: Offers structured mentorship or support services
    3: Provides basic family/community resources
    2: Offers referrals to support services
    1: Limited family/community assistance
    """,
)

TRANSPORTATION_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide transportation assistance services. This includes:
    - Public transit access
    - Driver's license assistance
    - Transportation vouchers
    - Ride services
    - Travel training
    """,
    ranking="""
    5: Provides comprehensive transportation services
    4: Offers multiple transportation options
    3: Provides basic transportation assistance
    2: Offers transportation referrals
    1: Limited transportation support
    """,
)

SPECIALIZED_SERVICES_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide specialized support services. This includes:
    - Domestic violence support
    - Sex offender programs
    - Youth-specific services
    - Culturally-specific programs
    - Special needs assistance
    """,
    ranking="""
    5: Provides comprehensive specialized support programs
    4: Offers targeted services for specific populations
    3: Provides basic specialized assistance
    2: Offers referrals to specialized services
    1: Limited specialized support available
    """,
)

COMMUNITY_REINTEGRATION_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide reintegration services. This includes:
    - Volunteer opportunities
    - Recreational activities
    - Civic engagement programs
    - Community involvement
    - Social reintegration support
    """,
    ranking="""
    5: Provides comprehensive reintegration programs
    4: Offers structured community engagement activities
    3: Provides basic reintegration assistance
    2: Offers referrals to community programs
    1: Limited reintegration support
    """,
)

UNKNOWN_PROMPTS = ValidationPromptSet(
    criteria="The data must provide some form of community service.",
    ranking="""
    5: Clearly defined service with direct assistance
    4: Well-structured support service
    3: Basic assistance or resources
    2: Information or referral service
    1: Unclear or minimal service offering
    """,
)

###########################
### SUBCATEGORY PROMPTS ###
###########################

HOUSING_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide housing assistance services. This includes:
    - Emergency shelter
    - Transitional housing
    - Permanent housing assistance
    - Housing search help
    - Rental assistance programs
    """,
    ranking="""
    5: Provides comprehensive housing assistance with multiple options
    4: Offers direct housing placement and support services
    3: Provides basic housing assistance or referrals
    2: Limited housing resources or temporary solutions
    1: Minimal housing-related support
    """,
)

FOOD_ASSISTANCE_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide food assistance services. This includes:
    - Food banks/pantries
    - Meal programs
    - Emergency food assistance
    - SNAP/food stamps assistance
    - Nutrition education
    """,
    ranking="""
    5: Comprehensive food assistance with multiple programs
    4: Regular food distribution with support services
    3: Basic food pantry or meal services
    2: Limited food assistance options
    1: Minimal or irregular food support
    """,
)

CLOTHING_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide clothing assistance services. This includes:
    - Free clothing distribution
    - Professional attire programs
    - Seasonal clothing assistance
    - Children's clothing programs
    - Emergency clothing support
    """,
    ranking="""
    5: Comprehensive clothing assistance for all needs
    4: Regular clothing distribution with specific programs
    3: Basic clothing assistance available
    2: Limited clothing resources or referrals
    1: Minimal clothing support services
    """,
)

JOB_TRAINING_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide job training services. This includes:
    - Vocational training programs
    - Skills development courses
    - Apprenticeships
    - Industry-specific training
    - On-the-job training opportunities
    """,
    ranking="""
    5: Provides comprehensive job training with certification
    4: Offers structured training programs with hands-on experience
    3: Provides basic skills training courses
    2: Offers limited training resources or workshops
    1: Minimal job training support
    """,
)

JOB_PLACEMENT_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide job placement services. This includes:
    - Direct job placement
    - Job search assistance
    - Employer partnerships
    - Job fairs and networking
    - Post-placement support
    """,
    ranking="""
    5: Provides guaranteed job placement with employer partnerships
    4: Offers active job placement assistance and follow-up
    3: Provides job search support and employer connections
    2: Offers job listings and basic referrals
    1: Limited job placement assistance
    """,
)

RESUME_INTERVIEW_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide resume and interview preparation services. This includes:
    - Resume writing assistance
    - Cover letter help
    - Interview coaching
    - Mock interviews
    - Professional development guidance
    """,
    ranking="""
    5: Provides comprehensive resume writing and interview preparation
    4: Offers personalized resume help and interview coaching
    3: Provides basic resume and interview assistance
    2: Offers templates and general interview tips
    1: Limited resume/interview resources
    """,
)

CERTIFICATION_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide certification and licensing assistance. This includes:
    - Professional certification programs
    - License application support
    - Exam preparation
    - Continuing education
    - Industry credentials
    """,
    ranking="""
    5: Provides full certification program with exam support
    4: Offers structured certification preparation
    3: Provides basic licensing assistance
    2: Offers certification information and guidance
    1: Limited certification/licensing resources
    """,
)

HIGH_SCHOOL_EQUIV_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide high school equivalency services. This includes:
    - GED preparation programs
    - High school diploma completion
    - Adult basic education
    - Academic tutoring
    - Test preparation assistance
    """,
    ranking="""
    5: Provides comprehensive GED/diploma program with support services
    4: Offers structured test preparation and tutoring
    3: Provides basic GED/diploma preparation
    2: Offers study materials and guidance
    1: Limited equivalency resources
    """,
)

POST_SECONDARY_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide post-secondary education services. This includes:
    - College preparation
    - Vocational training
    - Financial aid assistance
    - College application help
    - Academic counseling
    """,
    ranking="""
    5: Provides comprehensive college/vocational program support
    4: Offers structured college preparation services
    3: Provides basic post-secondary guidance
    2: Offers college/career information
    1: Limited post-secondary resources
    """,
)

LITERACY_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide literacy education services. This includes:
    - Reading instruction
    - Writing skills
    - ESL programs
    - Basic literacy tutoring
    - Language learning support
    """,
    ranking="""
    5: Provides comprehensive literacy education programs
    4: Offers structured literacy instruction
    3: Provides basic literacy tutoring
    2: Offers literacy resources and materials
    1: Limited literacy support services
    """,
)

DIGITAL_LITERACY_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide digital literacy services. This includes:
    - Computer skills training
    - Internet usage education
    - Digital tools instruction
    - Online safety guidance
    - Technology access
    """,
    ranking="""
    5: Provides comprehensive digital skills training with equipment
    4: Offers structured computer/internet instruction
    3: Provides basic digital literacy education
    2: Offers technology guidance and resources
    1: Limited digital literacy support
    """,
)

MENTAL_HEALTH_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide mental health counseling services. This includes:
    - Individual therapy
    - Group counseling
    - Crisis intervention
    - Psychiatric services
    - Mental health assessments
    """,
    ranking="""
    5: Provides comprehensive mental health treatment and counseling
    4: Offers regular therapy sessions with licensed professionals
    3: Provides basic mental health support and assessments
    2: Offers mental health referrals and resources
    1: Limited mental health guidance services
    """,
)

SUBSTANCE_ABUSE_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide substance abuse treatment services. This includes:
    - Drug and alcohol treatment
    - Recovery programs
    - Support groups
    - Detox services
    - Addiction counseling
    """,
    ranking="""
    5: Provides comprehensive substance abuse treatment programs
    4: Offers structured addiction recovery services
    3: Provides basic substance abuse counseling
    2: Offers addiction recovery resources and referrals
    1: Limited substance abuse support services
    """,
)

TRAUMA_CARE_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide trauma-informed care services. This includes:
    - Trauma therapy
    - PTSD treatment
    - Trauma support groups
    - Crisis counseling
    - Trauma-specific interventions
    """,
    ranking="""
    5: Provides comprehensive trauma-informed treatment
    4: Offers specialized trauma therapy services
    3: Provides basic trauma support and counseling
    2: Offers trauma-related resources and referrals
    1: Limited trauma support services
    """,
)

PRIMARY_CARE_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide primary care medical services. This includes:
    - General medical care
    - Preventive services
    - Health screenings
    - Basic medical treatment
    - Primary care physicians
    """,
    ranking="""
    5: Provides comprehensive primary care services
    4: Offers regular medical care with multiple providers
    3: Provides basic medical services and checkups
    2: Offers medical referrals and basic health screening
    1: Limited primary care services available
    """,
)

SPECIALIZED_CARE_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide specialized medical care services. This includes:
    - Specialist physicians
    - Specialized treatments
    - Medical procedures
    - Chronic disease management
    - Specialty clinics
    """,
    ranking="""
    5: Provides comprehensive specialized medical care
    4: Offers multiple specialty services and treatments
    3: Provides basic specialized medical care
    2: Offers specialist referrals and consultations
    1: Limited specialized care services
    """,
)

ADDICTION_MEDICINE_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide addiction medicine services. This includes:
    - Medication-assisted treatment
    - Medical detox services
    - Addiction specialists
    - Withdrawal management
    - Recovery medicine
    """,
    ranking="""
    5: Provides comprehensive addiction medicine treatment
    4: Offers medication-assisted treatment programs
    3: Provides basic addiction medical services
    2: Offers addiction medicine referrals
    1: Limited addiction medical support
    """,
)

HIV_AIDS_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide HIV/AIDS and Hepatitis C services. This includes:
    - HIV/AIDS testing and treatment
    - Hepatitis C screening and care
    - Medication management
    - Prevention services
    - Support services
    """,
    ranking="""
    5: Provides comprehensive HIV/AIDS and Hepatitis C care
    4: Offers regular treatment and monitoring services
    3: Provides basic testing and treatment services
    2: Offers testing and referral services
    1: Limited HIV/AIDS and Hepatitis C support
    """,
)

ID_SERVICES_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide identification document services. This includes:
    - State ID/driver's license assistance
    - Birth certificate requests
    - Social security card assistance
    - Document replacement help
    - Application support
    """,
    ranking="""
    5: Provides comprehensive ID document assistance and covers fees
    4: Offers direct help obtaining multiple forms of ID
    3: Provides basic ID application assistance
    2: Offers ID service information and referrals
    1: Limited ID document support
    """,
)

LEGAL_AID_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide legal aid services. This includes:
    - Criminal record expungement
    - Legal representation
    - Legal consultation
    - Court advocacy
    - Legal document assistance
    """,
    ranking="""
    5: Provides free comprehensive legal representation
    4: Offers sliding-scale legal services
    3: Provides basic legal consultation
    2: Offers legal information and referrals
    1: Limited legal support services
    """,
)

FINANCIAL_LITERACY_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide financial education services. This includes:
    - Money management classes
    - Budgeting workshops
    - Credit counseling
    - Banking assistance
    - Financial planning help
    """,
    ranking="""
    5: Provides comprehensive financial education programs
    4: Offers regular financial workshops and counseling
    3: Provides basic financial literacy classes
    2: Offers financial information and resources
    1: Limited financial education support
    """,
)

EMERGENCY_FINANCIAL_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide emergency financial assistance. This includes:
    - Emergency cash assistance
    - Bill payment help
    - Rent/utility assistance
    - Financial crisis support
    - Emergency loans
    """,
    ranking="""
    5: Provides immediate substantial financial assistance
    4: Offers emergency financial aid with support services
    3: Provides basic emergency financial help
    2: Offers financial emergency referrals
    1: Limited emergency financial support
    """,
)

FAMILY_REUNIFICATION_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide family reunification services. This includes:
    - Family counseling
    - Visitation support
    - Parent education
    - Child custody assistance
    - Family mediation
    """,
    ranking="""
    5: Provides comprehensive family reunification programs
    4: Offers structured family support services
    3: Provides basic family reunification help
    2: Offers family service referrals
    1: Limited family reunification support
    """,
)

MENTORSHIP_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide mentorship services. This includes:
    - One-on-one mentoring
    - Peer support
    - Life skills coaching
    - Career mentoring
    - Youth mentoring
    """,
    ranking="""
    5: Provides comprehensive mentorship programs
    4: Offers structured mentoring relationships
    3: Provides basic mentoring services
    2: Offers mentorship referrals
    1: Limited mentoring support
    """,
)

FAITH_BASED_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide faith-based support services. This includes:
    - Religious counseling
    - Spiritual guidance
    - Faith community connections
    - Religious services
    - Faith-based programs
    """,
    ranking="""
    5: Provides comprehensive faith-based programs
    4: Offers regular spiritual support services
    3: Provides basic faith-based assistance
    2: Offers religious service referrals
    1: Limited faith-based support
    """,
)

REENTRY_GROUPS_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide reentry support groups. This includes:
    - Peer support meetings
    - Group counseling
    - Recovery groups
    - Life skills groups
    - Community support circles
    """,
    ranking="""
    5: Provides comprehensive reentry group programs
    4: Offers regular support group meetings
    3: Provides basic group support services
    2: Offers support group referrals
    1: Limited group support options
    """,
)

PUBLIC_TRANSIT_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide public transit assistance. This includes:
    - Bus pass programs
    - Transit fare assistance
    - Route planning help
    - Transportation vouchers
    - Public transit training
    """,
    ranking="""
    5: Provides comprehensive transit assistance programs
    4: Offers regular transit support services
    3: Provides basic transit fare help
    2: Offers public transit information
    1: Limited transit support
    """,
)

DRIVERS_LICENSE_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide driver's license assistance. This includes:
    - License reinstatement help
    - Testing preparation
    - Fee assistance
    - Documentation help
    - DMV advocacy
    """,
    ranking="""
    5: Provides comprehensive license assistance
    4: Offers direct help with license process
    3: Provides basic license support
    2: Offers license information and referrals
    1: Limited license assistance
    """,
)

TRANSPORT_SERVICES_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide transportation services. This includes:
    - Ride services
    - Medical transportation
    - Employment transportation
    - Emergency transport
    - Vehicle assistance
    """,
    ranking="""
    5: Provides comprehensive transportation services
    4: Offers regular transportation assistance
    3: Provides basic transportation help
    2: Offers transportation referrals
    1: Limited transportation support
    """,
)

DOMESTIC_VIOLENCE_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide domestic violence support. This includes:
    - Emergency shelter
    - Crisis intervention
    - Safety planning
    - Counseling services
    - Legal advocacy
    """,
    ranking="""
    5: Provides comprehensive domestic violence services
    4: Offers multiple support services for survivors
    3: Provides basic domestic violence assistance
    2: Offers domestic violence referrals
    1: Limited domestic violence support
    """,
)

SEX_OFFENDER_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide sex offender support services. This includes:
    - Treatment programs
    - Counseling services
    - Registration assistance
    - Housing support
    - Compliance help
    """,
    ranking="""
    5: Provides comprehensive offender support programs
    4: Offers multiple specialized services
    3: Provides basic offender assistance
    2: Offers program referrals
    1: Limited offender support
    """,
)

YOUTH_RESOURCES_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide youth-specific resources. This includes:
    - Youth counseling
    - Educational support
    - Recreational programs
    - Mentoring services
    - Life skills training
    """,
    ranking="""
    5: Provides comprehensive youth programs
    4: Offers multiple youth services
    3: Provides basic youth assistance
    2: Offers youth program referrals
    1: Limited youth support
    """,
)

CULTURAL_PROGRAMS_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide culturally specific programs. This includes:
    - Cultural education
    - Language services
    - Cultural celebrations
    - Community connections
    - Cultural advocacy
    """,
    ranking="""
    5: Provides comprehensive cultural programs
    4: Offers multiple cultural services
    3: Provides basic cultural support
    2: Offers cultural program referrals
    1: Limited cultural assistance
    """,
)

VOLUNTEER_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide volunteer opportunities. This includes:
    - Community service
    - Skill-based volunteering
    - Group projects
    - Regular programs
    - One-time events
    """,
    ranking="""
    5: Provides comprehensive volunteer programs
    4: Offers regular volunteer opportunities
    3: Provides basic volunteer options
    2: Offers volunteer referrals
    1: Limited volunteer opportunities
    """,
)

RECREATION_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide recreational activities. This includes:
    - Sports programs
    - Fitness activities
    - Arts and crafts
    - Social events
    - Outdoor activities
    """,
    ranking="""
    5: Provides comprehensive recreation programs
    4: Offers regular recreational activities
    3: Provides basic recreation options
    2: Offers recreation referrals
    1: Limited recreation opportunities
    """,
)

CIVIC_ENGAGEMENT_PROMPTS = ValidationPromptSet(
    criteria="""
    The data must provide civic engagement opportunities. This includes:
    - Community organizing
    - Advocacy training
    - Voter registration
    - Leadership development
    - Community meetings
    """,
    ranking="""
    5: Provides comprehensive civic programs
    4: Offers regular engagement activities
    3: Provides basic civic involvement
    2: Offers civic engagement referrals
    1: Limited civic opportunities
    """,
)


CATEGORY_PROMPT_MAP = {
    ResourceCategory.BASIC_NEEDS: BASIC_NEEDS_PROMPTS,
    ResourceCategory.EMPLOYMENT_AND_CAREER: EMPLOYMENT_CAREER_PROMPTS,
    ResourceCategory.EDUCATION: EDUCATION_PROMPTS,
    ResourceCategory.BEHAVIORAL_HEALTH: BEHAVIORAL_HEALTH_PROMPTS,
    ResourceCategory.MEDICAL_AND_HEALTH: MEDICAL_AND_HEALTH_PROMPTS,
    ResourceCategory.LEGAL_AND_FINANCIAL: LEGAL_AND_FINANCIAL_PROMPTS,
    ResourceCategory.FAMILY_AND_COMMUNITY: FAMILY_COMMUNITY_PROMPTS,
    ResourceCategory.TRANSPORTATION: TRANSPORTATION_PROMPTS,
    ResourceCategory.SPECIALIZED_SERVICES: SPECIALIZED_SERVICES_PROMPTS,
    ResourceCategory.COMMUNITY_REINTEGRATION: COMMUNITY_REINTEGRATION_PROMPTS,
    ResourceCategory.UNKNOWN: UNKNOWN_PROMPTS,
}

SUBCATEGORY_PROMPT_MAP = {
    ResourceSubcategory.HOUSING: HOUSING_PROMPTS,
    ResourceSubcategory.FOOD_ASSISTANCE: FOOD_ASSISTANCE_PROMPTS,
    ResourceSubcategory.CLOTHING: CLOTHING_PROMPTS,
    ResourceSubcategory.JOB_TRAINING: JOB_TRAINING_PROMPTS,
    ResourceSubcategory.JOB_PLACEMENT: JOB_PLACEMENT_PROMPTS,
    ResourceSubcategory.RESUME_INTERVIEW: RESUME_INTERVIEW_PROMPTS,
    ResourceSubcategory.CERTIFICATION: CERTIFICATION_PROMPTS,
    ResourceSubcategory.HIGH_SCHOOL_EQUIV: HIGH_SCHOOL_EQUIV_PROMPTS,
    ResourceSubcategory.POST_SECONDARY: POST_SECONDARY_PROMPTS,
    ResourceSubcategory.LITERACY: LITERACY_PROMPTS,
    ResourceSubcategory.DIGITAL_LITERACY: DIGITAL_LITERACY_PROMPTS,
    ResourceSubcategory.MENTAL_HEALTH: MENTAL_HEALTH_PROMPTS,
    ResourceSubcategory.SUBSTANCE_ABUSE: SUBSTANCE_ABUSE_PROMPTS,
    ResourceSubcategory.TRAUMA_CARE: TRAUMA_CARE_PROMPTS,
    ResourceSubcategory.PRIMARY_CARE: PRIMARY_CARE_PROMPTS,
    ResourceSubcategory.SPECIALIZED_CARE: SPECIALIZED_CARE_PROMPTS,
    ResourceSubcategory.ADDICTION_MEDICINE: ADDICTION_MEDICINE_PROMPTS,
    ResourceSubcategory.HIV_AIDS: HIV_AIDS_PROMPTS,
    ResourceSubcategory.ID_SERVICES: ID_SERVICES_PROMPTS,
    ResourceSubcategory.LEGAL_AID: LEGAL_AID_PROMPTS,
    ResourceSubcategory.FINANCIAL_LITERACY: FINANCIAL_LITERACY_PROMPTS,
    ResourceSubcategory.EMERGENCY_FINANCIAL: EMERGENCY_FINANCIAL_PROMPTS,
    ResourceSubcategory.FAMILY_REUNIFICATION: FAMILY_REUNIFICATION_PROMPTS,
    ResourceSubcategory.MENTORSHIP: MENTORSHIP_PROMPTS,
    ResourceSubcategory.FAITH_BASED: FAITH_BASED_PROMPTS,
    ResourceSubcategory.REENTRY_GROUPS: REENTRY_GROUPS_PROMPTS,
    ResourceSubcategory.PUBLIC_TRANSIT: PUBLIC_TRANSIT_PROMPTS,
    ResourceSubcategory.DRIVERS_LICENSE: DRIVERS_LICENSE_PROMPTS,
    ResourceSubcategory.TRANSPORT_SERVICES: TRANSPORT_SERVICES_PROMPTS,
    ResourceSubcategory.DOMESTIC_VIOLENCE: DOMESTIC_VIOLENCE_PROMPTS,
    ResourceSubcategory.SEX_OFFENDER: SEX_OFFENDER_PROMPTS,
    ResourceSubcategory.YOUTH_RESOURCES: YOUTH_RESOURCES_PROMPTS,
    ResourceSubcategory.CULTURAL_PROGRAMS: CULTURAL_PROGRAMS_PROMPTS,
    ResourceSubcategory.VOLUNTEER: VOLUNTEER_PROMPTS,
    ResourceSubcategory.RECREATION: RECREATION_PROMPTS,
    ResourceSubcategory.CIVIC_ENGAGEMENT: CIVIC_ENGAGEMENT_PROMPTS,
}


def get_validation_prompts(
    category: ResourceCategory, subcategory: Optional[ResourceSubcategory] = None
) -> ValidationPromptSet:
    """Get the validation prompts for a given resource subcategory or category"""
    if subcategory:
        return SUBCATEGORY_PROMPT_MAP[subcategory]

    return CATEGORY_PROMPT_MAP[category]
