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
Enums and constants for resource categorization and metadata.

This module defines enumerations used throughout the resource search service,
including resource origins, distance modes, high-level categories, and
subcategories. These enums standardize the representation of resource data
across ingestion, storage, and search functionalities.
"""

from enum import Enum


class ResourceOrigin(str, Enum):
    GOOGLE_PLACES = "GOOGLE_PLACES"
    FACEBOOK = "FACEBOOK"
    CRAWLER = "CRAWLER"
    LOADER = "LOADER"


class DistanceMode(str, Enum):
    DRIVING = "driving"
    WALKING = "walking"
    BICYCLING = "bicycling"
    TRANSIT = "transit"


class ResourceCategory(str, Enum):
    """Resource categories"""

    BASIC_NEEDS = "Basic Needs"
    EMPLOYMENT_AND_CAREER = "Employment and Career Support"
    EDUCATION = "Education"
    BEHAVIORAL_HEALTH = "Behavioral Health Services"
    MEDICAL_AND_HEALTH = "Medical and Health Services"
    LEGAL_AND_FINANCIAL = "Legal and Financial Assistance"
    FAMILY_AND_COMMUNITY = "Family and Community Support"
    TRANSPORTATION = "Transportation"
    SPECIALIZED_SERVICES = "Specialized Services"
    COMMUNITY_REINTEGRATION = "Community and Social Reintegration"
    UNKNOWN = "Unknown"


class ResourceSubcategory(str, Enum):
    """Resource subcategories"""

    # Basic Needs
    HOUSING = "Housing"
    FOOD_ASSISTANCE = "Food Assistance"
    CLOTHING = "Clothing"

    # Employment and Career Support
    JOB_TRAINING = "Job Training Programs"
    JOB_PLACEMENT = "Job Placement Services"
    RESUME_INTERVIEW = "Resume and Interview Support"
    CERTIFICATION = "Certification and Licensing Assistance"

    # Education
    HIGH_SCHOOL_EQUIV = "High School Equivalency Programs"
    POST_SECONDARY = "Post-Secondary Education"
    LITERACY = "Literacy Programs"
    DIGITAL_LITERACY = "Digital Literacy"

    # Behavioral Health
    MENTAL_HEALTH = "Mental Health Counseling"
    SUBSTANCE_ABUSE = "Substance Abuse Treatment"
    TRAUMA_CARE = "Trauma-Informed Care"

    # Medical and Health
    PRIMARY_CARE = "Primary Care"
    SPECIALIZED_CARE = "Specialized Care"
    ADDICTION_MEDICINE = "Addiction Medicine"
    HIV_AIDS = "HIV/AIDS and Hepatitis C Services"

    # Legal and Financial
    ID_SERVICES = "Identification Services"
    LEGAL_AID = "Legal Aid"
    FINANCIAL_LITERACY = "Financial Literacy Programs"
    EMERGENCY_FINANCIAL = "Emergency Financial Assistance"

    # Family and Community Support
    FAMILY_REUNIFICATION = "Family Reunification Services"
    MENTORSHIP = "Mentorship Programs"
    FAITH_BASED = "Faith-Based Support"
    REENTRY_GROUPS = "Reentry Support Groups"

    # Transportation
    PUBLIC_TRANSIT = "Public Transit Access"
    DRIVERS_LICENSE = "Driver's License Assistance"
    TRANSPORT_SERVICES = "Transportation Services"

    # Specialized Services
    DOMESTIC_VIOLENCE = "Domestic Violence Support"
    SEX_OFFENDER = "Sex Offender-Specific Programs"
    YOUTH_RESOURCES = "Youth-Specific Resources"
    CULTURAL_PROGRAMS = "Culturally Specific Programs"

    # Community and Social Reintegration
    VOLUNTEER = "Volunteer Opportunities"
    RECREATION = "Recreation"
    CIVIC_ENGAGEMENT = "Civic Engagement"


CATEGORY_SUBCATEGORY_MAP = {
    ResourceCategory.BASIC_NEEDS: [
        ResourceSubcategory.HOUSING,
        ResourceSubcategory.FOOD_ASSISTANCE,
        ResourceSubcategory.CLOTHING,
    ],
    ResourceCategory.EMPLOYMENT_AND_CAREER: [
        ResourceSubcategory.JOB_TRAINING,
        ResourceSubcategory.JOB_PLACEMENT,
        ResourceSubcategory.RESUME_INTERVIEW,
        ResourceSubcategory.CERTIFICATION,
    ],
    ResourceCategory.EDUCATION: [
        ResourceSubcategory.HIGH_SCHOOL_EQUIV,
        ResourceSubcategory.POST_SECONDARY,
        ResourceSubcategory.LITERACY,
        ResourceSubcategory.DIGITAL_LITERACY,
    ],
    ResourceCategory.BEHAVIORAL_HEALTH: [
        ResourceSubcategory.MENTAL_HEALTH,
        ResourceSubcategory.SUBSTANCE_ABUSE,
        ResourceSubcategory.TRAUMA_CARE,
    ],
    ResourceCategory.MEDICAL_AND_HEALTH: [
        ResourceSubcategory.PRIMARY_CARE,
        ResourceSubcategory.SPECIALIZED_CARE,
        ResourceSubcategory.ADDICTION_MEDICINE,
        ResourceSubcategory.HIV_AIDS,
    ],
    ResourceCategory.LEGAL_AND_FINANCIAL: [
        ResourceSubcategory.ID_SERVICES,
        ResourceSubcategory.LEGAL_AID,
        ResourceSubcategory.FINANCIAL_LITERACY,
        ResourceSubcategory.EMERGENCY_FINANCIAL,
    ],
    ResourceCategory.FAMILY_AND_COMMUNITY: [
        ResourceSubcategory.FAMILY_REUNIFICATION,
        ResourceSubcategory.MENTORSHIP,
        ResourceSubcategory.FAITH_BASED,
        ResourceSubcategory.REENTRY_GROUPS,
    ],
    ResourceCategory.TRANSPORTATION: [
        ResourceSubcategory.PUBLIC_TRANSIT,
        ResourceSubcategory.DRIVERS_LICENSE,
        ResourceSubcategory.TRANSPORT_SERVICES,
    ],
    ResourceCategory.SPECIALIZED_SERVICES: [
        ResourceSubcategory.DOMESTIC_VIOLENCE,
        ResourceSubcategory.SEX_OFFENDER,
        ResourceSubcategory.YOUTH_RESOURCES,
        ResourceSubcategory.CULTURAL_PROGRAMS,
    ],
    ResourceCategory.COMMUNITY_REINTEGRATION: [
        ResourceSubcategory.VOLUNTEER,
        ResourceSubcategory.RECREATION,
        ResourceSubcategory.CIVIC_ENGAGEMENT,
    ],
    ResourceCategory.UNKNOWN: [],
}
