# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2026 Recidiviz, Inc.
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
"""Factory classes for deserializing identity ingest pipeline entities."""

from recidiviz.persistence.entity.entity_deserialize import (
    DeserializableEntityFieldValue,
    EntityFactory,
    entity_deserialize,
)
from recidiviz.pipelines.ingest.identity import entities


class IdentityExternalIdFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.IdentityExternalId:
        return entity_deserialize(
            cls=entities.IdentityExternalId,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class IdentityNameFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.IdentityName:
        return entity_deserialize(
            cls=entities.IdentityName,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class IdentityGenderFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.IdentityGender:
        return entity_deserialize(
            cls=entities.IdentityGender,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class IdentitySexFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.IdentitySex:
        return entity_deserialize(
            cls=entities.IdentitySex,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class IdentityRaceFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.IdentityRace:
        return entity_deserialize(
            cls=entities.IdentityRace,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class IdentityEthnicityFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.IdentityEthnicity:
        return entity_deserialize(
            cls=entities.IdentityEthnicity,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class IdentityPhoneNumberFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.IdentityPhoneNumber:
        return entity_deserialize(
            cls=entities.IdentityPhoneNumber,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class IdentityEmailFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.IdentityEmail:
        return entity_deserialize(
            cls=entities.IdentityEmail,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class IdentityAttributesFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.IdentityAttributes:
        return entity_deserialize(
            cls=entities.IdentityAttributes,
            converter_overrides={},
            defaults={},
            **kwargs,
        )


class IdentityFragmentFactory(EntityFactory):
    @staticmethod
    def deserialize(
        **kwargs: DeserializableEntityFieldValue,
    ) -> entities.IdentityFragment:
        return entity_deserialize(
            cls=entities.IdentityFragment,
            converter_overrides={},
            defaults={"external_ids": []},
            **kwargs,
        )
