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
"""Generic tests for the shared entity_documentation_utils.

These tests exercise `resolve_field_description` and
`get_field_descriptions_from_yaml` using synthetic entities (`fake_entities`)
and tmp-path YAML fixtures, so they don't depend on the structure of any real
entity module.
"""
import os
from pathlib import Path

import pytest

from recidiviz.persistence.entity import identity
from recidiviz.persistence.entity.entity_documentation_utils import (
    FIELD_KEY,
    RootFieldDescription,
    entity_field_descriptions_yaml_path,
    get_field_descriptions_from_yaml,
    resolve_field_description,
)
from recidiviz.tests.persistence.entity import fake_entities

_ROOT_FIELD = RootFieldDescription(
    field_name="state_code",
    description="Synthetic root-field description for testing.",
)


def _write_yaml(tmp_path: Path, name: str, content: str) -> str:
    """Writes `content` to a YAML at `tmp_path/name` and returns its absolute
    path. Each test should use a unique `name` to avoid colliding with the
    `@cache` on `get_field_descriptions_from_yaml` (which keys on path).
    """
    path = tmp_path / name
    path.write_text(content)
    return str(path)


def test_entity_field_descriptions_yaml_path_points_to_package_yaml() -> None:
    path = entity_field_descriptions_yaml_path(identity)
    assert path.endswith(
        "recidiviz/persistence/entity/identity/entity_field_descriptions.yaml"
    )
    assert os.path.exists(path)


def test_get_field_descriptions_from_yaml_loads_structured_content(
    tmp_path: Path,
) -> None:
    yaml_path = _write_yaml(
        tmp_path,
        "load.yaml",
        """
some_table:
  section_a:
    field_1: Description of field_1.
  section_b:
    field_2: Description of field_2.
""",
    )
    yaml_data = get_field_descriptions_from_yaml(yaml_path)
    assert yaml_data == {
        "some_table": {
            "section_a": {"field_1": "Description of field_1."},
            "section_b": {"field_2": "Description of field_2."},
        }
    }


def test_get_field_descriptions_from_yaml_resolves_anchors_and_merge_keys(
    tmp_path: Path,
) -> None:
    """Tables may share descriptions via YAML anchors/aliases and merge keys
    (e.g. identity cluster tables aliasing their fragment equivalents); these
    must be fully resolved in the loaded dict. Local keys win over merged keys.
    """
    yaml_path = _write_yaml(
        tmp_path,
        "anchors.yaml",
        """
table_a:
  fields: &table_a_fields
    field_1: Shared description of field_1.
    field_2: Description of field_2.

table_b:
  fields: *table_a_fields

table_c:
  fields:
    <<: *table_a_fields
    field_2: Overridden description of field_2.
    field_3: Description of field_3 only on table_c.
""",
    )
    yaml_data = get_field_descriptions_from_yaml(yaml_path)
    assert yaml_data["table_b"][FIELD_KEY] == yaml_data["table_a"][FIELD_KEY]
    assert yaml_data["table_c"][FIELD_KEY] == {
        "field_1": "Shared description of field_1.",
        "field_2": "Overridden description of field_2.",
        "field_3": "Description of field_3 only on table_c.",
    }


def test_reference_field_returns_reference_description(tmp_path: Path) -> None:
    """Forward-edge fields short-circuit before any YAML lookup."""
    yaml_path = _write_yaml(tmp_path, "ref.yaml", "fake_person: { fields: {} }")
    assert (
        resolve_field_description(
            fake_entities.FakePerson,
            "external_ids",
            yaml_path=yaml_path,
            root_field=_ROOT_FIELD,
            sections=(FIELD_KEY,),
        )
        == "Reference to FakePersonExternalId"
    )


def test_root_field_returns_configured_description(tmp_path: Path) -> None:
    yaml_path = _write_yaml(tmp_path, "root.yaml", "fake_person: { fields: {} }")
    assert (
        resolve_field_description(
            fake_entities.FakePerson,
            _ROOT_FIELD.field_name,
            yaml_path=yaml_path,
            root_field=_ROOT_FIELD,
            sections=(FIELD_KEY,),
        )
        == _ROOT_FIELD.description
    )


def test_yaml_lookup_returns_description(tmp_path: Path) -> None:
    yaml_path = _write_yaml(
        tmp_path,
        "lookup.yaml",
        """
fake_person:
  fields:
    full_name: A person's full legal name.
""",
    )
    assert (
        resolve_field_description(
            fake_entities.FakePerson,
            "full_name",
            yaml_path=yaml_path,
            root_field=_ROOT_FIELD,
            sections=(FIELD_KEY,),
        )
        == "A person's full legal name."
    )


def test_first_matching_section_wins(tmp_path: Path) -> None:
    """When multiple sections are consulted, the first match wins."""
    yaml_path = _write_yaml(
        tmp_path,
        "sections_first.yaml",
        """
fake_person:
  fields:
    full_name: From fields.
  extra:
    full_name: From extra.
""",
    )
    assert (
        resolve_field_description(
            fake_entities.FakePerson,
            "full_name",
            yaml_path=yaml_path,
            root_field=_ROOT_FIELD,
            sections=(FIELD_KEY, "extra"),
        )
        == "From fields."
    )


def test_falls_through_to_later_section(tmp_path: Path) -> None:
    """If an earlier section doesn't have the field, later sections are
    consulted (e.g. activity's `fields` + `normalization_only_fields`).
    """
    yaml_path = _write_yaml(
        tmp_path,
        "sections_fallthrough.yaml",
        """
fake_person:
  fields:
    something_else: irrelevant
  extra:
    full_name: From extra fallback.
""",
    )
    assert (
        resolve_field_description(
            fake_entities.FakePerson,
            "full_name",
            yaml_path=yaml_path,
            root_field=_ROOT_FIELD,
            sections=(FIELD_KEY, "extra"),
        )
        == "From extra fallback."
    )


def test_external_id_auto_generated_when_not_in_yaml(tmp_path: Path) -> None:
    yaml_path = _write_yaml(
        tmp_path,
        "ext_auto.yaml",
        "fake_person_external_id: { fields: {} }",
    )
    description = resolve_field_description(
        fake_entities.FakePersonExternalId,
        "external_id",
        yaml_path=yaml_path,
        root_field=_ROOT_FIELD,
        sections=(FIELD_KEY,),
    )
    assert "FakePersonExternalId" in description
    assert "unique identifier" in description


def test_primary_key_auto_generated_when_not_in_yaml(tmp_path: Path) -> None:
    yaml_path = _write_yaml(tmp_path, "pk_auto.yaml", "fake_person: { fields: {} }")
    assert fake_entities.FakePerson.get_primary_key_column_name() == "fake_person_id"
    description = resolve_field_description(
        fake_entities.FakePerson,
        "fake_person_id",
        yaml_path=yaml_path,
        root_field=_ROOT_FIELD,
        sections=(FIELD_KEY,),
    )
    assert description == (
        "Unique identifier for the FakePerson entity generated automatically by "
        "the Recidiviz system."
    )


def test_yaml_overrides_external_id_auto_gen(tmp_path: Path) -> None:
    """YAML lookup runs before the `external_id` auto-gen, so callers can
    override it for entities where the generic phrasing isn't right.
    """
    yaml_path = _write_yaml(
        tmp_path,
        "ext_override.yaml",
        """
fake_person_external_id:
  fields:
    external_id: Custom external_id description.
""",
    )
    assert (
        resolve_field_description(
            fake_entities.FakePersonExternalId,
            "external_id",
            yaml_path=yaml_path,
            root_field=_ROOT_FIELD,
            sections=(FIELD_KEY,),
        )
        == "Custom external_id description."
    )


def test_yaml_overrides_primary_key_auto_gen(tmp_path: Path) -> None:
    """YAML lookup runs before the PK auto-gen, so an entity whose PK column
    has a content-specific meaning (e.g. `IdentityCluster.identity_cluster_id`
    being a SHA-256) can override the generic auto-gen description.
    """
    yaml_path = _write_yaml(
        tmp_path,
        "pk_override.yaml",
        """
fake_person:
  fields:
    fake_person_id: Custom PK description.
""",
    )
    assert (
        resolve_field_description(
            fake_entities.FakePerson,
            "fake_person_id",
            yaml_path=yaml_path,
            root_field=_ROOT_FIELD,
            sections=(FIELD_KEY,),
        )
        == "Custom PK description."
    )


def test_raises_for_field_not_on_entity(tmp_path: Path) -> None:
    yaml_path = _write_yaml(tmp_path, "unknown.yaml", "fake_person: { fields: {} }")
    with pytest.raises(ValueError, match="Unexpected field for class"):
        resolve_field_description(
            fake_entities.FakePerson,
            "this_field_does_not_exist",
            yaml_path=yaml_path,
            root_field=_ROOT_FIELD,
            sections=(FIELD_KEY,),
        )


def test_raises_when_no_description_resolves(tmp_path: Path) -> None:
    """If a flat field is on the entity but isn't in YAML and isn't covered by
    the root / external_id / PK auto-gen branches, we raise rather than
    silently returning something misleading.
    """
    yaml_path = _write_yaml(tmp_path, "empty.yaml", "fake_person: { fields: {} }")
    with pytest.raises(ValueError, match="Didn't find description for field"):
        resolve_field_description(
            fake_entities.FakePerson,
            "full_name",
            yaml_path=yaml_path,
            root_field=_ROOT_FIELD,
            sections=(FIELD_KEY,),
        )
