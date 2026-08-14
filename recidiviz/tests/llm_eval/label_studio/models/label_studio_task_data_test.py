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
"""Tests for LabelStudioTaskData, including the one that keeps every task payload in sync
with the project config that parses it back.
"""
import importlib
import json
import pkgutil
import re
import sys
import unittest
from types import ModuleType, NoneType
from typing import Any, Optional, get_args

import attr

import recidiviz.llm_eval
import recidiviz.tools.llm_eval
from recidiviz.llm_eval.label_studio.models.document_extraction.cni_accuracy_per_field_task_data import (
    CNI_ACCURACY_PER_FIELD_TASK_NAME,
    CNIAccuracyPerFieldTaskData,
)
from recidiviz.llm_eval.label_studio.models.label_studio_project_config import (
    LabelStudioProjectConfig,
)
from recidiviz.llm_eval.label_studio.models.label_studio_task_data import (
    LabelStudioTaskData,
    LabelStudioTaskDataValue,
)

# Packages walked to find every LabelStudioTaskData subclass. A subclass is only
# discoverable once its module has been imported, so these get imported wholesale rather than
# named one by one, and a payload added anywhere in either tree is picked up without anyone
# remembering to register it here.
_PACKAGES_HOLDING_TASK_DATA = [recidiviz.llm_eval, recidiviz.tools.llm_eval]


def _import_all_submodules(package: ModuleType) -> None:
    """Imports every module under |package|, recursively."""
    for module_info in pkgutil.walk_packages(
        package.__path__, prefix=f"{package.__name__}."
    ):
        importlib.import_module(module_info.name)


def _all_task_data_subclasses() -> list[type[LabelStudioTaskData]]:
    """Returns every concrete LabelStudioTaskData subclass defined in the packages that
    hold real payloads.

    Subclasses declared in test modules are excluded. __subclasses__ sees every subclass
    loaded into the process, including the malformed fixtures below, which exist to violate
    the invariant this test enforces.
    """
    allowed_module_prefixes = tuple(
        f"{package.__name__}." for package in _PACKAGES_HOLDING_TASK_DATA
    )
    for package in _PACKAGES_HOLDING_TASK_DATA:
        _import_all_submodules(package)

    # Walked iteratively rather than recursively so the abstract base is never passed as
    # a type[LabelStudioTaskData] argument, which mypy rejects.
    descendants: list[type[LabelStudioTaskData]] = []
    to_visit: list[type[LabelStudioTaskData]] = LabelStudioTaskData.__subclasses__()
    while to_visit:
        subclass = to_visit.pop()
        descendants.append(subclass)
        to_visit.extend(subclass.__subclasses__())

    return [
        cls
        for cls in descendants
        if cls.__module__.startswith(allowed_module_prefixes)
        and not getattr(cls, "__abstractmethods__", frozenset())
        and _is_live_class(cls)
    ]


def _is_live_class(cls: type) -> bool:
    """Returns whether |cls| is the class its module currently binds, rather than a stale
    duplicate.

    attr.define defaults to slots=True, which builds a replacement class object. The
    discarded pre-slots original stays registered in its base's __subclasses__, carrying
    __attrs_attrs__ but no fields. Garbage collection normally removes it before anything
    looks, so discovery that trusts __subclasses__ fails only in long runs. The module's own
    binding is the class that actually exists.
    """
    module = sys.modules.get(cls.__module__)
    return module is not None and getattr(module, cls.__name__, None) is cls


def _declared_value_types(annotation: object) -> set[type]:
    """Returns the concrete types an attrs field annotation permits, dropping None and
    reducing a generic to its container, so "str | None" gives {str} and "dict[str, Any]"
    gives {dict}.
    """
    if isinstance(annotation, type):
        return {annotation}
    # A subscripted generic such as dict[str, Any] carries an __origin__ naming its
    # container, and a union such as str | None does not, which tells the two apart. An
    # Optional[str] written the older way has one, but it holds typing.Union rather than a
    # class, so it falls through to the union branch below.
    container = getattr(annotation, "__origin__", None)
    if isinstance(container, type):
        return {container}
    if union_members := get_args(annotation):
        declared: set[type] = set()
        for member in union_members:
            if member is NoneType:
                continue
            declared |= _declared_value_types(member)
        return declared
    raise ValueError(f"Cannot read concrete types out of annotation [{annotation}].")


class TaskDataMatchesProjectConfigTest(unittest.TestCase):
    """Tests that every task payload defined in the repo agrees with the project config
    that parses it back, meaning the same field names holding types that config's bq_types
    can carry.

    The reverse does not hold and is not checked: a project config may have no payload
    class at all, because not every annotation task has its tasks created in this
    codebase.
    """

    def test_all_task_data_classes_match_their_project_config(self) -> None:
        task_data_classes = _all_task_data_subclasses()
        self.assertTrue(
            task_data_classes,
            "Found no LabelStudioTaskData subclasses. Either none exist, or the packages "
            "walked to discover them no longer hold them.",
        )
        for task_data_class in task_data_classes:
            with self.subTest(task_data_class=task_data_class.__name__):
                # Raises if the class names a task that has no project config.
                config = task_data_class.project_config()
                declared_types_by_field_name = {
                    field.name: _declared_value_types(field.type)
                    for field in attr.fields(task_data_class)
                }

                missing = sorted(
                    field.column_name
                    for field in config.task_data_fields
                    if field.column_name not in declared_types_by_field_name
                )
                self.assertEqual(
                    [],
                    missing,
                    f"[{task_data_class.__name__}] declares no field for {missing}, "
                    f"which the [{config.task_name}] parsed annotations view projects out "
                    f"of task.data. Every projected field has to be written, or its "
                    f"column comes back all-NULL.",
                )

                for field in config.task_data_fields:
                    unusable = sorted(
                        declared_type.__name__
                        for declared_type in declared_types_by_field_name[
                            field.column_name
                        ]
                        if declared_type not in field.allowed_value_types
                    )
                    self.assertEqual(
                        [],
                        unusable,
                        f"[{task_data_class.__name__}.{field.column_name}] is typed "
                        f"{unusable}, which the [{config.task_name}] config's bq_type "
                        f"[{field.bq_type.value}] cannot carry. Allowed: "
                        f"{sorted(t.__name__ for t in field.allowed_value_types)}.",
                    )

    def test_no_task_data_class_overrides_attrs_post_init(self) -> None:
        """attrs calls only the most derived __attrs_post_init__, so a subclass defining one
        would silently drop the config check LabelStudioTaskData does there. Subclass
        invariants belong in validate_payload instead.
        """
        for task_data_class in _all_task_data_subclasses():
            with self.subTest(task_data_class=task_data_class.__name__):
                # assertFalse rather than assertNotIn, which would dump the whole class
                # dict into the failure message and bury the explanation.
                self.assertFalse(
                    "__attrs_post_init__" in vars(task_data_class),
                    f"[{task_data_class.__name__}] defines __attrs_post_init__, which "
                    f"overrides the one LabelStudioTaskData uses to check the payload "
                    f"against its project config. Move those invariants to "
                    f"validate_payload.",
                )


_SAMPLE_VALUE_BY_TYPE: dict[type, LabelStudioTaskDataValue] = {
    str: "a value",
    int: 1,
    float: 1.5,
    bool: True,
    dict: {"key": "value"},
    list: ["value"],
}


def _minimal_valid_task_data(
    config: LabelStudioProjectConfig,
) -> dict[str, LabelStudioTaskDataValue]:
    """Returns a task.data dict holding a usable value for every field |config| projects
    and nothing else, which is the baseline a test breaks one field of.
    """
    return {
        field.column_name: _SAMPLE_VALUE_BY_TYPE[field.allowed_value_types[0]]
        for field in config.task_data_fields
    }


@attr.define(frozen=True, kw_only=True)
class _PartialCNIAccuracyTaskData(LabelStudioTaskData):
    """A CNI accuracy payload carrying only two of the project's fields.

    The fields carry no attrs validators, because a validator would reject a wrongly typed
    value at construction, before the config-driven check under test could see it.
    """

    document_id: str = attr.ib()
    task_order: int = attr.ib()

    @classmethod
    def task_name(cls) -> str:
        return CNI_ACCURACY_PER_FIELD_TASK_NAME


@attr.define(frozen=True, kw_only=True)
class _CompleteCNIAccuracyTaskData(LabelStudioTaskData):
    """A CNI accuracy payload carrying every field the project config projects, so a test
    reaches the checks that run once the payload is complete.

    The fields carry no attrs validators, because a validator would reject a wrongly typed
    value at construction, before the config-driven check could see it.
    """

    state_code: str = attr.ib()
    document_id: str = attr.ib()
    document_text: str = attr.ib()
    field_name: str = attr.ib()
    group: str = attr.ib()
    task_order: int = attr.ib()
    extracted_value: str = attr.ib()
    field_description: str = attr.ib()
    extractor_version_id: str = attr.ib()

    @classmethod
    def task_name(cls) -> str:
        return CNI_ACCURACY_PER_FIELD_TASK_NAME


@attr.define(frozen=True, kw_only=True)
class _TaskDataWithContainerField(_CompleteCNIAccuracyTaskData):
    """A payload holding a JSON object in one field. The config projects no such field, so
    this one rides along as context for the annotator.
    """

    annotator_hints: dict[str, Any] = attr.ib()


def _complete_cni_accuracy_payload() -> CNIAccuracyPerFieldTaskData:
    """Returns the real CNI accuracy payload with every field populated."""
    return CNIAccuracyPerFieldTaskData(
        state_code="US_CO",
        document_id="doc_a",
        document_text="Client started at Walmart as a cashier.",
        prompt_description="Pull employment details out of case notes.",
        field_name="job_title",
        field_description="The person's job title at the employer.",
        group="employers[0]",
        extracted_value="cashier",
        confidence_level="INFERRED",
        array_element_json='{"employer_name": "Walmart", "job_title": "cashier"}',
        extractor_version_id="9f8e7d",
        doc_index=1,
        field_index=2,
        total_fields=3,
        task_order=2,
    )


class LabelStudioTaskDataTest(unittest.TestCase):
    """Tests for the validation LabelStudioTaskData applies at construction."""

    def test_unknown_task_name_raises(self) -> None:
        @attr.define(frozen=True, kw_only=True)
        class _NoSuchProjectTaskData(LabelStudioTaskData):
            @classmethod
            def task_name(cls) -> str:
                return "not_a_real_task"

        with self.assertRaisesRegex(
            ValueError,
            r"^Found no Label Studio project config for task \[not_a_real_task\]",
        ):
            _NoSuchProjectTaskData()

    def test_payload_missing_projected_fields_cannot_be_constructed(self) -> None:
        # Named fields rather than the whole missing list, so adding or removing a
        # projected column in the config does not break this test.
        with self.assertRaisesRegex(
            ValueError,
            r"is missing field\(s\) \[.*'document_text'.*'state_code'.*\], which its "
            r"parsed annotations view projects into columns",
        ):
            _PartialCNIAccuracyTaskData(document_id="doc_a", task_order=1)

    def test_config_check_precedes_validate_payload(self) -> None:
        @attr.define(frozen=True, kw_only=True)
        class _PayloadWithOwnInvariant(_PartialCNIAccuracyTaskData):
            def validate_payload(self) -> None:
                raise ValueError("Subclass invariant violated.")

        # This payload is missing projected fields, so the config check fails before the
        # subclass hook runs.
        with self.assertRaisesRegex(ValueError, r"is missing field\(s\)"):
            _PayloadWithOwnInvariant(document_id="doc_a", task_order=1)

    def test_validate_payload_runs_once_the_config_check_passes(self) -> None:
        @attr.define(frozen=True, kw_only=True)
        class _PayloadWithFailingInvariant(_CompleteCNIAccuracyTaskData):
            def validate_payload(self) -> None:
                raise ValueError("Subclass invariant violated.")

        with self.assertRaisesRegex(ValueError, r"^Subclass invariant violated\.$"):
            _PayloadWithFailingInvariant(
                state_code="US_CO",
                document_id="doc_a",
                document_text="Client started at Walmart as a cashier.",
                field_name="job_title",
                group="employers[0]",
                task_order=2,
                extracted_value="cashier",
                field_description="The person's job title at the employer.",
                extractor_version_id="9f8e7d",
            )

    def test_payload_of_all_nulls_passes(self) -> None:
        """Every task.data column the parsed annotations view emits is NULLABLE, so a
        producer that has nothing for a field may send None.
        """
        config = _CompleteCNIAccuracyTaskData.project_config()
        config.validate_task_data(
            {field.column_name: None for field in config.task_data_fields}
        )

    def test_missing_field_message_names_every_projected_field(self) -> None:
        config = _CompleteCNIAccuracyTaskData.project_config()
        projected = sorted(field.column_name for field in config.task_data_fields)
        with self.assertRaisesRegex(
            ValueError, re.escape(f"is missing field(s) {projected}")
        ):
            config.validate_task_data({})

    def test_wrongly_typed_value_raises(self) -> None:
        # A complete payload, so the type check is what fails rather than the presence
        # check that runs before it. task_order is INT64 in the config, so a string there
        # would CAST to NULL in the parsed annotations view.
        config = _PartialCNIAccuracyTaskData.project_config()
        task_data = _minimal_valid_task_data(config)
        task_data["task_order"] = "1"

        with self.assertRaisesRegex(
            ValueError,
            r"has an unusable value: Task data field \[task_order\] declares bq_type "
            r"\[INT64\], so its value must be one of \['int'\] or None, but found \[1\] "
            r"of type \[str\]",
        ):
            config.validate_task_data(task_data)

    def test_complete_payload_passes(self) -> None:
        config = _PartialCNIAccuracyTaskData.project_config()
        config.validate_task_data(_minimal_valid_task_data(config))

    def test_extra_keys_allowed(self) -> None:
        config = _PartialCNIAccuracyTaskData.project_config()
        task_data = _minimal_valid_task_data(config)
        task_data["shown_to_annotators_but_not_exported"] = "fine"
        config.validate_task_data(task_data)


class TaskDataOutputTest(unittest.TestCase):
    """Tests what a constructed payload hands to Label Studio."""

    def test_task_data_holds_every_declared_field(self) -> None:
        self.assertEqual(
            {
                "state_code": "US_CO",
                "document_id": "doc_a",
                "document_text": "Client started at Walmart as a cashier.",
                "prompt_description": "Pull employment details out of case notes.",
                "field_name": "job_title",
                "field_description": "The person's job title at the employer.",
                "group": "employers[0]",
                "extracted_value": "cashier",
                "confidence_level": "INFERRED",
                "array_element_json": (
                    '{"employer_name": "Walmart", "job_title": "cashier"}'
                ),
                "extractor_version_id": "9f8e7d",
                "doc_index": 1,
                "field_index": 2,
                "total_fields": 3,
                "task_order": 2,
            },
            _complete_cni_accuracy_payload().task_data,
        )

    def test_to_import_json_wraps_the_payload_for_upload(self) -> None:
        payload = _complete_cni_accuracy_payload()
        self.assertEqual(
            [{"data": payload.task_data}], json.loads(payload.to_import_json())
        )

    def test_to_import_json_keeps_numbers_and_nulls_unquoted(self) -> None:
        """A number that arrived as a string, or a null that arrived as "None", would fail
        the cast the parsed annotations view applies.
        """
        payload = attr.evolve(_complete_cni_accuracy_payload(), confidence_level=None)
        uploaded = json.loads(payload.to_import_json())[0]["data"]
        self.assertEqual(2, uploaded["task_order"])
        self.assertIsNone(uploaded["confidence_level"])

    def test_container_values_survive_whole(self) -> None:
        payload = _TaskDataWithContainerField(
            state_code="US_CO",
            document_id="doc_a",
            document_text="Client started at Walmart as a cashier.",
            field_name="job_title",
            group="employers[0]",
            task_order=2,
            extracted_value="cashier",
            field_description="The person's job title at the employer.",
            extractor_version_id="9f8e7d",
            annotator_hints={"prior_values": ["Walmart", "Ace Hardware"]},
        )
        expected_hints = {"prior_values": ["Walmart", "Ace Hardware"]}
        self.assertEqual(expected_hints, payload.task_data["annotator_hints"])
        uploaded = json.loads(payload.to_import_json())[0]["data"]
        self.assertEqual(expected_hints, uploaded["annotator_hints"])


class DeclaredValueTypesTest(unittest.TestCase):
    """Tests the annotation reader that TaskDataMatchesProjectConfigTest compares types
    with. A reader that returned nothing would let that test pass without comparing
    anything.
    """

    def test_plain_type(self) -> None:
        self.assertEqual({int}, _declared_value_types(int))

    def test_optional_type_drops_none(self) -> None:
        self.assertEqual({str}, _declared_value_types(str | None))

    def test_optional_spelled_the_older_way(self) -> None:
        self.assertEqual({str}, _declared_value_types(Optional[str]))

    def test_union_keeps_every_member(self) -> None:
        self.assertEqual({int, str}, _declared_value_types(str | int))

    def test_subscripted_generic_reduces_to_its_container(self) -> None:
        self.assertEqual({dict}, _declared_value_types(dict[str, Any]))

    def test_unreadable_annotation_raises(self) -> None:
        with self.assertRaisesRegex(
            ValueError, r"^Cannot read concrete types out of annotation \[a string\]\.$"
        ):
            _declared_value_types("a string")


if __name__ == "__main__":
    unittest.main()
