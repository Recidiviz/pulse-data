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
"""Holds the task.data payload of one CNI per-field accuracy annotation task, which is a
single value an extractor pulled out of a single document plus everything a human needs to
decide whether it is right.

One task asks one question, such as whether the model was right to read a job_title of
cashier out of a given note. Answering it takes the note itself, what the field is supposed
to capture, and, for a value from an array element, which record it belongs to.

Every field here is a key of the uploaded payload and has to stay in step with the
task_data_fields of cni_accuracy_per_field.yaml, which projects them back into columns after
annotation. Constructing a payload checks it against that config.

This declares the payload and nothing else. The export script maps an extracted value onto it.
"""
import attr

from recidiviz.common import attr_validators
from recidiviz.llm_eval.label_studio.models.label_studio_task_data import (
    LabelStudioTaskData,
)

CNI_ACCURACY_PER_FIELD_TASK_NAME = "cni_accuracy_per_field"

# What an annotator is shown in place of a field the model left null. The sample query
# returns a real NULL for those; this is purely how the absence is rendered in the task,
# since "the document says nothing about this field" is a claim the annotator has to be
# able to read and disagree with.
NULL_VALUE_DISPLAY_TEXT = "(no value extracted)"


@attr.define(frozen=True, kw_only=True)
class CNIAccuracyPerFieldTaskData(LabelStudioTaskData):
    """Holds the task.data payload of one CNI per-field accuracy annotation task, which is a
    single extracted value plus everything a human needs to decide whether it is right.
    """

    state_code: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """State the annotated document belongs to, as a StateCode's string value, since
    task.data crosses to Label Studio as JSON and an enum cannot travel."""

    document_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Identifier of the document text the value was extracted from."""

    document_text: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """The document itself, which the annotator reads to judge the value."""

    prompt_description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """What the extractor as a whole was asked to pull out of documents like this one,
    which frames what counts as a correct value.
    """

    field_name: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Name of the field being annotated."""

    field_description: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """What that field is supposed to capture, from the extractor's output schema. For an
    ENUM field this spells out every allowed value and its meaning, so the annotator is
    judging against the same definitions the model was given.
    """

    group: str = attr.ib(validator=attr_validators.is_str)
    """Which array element the value came from, as "employers[0]", or empty string for a
    top-level field.
    """

    extracted_value: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """What the model put in the field, or NULL_VALUE_DISPLAY_TEXT when it extracted
    nothing. Never empty, because an annotator has to be able to read the claim that the
    document says nothing about this field in order to disagree with it.
    """

    confidence_level: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """How confident the model reported being, as a ConfidenceLevel's string value, since
    task.data crosses to Label Studio as JSON and an enum cannot travel. None where there is
    no confidence to report, as for a field whose array came back empty.
    """

    array_element_json: str | None = attr.ib(validator=attr_validators.is_opt_str)
    """The whole array element this value came from, as JSON, or None for a top-level field.

    It holds every sub-field the schema declares, the null ones included, since an annotator
    judging a job_title of cashier needs the employer_name beside it to know which job the
    question is about.
    """

    extractor_version_id: str = attr.ib(validator=attr_validators.is_non_empty_str)
    """Version of the extractor config that produced the value, so annotations can be
    attributed to the exact prompt they were judging.
    """

    doc_index: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-indexed position of this task's document within the batch."""

    field_index: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-indexed position of this task among those asked about the same document."""

    total_fields: int = attr.ib(validator=attr_validators.is_positive_int)
    """How many tasks the batch asks about this task's document."""

    task_order: int = attr.ib(validator=attr_validators.is_positive_int)
    """1-indexed position of this task within the whole batch."""

    @classmethod
    def task_name(cls) -> str:
        return CNI_ACCURACY_PER_FIELD_TASK_NAME
