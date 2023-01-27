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
"""Util for launching a single Dataflow pipeline with flex templates."""
from __future__ import absolute_import

import argparse
import logging
from typing import List, Type

from recidiviz.calculator.pipeline.base_pipeline import (
    BasePipeline,
    PipelineRunDelegate,
)
from recidiviz.calculator.pipeline.utils.pipeline_run_delegate_utils import (
    collect_all_pipeline_run_delegate_classes,
)


def delegate_cls_for_pipeline_name(pipeline_name: str) -> Type[PipelineRunDelegate]:
    """Finds the PipelineRunDelegate class corresponding to the pipeline with the
    given |pipeline_name|."""
    all_run_delegates = collect_all_pipeline_run_delegate_classes()
    delegates_with_pipeline_name = [
        delegate
        for delegate in all_run_delegates
        if delegate.pipeline_config().pipeline_name.lower() == pipeline_name
    ]

    if len(delegates_with_pipeline_name) != 1:
        raise ValueError(
            "Expected exactly one PipelineRunDelegate with the "
            f"pipeline_name: {pipeline_name}. Found: {delegates_with_pipeline_name}."
        )

    return delegates_with_pipeline_name[0]


def run_flex_pipeline(pipeline_name: str, argv: List[str]) -> None:
    """Runs the given pipeline_module with the arguments contained in argv."""
    delegate_cls = delegate_cls_for_pipeline_name(pipeline_name)
    pipeline = BasePipeline(
        pipeline_run_delegate=delegate_cls.build_from_args(
            argv, use_flex_templates=True
        )
    )
    pipeline.run()


# TODO(#18108): consider creating a main for each type of pipeline (metric, normalization, supplemental)
# to avoid needing to dynamically collect delegates to use one
if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline_name", help="name of pipeline to run")

    args, other_args = parser.parse_known_args()

    run_flex_pipeline(
        pipeline_name=args.pipeline_name,
        argv=other_args,
    )
