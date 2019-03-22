# Recidiviz - a data platform for criminal justice reform
# Copyright (C) 2019 Recidiviz, Inc.
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

"""Creates monitoring client for measuring and recording stats."""
import logging
from contextlib import contextmanager
from functools import wraps
from typing import Any, Dict, Optional

from opencensus.tags import TagMap, execution_context
from opencensus.stats import stats as stats_module
from opencensus.stats.exporters import stackdriver_exporter as stackdriver

from recidiviz.utils import environment, metadata

_stats = None
def stats():
    global _stats
    if not _stats:
        new_stats = stats_module.Stats()
        if environment.in_gae() and not environment.in_test():
            exporter = stackdriver.new_stats_exporter(stackdriver.Options(
                project_id=metadata.project_id(),
            ))
            new_stats.view_manager.register_exporter(exporter)
        _stats = new_stats
    return _stats


@environment.test_only
def clear_stats():
    global _stats
    _stats = None


def register_views(views):
    if environment.in_gae() and not environment.in_test():
        for view in views:
            stats().view_manager.register_view(view)


def set_thread_local_tags(tags: Dict[str, Any]):
    tag_map = execution_context.get_current_tag_map()
    if not tag_map:
        tag_map = TagMap()
        execution_context.set_current_tag_map(tag_map)

    for key, value in tags.items():
        if value:
            tag_map.insert(key, str(value))
        else:
            tag_map.delete(key)


def thread_local_tags() -> Optional[TagMap]:
    """Returns a copy of the thread local TagMap"""
    tag_map = execution_context.get_current_tag_map()
    return TagMap(tag_map.map if tag_map else None)


@contextmanager
def push_tags(tags: Dict[str, Any]):
    set_thread_local_tags(tags)
    try:
        yield
    finally:
        set_thread_local_tags({key: None for key in tags})


def with_region_tag(func):

    @wraps(func)
    def set_region_tag(region_code, *args, **kwargs):
        with push_tags({TagKey.REGION: region_code}):
            return func(region_code, *args, **kwargs)

    return set_region_tag


@contextmanager
def measurements(tags=None):
    mmap = stats().stats_recorder.new_measurement_map()
    try:
        yield mmap
    finally:
        tag_map = thread_local_tags()
        for key, value in tags.items():
            tag_map.insert(key, str(value))
        # Log to see if region not getting set is our bug or an opencensus bug.
        if not tag_map.map.get(TagKey.REGION):
            logging.warning('No region set for metric, tags are: %s',
                            tag_map.map)
        mmap.record(tag_map)


class TagKey:
    REGION = 'region'
    SHOULD_PERSIST = 'should_persist'
    PERSISTED = 'persisted'
    ERROR = 'error'
    ENTITY_TYPE = 'entity_type'
    STATUS = 'status'
