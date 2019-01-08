# Recidiviz - a platform for tracking granular recidivism metrics in real time
# Copyright (C) 2018 Recidiviz, Inc.
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
from contextlib import contextmanager

from opencensus.tags import TagMap
from opencensus.stats import stats as stats_module
from opencensus.stats.exporters import stackdriver_exporter as stackdriver

from recidiviz.utils import environment, metadata

_stats = None
def stats():
    global _stats
    if not _stats:
        new_stats = stats_module.Stats()
        # TODO(#59): When we want to test this, we will have to remove the
        # `in_test` check. It is currently in place so that this isn't triggered
        # even when `in_prod` is mocked out in other unit tests.
        if environment.in_prod() and not environment.in_test():
            exporter = stackdriver.new_stats_exporter(stackdriver.Options(
                project_id=metadata.project_id(),
            ))
            new_stats.view_manager.register_exporter(exporter)
        _stats = new_stats
    return _stats

def register_views(views):
    # TODO(#59): Same as above.
    if environment.in_prod() and not environment.in_test():
        for view in views:
            stats().view_manager.register_view(view)

@contextmanager
def measurements(tags=None):
    mmap = stats().stats_recorder.new_measurement_map()
    yield mmap
    tag_map = TagMap()
    for key, value in tags.items():
        tag_map.insert(key, str(value))
    mmap.record(tag_map)


class TagKey:
    REGION = 'region'
    SHOULD_PERSIST = 'should_persist'
    PERSISTED = 'persisted'
