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


"""Custom configuration for our Google App Engine instances.

Specifies where installed libraries reside in the project.
Modifies our logging output format across the project.
"""


# appengine_config.py
import logging
import os
from google.appengine.ext import vendor

# Add any libraries installed in the "lib" folder.
vendor.add(os.path.join(os.path.dirname(os.path.realpath(__file__)), 'lib'))

# Override GAE logger to use our formatting
log_format = "%(module)s/%(funcName)s : %(message)s"
fr = logging.Formatter(log_format)
handlers = logging.getLogger().handlers
if handlers:
    handlers[0].setFormatter(fr)
