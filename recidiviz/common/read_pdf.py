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
# ============================================================================
"""Used to make calls from the default service to the read_pdf service in order
to run tabula to parse PDFs. """
import os
import pickle
from typing import Any

import tabula

from recidiviz.cloud_functions.cloud_function_utils import make_iap_request
from recidiviz.utils import environment
from recidiviz.utils.metadata import project_id

_CLIENT_ID = {
    'recidiviz-staging': ('984160736970-flbivauv2l7sccjsppe34p7436l6890m.apps.'
                          'googleusercontent.com'),
    'recidiviz-123': ('688733534196-uol4tvqcb345md66joje9gfgm26ufqj6.apps.'
                      'googleusercontent.com')
}


def read_pdf(
        location: str, filename: str, **kwargs: Any) -> Any:
    if environment.in_test():
        return tabula.read_pdf(filename, **kwargs)

    client_id = _CLIENT_ID[project_id()]
    url = f'https://read-pdf-dot-{project_id()}.appspot.com/read_pdf'

    response = make_iap_request(url, client_id, method='POST',
                                params={'location': location,
                                        'filename': os.path.basename(filename)},
                                json=kwargs)
    response.raise_for_status()
    return pickle.loads(response.content)
