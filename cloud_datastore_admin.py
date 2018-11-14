# This code was copied from Google Cloud Platform documentation on automated
# Datastore exports: https://cloud.google.com/datastore/docs/schedule-export
# All rights reserved by Google, Inc.
# ==========================================================================

"""A module for performing exports of our Datastore."""

import datetime
import httplib
import json
import logging

from flask import Flask, request
from google.appengine.api import app_identity, urlfetch

from recidiviz.utils.auth import authenticate_request

app = Flask(__name__)

@app.route('/datastore_export')
@authenticate_request
def datastore_export():
    """Triggers an export of Cloud Datastore.

    The exact data that is exported is determined from parameters in the
    request.
    """
    access_token, _ = app_identity.get_access_token(
        'https://www.googleapis.com/auth/datastore')
    app_id = app_identity.get_application_id()
    timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

    output_url_prefix = request.args.get('output_url_prefix')
    assert output_url_prefix and output_url_prefix.startswith('gs://')
    if '/' not in output_url_prefix[5:]:
        # Only a bucket name has been provided - no prefix or trailing slash
        output_url_prefix += '/' + timestamp
    else:
        output_url_prefix += timestamp

    ds_entity_filter = {
        'kinds': request.args.getlist('kind'),
        'namespace_ids': request.args.getlist('namespace_id')
    }
    ds_request = {
        'project_id': app_id,
        'output_url_prefix': output_url_prefix,
        'entity_filter': ds_entity_filter
    }
    ds_headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + access_token
    }
    ds_url = 'https://datastore.googleapis.com/v1/projects/%s:export' % app_id
    try:
        result = urlfetch.fetch(
            url=ds_url,
            payload=json.dumps(ds_request),
            method=urlfetch.POST,
            deadline=60,
            headers=ds_headers)
        if result.status_code == httplib.OK:
            logging.info(result.content)
        elif result.status_code >= 500:
            logging.error(result.content)
        else:
            logging.warning(result.content)
        return ('', result.status_code)
    except urlfetch.Error:
        logging.exception('Failed to initiate export.')
        return ('', httplib.INTERNAL_SERVER_ERROR)
