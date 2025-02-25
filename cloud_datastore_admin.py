# This code was copied from Google Cloud Platform documentation on automated
# Datastore exports: https://cloud.google.com/datastore/docs/schedule-export
# All rights reserved by Google, Inc.
# ==========================================================================

"""A module for performing exports of our Datastore."""

import datetime
import httplib
import json
import logging
import webapp2
from auth import authenticate_request
from google.appengine.api import app_identity
from google.appengine.api import urlfetch


class Export(webapp2.RequestHandler):
    """Request handler which supports triggering exports of our Datastore."""
    @authenticate_request
    def get(self):
        """Triggers an export of Cloud Datastore.

        The exact data that is exported is determined from parameters in the
        request.
        """
        access_token, _ = app_identity.get_access_token(
            'https://www.googleapis.com/auth/datastore')
        app_id = app_identity.get_application_id()
        timestamp = datetime.datetime.now().strftime('%Y%m%d-%H%M%S')

        output_url_prefix = self.request.get('output_url_prefix')
        assert output_url_prefix and output_url_prefix.startswith('gs://')
        if '/' not in output_url_prefix[5:]:
            # Only a bucket name has been provided - no prefix or trailing slash
            output_url_prefix += '/' + timestamp
        else:
            output_url_prefix += timestamp

        entity_filter = {
            'kinds': self.request.get_all('kind'),
            'namespace_ids': self.request.get_all('namespace_id')
        }
        request = {
            'project_id': app_id,
            'output_url_prefix': output_url_prefix,
            'entity_filter': entity_filter
        }
        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + access_token
        }
        url = 'https://datastore.googleapis.com/v1/projects/%s:export' % app_id
        try:
            result = urlfetch.fetch(
                url=url,
                payload=json.dumps(request),
                method=urlfetch.POST,
                deadline=60,
                headers=headers)
            if result.status_code == httplib.OK:
                logging.info(result.content)
            elif result.status_code >= 500:
                logging.error(result.content)
            else:
                logging.warning(result.content)
            self.response.status_int = result.status_code
        except urlfetch.Error:
            logging.exception('Failed to initiate export.')
            self.response.status_int = httplib.INTERNAL_SERVER_ERROR


app = webapp2.WSGIApplication(
    [
        ('/datastore_export', Export),
    ], debug=True)
