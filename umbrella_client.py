# -*- coding: utf-8 -*-

# Copyright (c) 2018 CoNWeT Lab., Universidad Politécnica de Madrid

# This file is part of BAE Umbrella service plugin.

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import unicode_literals

import json
import requests
import urllib
from datetime import datetime
from urlparse import urljoin, urlparse, parse_qs

from django.core.exceptions import PermissionDenied

from wstore.asset_manager.resource_plugins.plugin_error import PluginError


class UmbrellaClient(object):

    def __init__(self, server, token, key):
        self._server = server
        self._token = token
        self._key = key
        self._accounting_processor = {
            'api call': self._process_call_accounting
        }

    def _make_request(self, path, method, **kwargs):
        url = urljoin(self._server, path)
        try:
            resp = method(url, **kwargs)
        except requests.ConnectionError:
            raise PermissionDenied('Invalid resource: API Umbrella server is not responding')

        if resp.status_code == 404:
            raise PluginError('The provided Umbrella resource does not exist')
        elif resp.status_code != 200:
            raise PluginError('Umbrella gives an error accessing the provided resource')

        return resp

    def _get_request(self, path):
        resp = self._make_request(path, requests.get, headers={
            'X-Api-Key': self._key,
            'X-Admin-Auth-Token': self._token
        }, verify=False)

        return resp.json()

    def _put_request(self, path, body):
        self._make_request(path, requests.get, json=body, headers={
            'X-Api-Key': self._key,
            'X-Admin-Auth-Token': self._token
        }, verify=False)

    def _post_request(self, path, body):
        resp = self._make_request(path, requests.post, data=body, headers={
            'X-Api-Key': self._key,
            'X-Admin-Auth-Token': self._token
        }, verify=False)

        return resp.json()

    def _paginate_data(self, url, err_msg, page_processor):
        page_len = 100
        start = 0
        processed = False
        matching_elem = None

        while not processed:
            result = self._get_request(url + '&start={}&length={}'.format(start, page_len))

            # There is no remaining elements
            if not len(result['data']):
                raise PluginError(err_msg)
            
            for elem in result['data']:
                processed = page_processor(elem)

                # The page element has been found
                if processed:
                    matching_elem = elem
                    break
            
            start += page_len

        return matching_elem

    def validate_service(self, path):
        err_msg = 'The provided asset is not supported. ' \
                  'Only services protected by API Umbrella are supported'

        # Split the path of the service 
        paths = [p for p in path.split('/') if p != '']
        if not len(paths):
            # API umbrella resources include a path for matching the service
            raise PluginError(err_msg)

        # Make paginated requests to API umbrella looking for the provided paths
        url = '/api-umbrella/v1/apis.json?search[value]={}&search[regex]=false'.format(paths[0])
        def page_processor(api):
            front_path = [p for p in api['frontend_prefixes'].split('/') if p != '']
            return len(front_path) <= len(paths) and front_path == paths[:len(front_path)]

        matching_elem = self._paginate_data(url, err_msg, page_processor)

        # If the API is configured to accept access tokens from an external IDP save its external id
        app_id = None
        if 'idp_app_id' in matching_elem['settings'] and len(matching_elem['settings']['idp_app_id']):
            app_id = matching_elem['settings']['idp_app_id']

        return app_id

    def check_role(self, role):
        # Check that the provided role already exists, in order to avoid users creating new roles
        # using this service
        existing_roles = self._get_request('api-umbrella/v1/user_roles')
        for existing_role in existing_roles['user_roles']:
            if existing_role['id'] == role:
                break
        else:
            raise PluginError('The role {} does not exist in API Umbrella instance'.format(role))

    def _get_user_model(self, email):
        # Search users using the email field
        url = '/api-umbrella/v1/users?search[value]={}'.format(email)
        err_msg = 'There is not any user registered in Umbrella instance with email: {}'.format(email)

        def page_processor(user_result):
            return user_result['email'] == email

        matching_user = self._paginate_data(url, err_msg, page_processor)
        user_id = matching_user['id']

        # Get user model
        return self._get_request('/api-umbrella/v1/users/{}'.format(user_id))

    def _filter_roles(self, user_model, role):
        new_roles = []
        if user_model['user']['roles'] is not None:
            # Parse existing roles
            new_roles = [user_role for user_role in user_model['user']['roles'] if user_role != role]

        return new_roles

    def grant_permission(self, user, role):
        self.check_role(role)
        user_model = self._get_user_model(user.email)

        # Update user roles
        new_roles = self._filter_roles(user_model, role).append(role)

        user_model['user']['roles'] = new_roles

        self._put_request('/api-umbrella/v1/users/{}'.format(user_model['user']['id']), user_model)

    def revoke_permission(self, user, role):
        self.check_role(role)
        user_model = self._get_user_model(user.email)
        user_model['user']['roles'] = self._filter_roles(user_model, role)
        self._put_request('/api-umbrella/v1/users/{}'.format(user_model['user']['id']), user_model)

    def _get_rule(self, field, value, operator='equal'):
        return {
            'id': field,
            'field': field,
            'type': 'string',
            'input': 'text',
            'operator': operator,
            'value': value
        }
    
    def _get_null_rule(self, field):
        return {
            'id': field,
            'field': field,
            'type': 'string',
            'input': 'select',
            'operator': 'is_null',
            'value': None
        }

    def _paginate_accounting(self, params, accounting, accounting_aggregator, unit):
        page_len = 500
        start = 0
        processed = False

        current_date = None
        current_value = 0

        while not processed:
            params['start'] = start
            params['length'] = page_len
            result = self._post_request('/api-umbrella/v1/analytics/logs.json', params)

            # There is no remaining elements
            if not len(result['data']):
                processed = True

            for elem in result['data']:
                # Process log timestamp (Which includes milliseconds)
                date = datetime.utcfromtimestamp(elem['request_at']/1000.0)
                day = date.date()

                if current_date is None:
                    # New day to be aggregated
                    current_date = day

                # If new day is higher save the accounting info
                if day > current_date:
                    accounting.append({
                        'unit': unit,
                        'value': current_value,
                        'date': unicode(current_date.isoformat()) + 'T00:00:00Z'
                    })

                    # Set current day and reset value
                    current_date = day
                    current_value = 0

                current_value += accounting_aggregator(elem)
            start += page_len

        # Save last info
        if current_value > 0:
            accounting.append({
                'unit': unit,
                'value': current_value,
                'date': unicode(current_date.isoformat()) + 'T00:00:00Z'
            })

    def _process_call_accounting(self, params, parsed_url):
        def list_equal_elems(list1, list2):
            intersect = set(list2).intersection(list1)
            return len(list1) == len(list2) == len(intersect)

        accounting = []
        def call_aggregator(elem):
            account = 1
            # Filter query strings during aggregation to enable changing the order and
            # included extra params when enabled
            if len(parsed_url.query):
                parsed_elem_qs = parse_qs(elem['request_url_query'])
                url_qs = parse_qs(parsed_url.query)

                # Check that all the query strings of the asset URL are included
                for key, value in url_qs.iteritems():
                    if key not in parsed_elem_qs or not list_equal_elems(value, parsed_elem_qs[key]):
                        account = 0
                        break

            return account

        self._paginate_accounting(params, accounting, call_aggregator, 'api call')

        return accounting

    def get_drilldown_by_service(self, email, service, start_at, end_at, unit):
        parsed_url = urlparse(service)

        rules = [
            self._get_null_rule('gatekeeper_denied_code'),
            self._get_rule('user_email', email)
        ]

        # Include path rule depending on whether subpath requests are allowed
        path_operator = 'equal'
        rules.append(self._get_rule('request_path', parsed_url.path, operator=path_operator))

        query = {
            'condition': 'AND',
            'rules': rules,
            'valid': True
        }

        params = {
            'start_at': start_at,
            'end_at': end_at,
            'query': json.dumps(query)
        }

        return self._accounting_processor[unit](params, parsed_url)
