
# -*- coding: utf-8 -*-

# Copyright (c) 2018 CoNWeT Lab., Universidad Politécnica de Madrid

# This file is part of BAE Umbrella Service plugin.

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

import requests
from datetime import datetime, timedelta
from urlparse import urlparse, urljoin

from django.core.exceptions import PermissionDenied
from django.conf import settings

from wstore.asset_manager.resource_plugins.plugin import Plugin
from wstore.asset_manager.resource_plugins.plugin_error import PluginError
from wstore.models import User

from settings import UNITS
from umbrella_client import UmbrellaClient
from keystone_client import KeystoneClient


class OrionQuery(Plugin):

    def __init__(self, plugin_model):
        super(OrionQuery, self).__init__(plugin_model)
        self._units = UNITS

    def _get_umbrella_client(self, url, credentials):
        parsed_url = urlparse(url)
        server = '{}://{}'.format(parsed_url.scheme, parsed_url.netloc)

        return UmbrellaClient(server, credentials['token'], credentials['key'])

    def _get_keystone_client(self, credentials):
        keystone_client = KeystoneClient()
        keystone_client.set_app_id(credentials['app_id'])

        return keystone_client

    def _check_api(self, url, token, key, server):
        parsed_url = urlparse(url)
        parsed_server = urlparse(server)

        server_endpoint = '{}://{}'.format(parsed_server.scheme, parsed_server.netloc)

        umbrella_client = UmbrellaClient(server_endpoint, token, key)
        return umbrella_client.validate_service(parsed_url.path)

    def on_post_product_spec_validation(self, provider, asset):
        # If customer access to subpaths is allowed, the URL must not include query string
        url = asset.get_url()
        parsed_url = urlparse(url)

        # Validate that the provided URL is an orion query
        if 'v2/entities' not in parsed_url.path or parsed_url.query == '':
            raise PluginError('The provided URL is not a valid Context Broker query')

        # Check that the URL provided in the asset is a valid API Umbrella service
        token = asset.meta_info['admin_token']
        key = asset.meta_info['admin_key']
        server = asset.meta_info['api_umbrella_server']

        asset.meta_info['app_id'] = self._check_api(url, token, key, server)

        keystone_client = self._get_keystone_client({
            'app_id': asset.meta_info['app_id']
        })
        keystone_client.check_ownership(provider.name)
        keystone_client.check_role(asset.meta_info['app_id'], asset.meta_info['role'])

        asset.save()

    def on_post_product_offering_validation(self, asset, product_offering):
        # Validate that the pay-per-use model (if any) is supported by the backend
        if 'productOfferingPrice' in product_offering:
            has_usage = False
            supported_units = [unit['name'].lower() for unit in self._units]

            for price_model in product_offering['productOfferingPrice']:
                if price_model['priceType'] == 'usage':
                    has_usage = True

                    if price_model['unitOfMeasure'].lower() not in supported_units:
                        raise PluginError('Unsupported accounting unit ' +
                                          price_model['unit'] + '. Supported units are: ' + ','.join(supported_units))

    def on_product_acquisition(self, asset, contract, order):
        # Activate API resources
        token = asset.meta_info['admin_token']
        key = asset.meta_info['admin_key']
        server = asset.meta_info['api_umbrella_server']

        client = self._get_keystone_client({
            'app_id': asset.meta_info['app_id']
        })

        client.grant_permission(order.customer, asset.meta_info['role'])

    def on_product_suspension(self, asset, contract, order):
        # Suspend API Resources
        token = asset.meta_info['admin_token']
        key = asset.meta_info['admin_key']
        server = asset.meta_info['api_umbrella_server']

        client = self._get_keystone_client({
            'app_id': asset.meta_info['app_id']
        })

        client.revoke_permission(order.customer, asset.meta_info['role'])

    ####################################################################
    #######################  Accounting Handlers #######################
    ####################################################################

    def get_usage_specs(self):
        return self._units

    def get_pending_accounting(self, asset, contract, order):
        accounting = []
        last_usage = None
        # Read pricing model to know the query to make
        if 'pay_per_use' in contract.pricing_model:
            unit = contract.pricing_model['pay_per_use'][0]['unit']

            # Read the date of the last SDR
            if contract.last_usage is not None:
                start_at = unicode(contract.last_usage.isoformat()).replace(' ', 'T') + 'Z'
            else:
                # The maximum time between refreshes is 30 days, so in the worst case
                # consumption started 30 days ago
                start_at = unicode((datetime.utcnow() - timedelta(days=31)).isoformat()).replace(' ', 'T') + 'Z'

            # Retrieve pending usage
            last_usage = datetime.utcnow()
            end_at = unicode(last_usage.isoformat()).replace(' ', 'T') + 'Z'

            # Check the accumulated usage for all the resources of the dataset
            # Accounting is always done by Umbrella no mather who validates permissions
            token = asset.meta_info['admin_token']
            key = asset.meta_info['admin_key']

            url = asset.get_url()
            server = asset.meta_info['api_umbrella_server']

            client = self._get_umbrella_client(server, {
                'token': token,
                'key': key
            })
            accounting.extend(client.get_drilldown_by_service(order.customer.email, url, start_at, end_at, unit.lower()))

        return accounting, last_usage
