#!/usr/bin/python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse

import logging
import os
import sys

from google.datacatalog_connectors.kafka.sync import datacatalog_synchronizer


class KafkaSchemaRegistry2DatacatalogCli:

    @staticmethod
    def run(argv):
        args = KafkaSchemaRegistry2DatacatalogCli.__parse_args(argv)
        # Enable logging
        logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

        if args.service_account_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] \
                = args.service_account_path

        # load schema registry synchronizer
        datacatalog_synchronizer.DataCatalogSynchronizer(
            project_id=args.datacatalog_project_id,
            location_id=args.datacatalog_location_id,
            bootstrap_servers=args.kafka_bootstrap_servers,
            kafka_api_key=args.kafka_api_key,
            kafka_api_secret=args.kafka_api_secret,
            sr_endpoint=args.schema_registry_endpoint,
            sr_api_key=args.schema_registry_api_key,
            sr_api_secret=args.schema_registry_api_secret,
            enable_monitoring=args.enable_monitoring).run()

    @staticmethod
    def __parse_args(argv):
        parser = argparse.ArgumentParser(
            description='Command line to sync Kafka Schema Registry to Datacatalog')

        parser.add_argument('--datacatalog-project-id',
                            help='Your Google Cloud project ID',
                            required=True)
        parser.add_argument('--datacatalog-location-id',
                            help='Location ID to be used for your Google Cloud Datacatalog',
                            required=True)
        parser.add_argument('--kafka-bootstrap-servers',
                            help='Kafka Bootstrap Servers list (host:port)',
                            required=True)
        parser.add_argument('--kafka-api-key',
                            help='Kafka API Key',
                            required=True)
        parser.add_argument('--kafka-api-secret',
                            help='Kafka API Secret',
                            required=True)
        parser.add_argument('--schema-registry-endpoint',
                            help='Schema Registry Endpoint URL',
                            required=True)
        parser.add_argument('--schema-registry-api-key',
                            help='Schema Registry API Key',
                            required=True)
        parser.add_argument('--schema-registry-api-secret',
                            help='Schema Registry API Secret',
                            required=True)
        parser.add_argument('--service-account-path',
                            help='Local Service Account path '
                                 '(Can be suplied as GOOGLE_APPLICATION_CREDENTIALS env var)')
        parser.add_argument('--enable-monitoring',
                            help='Enables monitoring metrics on the connector')
        return parser.parse_args(argv)


def main():
    argv = sys.argv
    KafkaSchemaRegistry2DatacatalogCli().run(
        argv[1:] if len(argv) > 0 else argv)


if __name__ == '__main__':
    main()
