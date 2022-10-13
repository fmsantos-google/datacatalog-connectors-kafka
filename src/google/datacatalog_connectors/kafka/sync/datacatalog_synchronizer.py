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

import logging
import uuid

from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from google.datacatalog_connectors.commons.cleanup \
    import datacatalog_metadata_cleaner
from google.datacatalog_connectors.commons.monitoring \
    import metrics_processor

# from google.datacatalog_connectors.kafka import entities
from google.datacatalog_connectors.kafka import scrape
from google.datacatalog_connectors.kafka.prepare import \
    assembled_entry_factory


class DataCatalogSynchronizer:

    def __init__(self, project_id, location_id, bootstrap_servers,
        kafka_api_key=None, kafka_api_secret=None, sr_endpoint=None,
        sr_api_key=None, sr_api_secret=None, enable_monitoring=None):
        self.__entry_group_id = 'kafka'
        self.__project_id = project_id
        self.__location_id = location_id
        self.__bootstrap_servers = bootstrap_servers
        self.__kafka_api_key = kafka_api_key
        self.__kafka_api_secret = kafka_api_secret
        self.__schema_registry_endpoint = sr_endpoint
        self.__schema_registry_api_key = sr_api_key
        self.__schema_registry_api_secret = sr_api_secret
        self.__task_id = uuid.uuid4().hex[:8]
        self.__metrics_processor = metrics_processor.MetricsProcessor(
            project_id, location_id, self.__entry_group_id, enable_monitoring,
            self.__task_id)
        self.__adm_client = None
        self.__sr_client = None

    def __get_admin_client(self):
        if not self.__adm_client:
            self.__adm_client = AdminClient(conf={
                'bootstrap.servers': self.__bootstrap_servers,
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': self.__kafka_api_key,
                'sasl.password': self.__kafka_api_secret
            })
        return self.__adm_client

    def __get_sr_client(self):
        if not self.__sr_client:
            self.__sr_client = SchemaRegistryClient(conf={
                "url": self.__schema_registry_endpoint,
                "basic.auth.user.info": "{api_key}:{api_secret}".format(
                    api_key=self.__schema_registry_api_key,
                    api_secret=self.__schema_registry_api_secret
                )
            })
        return self.__sr_client

    def run(self):
        logging.info('\n==============Start kafka-to-datacatalog============')

        logging.info('\n\n==============Scrape metadata===============')

        cluster_scraper = scrape.MetadataClusterScraper(
            self.__get_admin_client())

        schema_scraper = scrape.MetadataSchemaScraper(self.__get_sr_client())

        cluster_metadata = cluster_scraper.get_cluster_metadata()

        topics_metadata = {}
        for topic, metadata in cluster_metadata.topics.items():
            schemas_metadata = schema_scraper.get_schema_metadata(topic)
            topic_metadata = {
                'name': metadata.topic,
                'broker': cluster_metadata.orig_broker_name,
                'schemas': {
                    f'{topic}-key': schemas_metadata[f'{topic}-key'],
                    f'{topic}-value': schemas_metadata[f'{topic}-value']
                }
            }
            topics_metadata[topic] = topic_metadata

        logging.info('\n{}'.format(
            len(topics_metadata)) + ' topics ready to be ingested...')

        logging.info('\n\n==============Prepare metadata===============')
        # Prepare.
        logging.info('\nPreparing the metadata...')

        factory = assembled_entry_factory.AssembledEntryFactory(
            self.__project_id, self.__location_id, self.__bootstrap_servers,
            self.__schema_registry_endpoint, self.__entry_group_id)

        prepared_entries = factory.make_entries_from_topic_metadata(topics_metadata)

        self.__log_metadata(cluster_metadata)
        self.__log_entries(prepared_entries)

        logging.info('\n==============Ingest metadata===============')

        cleaner = datacatalog_metadata_cleaner.DataCatalogMetadataCleaner(
            self.__project_id, self.__location_id, self.__entry_group_id)

        # Since we can't rely on search returning the ingested entries,
        # we clean up the obsolete entries before ingesting.
        # if sync_event == entities.SyncEvent.MANUAL_DATABASE_SYNC:
        #     assembled_entries_data = []
        #     for database_entry, table_related_entries in prepared_entries:
        #         assembled_entries_data.extend(
        #             [database_entry, *table_related_entries])
        #
        #     cleaner.delete_obsolete_metadata(
        #         assembled_entries_data,
        #         'system={}'.format(self.__entry_group_id))
        #
        #     del assembled_entries_data
        #
        # # Ingest.
        # logging.info('\nStarting to ingest custom metadata...')
        # if sync_event not in self.__CLEAN_UP_EVENTS:
        #     self.__ingest_created_or_updated(prepared_entries)
        # elif sync_event == entities.SyncEvent.DROP_DATABASE:
        #     self.__cleanup_deleted_databases(cleaner, prepared_entries)
        # elif sync_event == entities.SyncEvent.DROP_TABLE:
        #     self.__cleanup_deleted_tables(cleaner, prepared_entries)

        logging.info('\n==============End hive-to-datacatalog===============')
        self.__after_run()

        return self.__task_id

    #
    # def __ingest_created_or_updated(self, prepared_entries):
    #     ingestor = datacatalog_metadata_ingestor.DataCatalogMetadataIngestor(
    #         self.__project_id, self.__location_id, self.__entry_group_id)
    #     for database_entry, table_related_entries in prepared_entries:
    #         ingestor.ingest_metadata([database_entry, *table_related_entries])
    #
    #
    # def __after_run(self):
    #     self.__metrics_processor.process_elapsed_time_metric()
    #
    #
    # def __log_entries(self, prepared_entries):
    #     entries_len = sum([len(tables) for (_, tables) in prepared_entries],
    #                       len(prepared_entries))
    #     self.__metrics_processor.process_entries_length_metric(entries_len)
    #
    #
    # def __log_metadata(self, metadata):
    #     # sqlalchemy uses a object proxy, so we must convert it
    #     # before generating the metric.
    #     databases_json = json.dumps(
    #         ([database.dump() for database in metadata['databases']]))
    #     self.__metrics_processor.process_metadata_payload_bytes_metric(
    #         databases_json)
    #
    #
    # @classmethod
    # def __cleanup_deleted_tables(cls, cleaner, prepared_entries):
    #     for _, table_related_entries in prepared_entries:
    #         try:
    #             cleaner.delete_metadata(table_related_entries)
    #             logging.info('\nTables deleted: {}'.format(
    #                 len(table_related_entries)))
    #         except:  # noqa: E722
    #             logging.info('Exception deleting Entries')
    #
    #
    # @classmethod
    # def __cleanup_deleted_databases(cls, cleaner, prepared_entries):
    #     for database_entry, _ in prepared_entries:
    #         databases = [database_entry]
    #         try:
    #             cleaner.delete_metadata(databases)
    #             logging.info('\nDatabases deleted: {}'.format(len(databases)))
    #         except:  # noqa: E722
    #             logging.info('Exception deleting Entries')
