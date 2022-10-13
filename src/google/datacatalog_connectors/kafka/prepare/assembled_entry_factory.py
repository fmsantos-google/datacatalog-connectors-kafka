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

from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.kafka.prepare import \
    datacatalog_entry_factory


class AssembledEntryFactory:

    def __init__(self, project_id, location_id, bootstrap_servers,
        schema_registry_endpoint, entry_group_id):
        self.__datacatalog_entry_factory = \
            datacatalog_entry_factory.DataCatalogEntryFactory(
                project_id,
                location_id,
                bootstrap_servers,
                schema_registry_endpoint,
                entry_group_id)

    def make_entries_from_topic_metadata(self, topics_metadata):
        assembled_entries = []
        for topic in topics_metadata.items():
            assembled_topic = self.__make_entries_for_topic(topic)

            logging.info('\n--> Topic: %s', topic['name'])
            logging.info('\n%s schemas ready to be ingested...', len(topic['schemas']))
            assembled_schemas = self.__make_entry_for_schemas(topic['schemas'], topic['name'])

            assembled_entries.append((assembled_topic, assembled_schemas))
        return assembled_entries

    def __make_entries_for_topic(self, topic):
        entry_id, entry = self. \
            __datacatalog_entry_factory.make_entries_for_topic(topic)

        return prepare.AssembledEntryData(entry_id, entry)

    def __make_entry_for_schemas(self, tables_dict, database_name):
        entries = []
        for table_dict in tables_dict:
            entry_id, entry = self. \
                __datacatalog_entry_factory.make_entry_for_table(table_dict,
                                                                 database_name)

            entries.append(prepare.AssembledEntryData(entry_id, entry))
        return entries
