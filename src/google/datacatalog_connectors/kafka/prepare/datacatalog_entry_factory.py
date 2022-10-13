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

from google.cloud import datacatalog
from google.datacatalog_connectors.commons.prepare import base_entry_factory


class DataCatalogEntryFactory(base_entry_factory.BaseEntryFactory):
    __ENTRY_ID_INVALID_CHARS_REGEX_PATTERN = r'[^a-zA-Z0-9_]+'

    def __init__(self, project_id, location_id, bootstrap_servers,
        schema_registry_endpoint, entry_group_id):
        self.__project_id = project_id
        self.__location_id = location_id
        self.__bootstrap_servers = bootstrap_servers
        self.__schema_registry_endpoint = schema_registry_endpoint
        self.__entry_group_id = entry_group_id

    def make_entries_for_topic(self, topic_metadata):
        entry_id = self._format_id_with_hashing(
            topic_metadata[1]["name"].lower(),
            regex_pattern=self.__ENTRY_ID_INVALID_CHARS_REGEX_PATTERN)

        entry = datacatalog.Entry()

        entry.user_specified_type = 'topic'
        entry.user_specified_system = 'kafka'

        entry.display_name = self._format_display_name(
            topic_metadata[1]['name'])

        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            entry_id)

        entry.linked_resource = \
            self._format_linked_resource('//{}//{}'.format(
                self.__bootstrap_servers,
                topic_metadata[1]['name']
            ))

        return entry_id, entry

    def make_entry_for_schema(self, schema, schema_metadata, topic_name):
        entry_id = self.__make_entry_id_for_schema(schema, topic_name,
                                                   schema_metadata)

        entry = datacatalog.Entry()

        entry.user_specified_type = 'schema'
        entry.user_specified_system = 'kafka'

        entry.display_name = self._format_display_name(schema)

        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            entry_id)
        entry.description = f'Format: {schema_metadata["type"]}\n{schema_metadata["doc"] if "doc" in schema_metadata else ""}'

        fields = []
        if 'fields' in schema_metadata['schema']:
            for field in schema_metadata['schema']['fields']:
                if isinstance(field['type'], str):
                    name = field['name']
                    type = field['type']
                    doc = field['doc'] if 'doc' in field else None
                    col = datacatalog.ColumnSchema(
                        column=name,
                        type=type,
                        description=doc)
                    fields.append(col)
        entry.schema.columns.extend(fields)

        return entry_id, entry

    def __make_entry_id_for_schema(self, schema, topic_name, schema_metadata):
        # We normalize and hash first the topic_name.
        normalized_topic_name = self._format_id_with_hashing(
            topic_name.lower(),
            regex_pattern=self.__ENTRY_ID_INVALID_CHARS_REGEX_PATTERN)

        # Next we do the same for the table name.
        normalized_schema_name = self._format_id_with_hashing(
            schema.lower(),
            regex_pattern=self.__ENTRY_ID_INVALID_CHARS_REGEX_PATTERN)

        entry_id = '{}__{}'.format(normalized_topic_name,
                                   normalized_schema_name)

        # Then we hash the combined result again to make sure it
        # does not hit the 64 chars limit.
        return self._format_id_with_hashing(
            entry_id,
            regex_pattern=self.__ENTRY_ID_INVALID_CHARS_REGEX_PATTERN)

    @staticmethod
    def __format_entry_field_type(source_name):
        formatted_name = source_name.replace('&', '_')
        formatted_name = formatted_name.replace(':', '_')
        formatted_name = formatted_name.replace('/', '_')
        return formatted_name
