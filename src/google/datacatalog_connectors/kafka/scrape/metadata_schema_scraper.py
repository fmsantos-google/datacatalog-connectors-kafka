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
import json
import logging
from typing import List

from confluent_kafka.schema_registry import RegisteredSchema
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry import SchemaRegistryError


class MetadataSchemaScraper:

    def __init__(self, client: SchemaRegistryClient = None):
        self.__client = client

    def get_schema_metadata(self, topic) -> List[
        RegisteredSchema]:
        result = {}
        logging.info('[Scrape] fetching schema ...')
        for subject in [f"{topic}-value", f"{topic}-key"]:
            try:
                latest_version = self.__client.get_latest_version(subject)
                result[subject] = {
                    'type': latest_version.schema.schema_type,
                    'schema': json.loads(
                        latest_version.schema.schema_str) if latest_version.schema.schema_type == 'AVRO' else None,
                    'references': latest_version.schema.references
                }
            except SchemaRegistryError as e:
                logging.error(e)
                result[subject] = None

        return result
