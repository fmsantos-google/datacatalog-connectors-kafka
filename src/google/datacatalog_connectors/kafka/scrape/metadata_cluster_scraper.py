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

from confluent_kafka.admin import AdminClient
from confluent_kafka.admin import ClusterMetadata


class MetadataClusterScraper:

    def __init__(self, adm_client: AdminClient = None):
        self.__adm_client = adm_client

    def get_cluster_metadata(self) -> ClusterMetadata:
        try:
            logging.info('[Scrape] fetching metadata ...')

            cluster_metadata: ClusterMetadata = self.__adm_client.list_topics()

            return cluster_metadata

        except Exception as e:
            logging.error('Error retrieving metadata from Kafka', e)
            raise
