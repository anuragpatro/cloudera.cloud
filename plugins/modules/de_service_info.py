#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright 2021 Cloudera, Inc. All Rights Reserved.
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

from ansible.module_utils.basic import AnsibleModule
# from ansible_collections.cloudera.cloud.plugins.module_utils.cdp_common import CdpModule
from ..module_utils.cdp_common import CdpModule

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = r'''
---
module: de_service_info
short_description: Gather information about CDP Data Engineering Services.
description:
    - Gather information about CDP Data Engineering Services.
author:
  - "Anurag Patro (@anuragpatro)"
requirements:
  - cdpy
options:
  environment:
    description:
      - The name of the environment where the CDE services are enabled.
    type: str
    aliases:
      - env
  remove_deleted:
    description:
      - Filter out deleted CDE services from the list.
    type: bool
    default: False
extends_documentation_fragment:
  - cloudera.cloud.cdp_sdk_options
  - cloudera.cloud.cdp_auth_options
'''

EXAMPLES = r'''
# Note: These examples do not set authentication details.

# List information about all Data Engineering Services
- cloudera.cloud.de_service_info:

# Gather information about all Data Engineering Services within an Environment
- cloudera.cloud.de_service_info:
    env: an-example-environment-or-crn
    
# Filter out deleted Data Engineering Services and gather information
- cloudera.cloud.de_service_info:
    remove_deleted: True
'''

RETURN = r'''
---
services:
  description: List of Data Engineering Services
  type: list
  returned: always
  elements: dict
  contains:
    name:
      description: Name of the CDE Service.
      returned: always
      type: str
    clusterId:
      description: Cluster Id of the CDE Service.
      returned: always
      type: str
    environmentName:
      description: CDP Environment Name.
      returned: always
      type: str
    environmentCrn:
      description: CRN of the environment.
      returned: always
      type: str
    tenantId:
      description: CDP tenant ID.
      returned: always
      type: str
    resources:
      description: Resources details of CDE Service.
      returned: always
      type: dict
      contains:
        instance_type:
          description: Instance type of the CDE Service.
          returned: always
          type: str
        min_instances:
          description: Minimum Instances for the CDE service.
          returned: always
          type: str
        max_instances:
          description: Maximum instances for the CDE service.
          returned: always
          type: str
        initial_instances:
          description: Initial instances for the CDE service.
          returned: always
          type: str
        min_spot_instances:
          description: Minimum number of spot instances for the CDE service.
          returned: always
          type: str
        max_spot_instances:
          description: Maximum Number of Spot instances.
          returned: always
          type: str
        initial_spot_instances:
          description: Initial Spot Instances for the CDE Service.
          returned: always
          type: str
        root_vol_size:
          description: Root Volume Size.
          returned: always 
          type: str
    status:
      description: Status of the CDE service.
      returned: always
      type: str
    creatorEmail:
      description: Email address of the creator of the CDE service.
      returned: when supported
      type: str
    creatorCrn:
      description: CRN of the creator.
      returned: always
      type: str
    enablingTime:
      description: Timestamp of service enabling.
      returned: always
      type: str
    clusterFqdn:
      description: FQDN of the CDE service.
      returned: always
      type: str
    cloudPlatform:
      description: The cloud platform where the CDE service is enabled.
      returned: always
      type: str
    dataLakeFileSystems:
      description: The Data lake file system.
      returned: when supported
      type: str
    logLocation:
      description: Location for the log files of jobs.
      returned: when supported
      type: str
    dataLakeAtlasUIEndpoint:
      description: Endpoint of Data Lake Atlas.
      returned: when supported
      type: str
    chartValueOverrides:
      description: Chart overrides for the Virtual Cluster.
      returned: always
      type: list
      elements: dict
      suboptions:
        chartName:
          description: Name of the chart that has to be overridden, for eg- "dex-app", "dex-base".
          type: str
        overrides:
          description: Space separated key value-pairs for overriding chart values. The key and the value must be 
          separated using a colon(:) For eg- "airflow.enabled:true safari.enabled:true".
          type: str
sdk_out:
  description: Returns the captured CDP SDK log.
  returned: when supported
  type: str
sdk_out_lines:
  description: Returns a list of each line of the captured CDP SDK log.
  returned: when supported
  type: list
  elements: str
'''


class DeServiceInfo(CdpModule):
    def __init__(self, module):
        super(DeServiceInfo, self).__init__(module)

        # Set variables
        self.env = self._get_param('env')
        self.remove_deleted = self._get_param('remove_deleted')

        # Initialize return values
        self.services = []
        self.list_svcs = []

        # Execute logic process
        self.process()

    @CdpModule._Decorators.process_debug
    def process(self):
        # Gather list of services
        if self.env is not None:
            # Environment-specific list
            env_crn = self.cdpy.environments.resolve_environment_crn(self.env)
            if env_crn is not None:
                self.list_svcs = self.cdpy.de.list_services(self.env)
        else:
            # List of services from all environments
            self.list_svcs = self.cdpy.de.list_services(remove_deleted=self.remove_deleted)

        # Gather description of services listed
        for service in self.list_svcs:
            self.services.append(
                self.cdpy.de.describe_service(cluster_id=service['clusterId'])
            )


def main():
    module = AnsibleModule(
        argument_spec=CdpModule.argument_spec(
            env=dict(type='str', aliases=['environment']),
            remove_deleted=dict(type='bool', default=False)
        ),
        supports_check_mode=True
    )

    result = DeServiceInfo(module)
    output = dict(changed=result.changed, cluster=result.services)

    if result.debug:
        output.update(sdk_out=result.log_out, sdk_out_lines=result.log_lines)

    module.exit_json(**output)


if __name__ == '__main__':
    main()
