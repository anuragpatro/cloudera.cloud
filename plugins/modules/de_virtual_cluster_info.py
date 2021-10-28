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
from ansible_dev.collections.ansible_collections.cloudera.cloud.plugins.module_utils.cdp_common import CdpModule

ANSIBLE_METADATA = {'metadata_version': '1.1',
                    'status': ['preview'],
                    'supported_by': 'community'}

DOCUMENTATION = r'''
---
module: de_virtual_cluster_info
short_description: Gather information about CDE Virtual Clusters.
description: 
    - Gather information about CDE Virtual Clusters.
author:
  - "Anurag Patro (@anuragpatro)"
requirements:
  - cdpy
options:
  cluster_id:
    description:
      - The identifier of the parent Data Engineering Service of the Virtual Cluster(s).
    type: str
  vc_id:
    description:
      - The identifier of the Virtual Cluster.
      - Requires I(cluster_id)
    type: str
extends_documentation_fragment:
  - cloudera.cloud.cdp_sdk_options
  - cloudera.cloud.cdp_auth_options
'''

EXAMPLES = r'''
# Note: These examples do not set authentication details.

# List all Virtual Clusters in a Service
- cloudera.cloud.de_virtual_cluster_info:
    cluster_id: example-cluster-id

# Describe a Virtual Cluster by ID
- cloudera.cloud.de_virtual_cluster_info:
    cluster_id: example-cluster-id
    vc_id: example-virtual-cluster-id
'''

RETURN = r'''
---
vcs:
  description: List of CDE Virtual Clusters
  type: list
  returned: always
  elements: dict
  contains:
    vcId:
      description: Virtual Cluster ID.
      returned: always
      type: str
    vcName:
      description: Name of the CDE Virtual Cluster.
      returned: always
      type: str
    clusterId:
      description: Cluster ID of the CDE service that contains the Virtual Cluster.
      returned: always
      type: str
    status:
      description: Status of the Virtual Cluster.
      returned: always
      type: str
    resources:
      description: Resources details of CDE Virtual Cluster.
      returned: always
      type: dict
      suboptions:
        cpuRequests:
          description: The CPU requests for VC for running spark jobs.
          type: str
        memRequests:
          description: The Memory requests for VC for running spark jobs.
          type: str
        actualCpuRequests:
          description: Actual CPU request for the VC. This accounts for other dex apps(eg. livy, airflow), that run in the virtual cluster.
          type: str
        actualMemoryRequests:
          description: Actual Memory request for the VC. This accounts for other dex apps(eg. livy, airflow), that run in the virtual cluster.
          type: str
    creatorEmail:
      description: Email address of the creator of Virtual Cluster.
      returned: when supported
      type: str
    creatorID:
      description: ID of the creator of Virtual Cluster.
      returned: always
      type: str
    creatorName:
      description: Name of the creator of the Virtual Cluster.
      returned: when supported
      type: str
    vcApiUrl:
      description: Url for the Virtual Cluster APIs.
      returned: always
      type: str
    VcUiUrl:
      description: URL of the CDE Virtual Cluster UI.
      returned: always
      type: str
    historyServerUrl:
      description: Spark History Server URL for the Virtual Cluster.
      returned: always
      type: str
    livyServerUrl:
      description: Livy Server URL for the Virtual Cluster.
      returned: always
      type: str
    safariUrl:
      description: Safari URL for the Virtual Cluster.
      returned: always
      type: str
    creationTime:
      description: Time of creation of the virtual Cluster.
      returned: always
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


class DeVirtualClusterInfo(CdpModule):
    def __init__(self, module):
        super(DeVirtualClusterInfo, self).__init__(module)

        # Set variables
        self.cluster_id = self._get_param('cluster_id')
        self.vc_id = self._get_param('vc_id')

        # Initialize return values
        self.virtual_clusters = []

        # Execute logic process
        self.process()

    @CdpModule._Decorators.process_debug
    def process(self):
        if self.vc_id is not None:
            target = self.cdpy.de.describe_vc(cluster_id=self.cluster_id, vc_id=self.vc_id)
            if target is not None:
                self.virtual_clusters.append(target)
        else:
            vcs = self.cdpy.de.list_vcs(cluster_id=self.cluster_id)
            for vc in vcs:
                self.virtual_clusters.append(
                    self.cdpy.de.describe_vc(cluster_id=self.cluster_id, vc_id=vc['vcId'])
                )


def main():
    module = AnsibleModule(
        argument_spec=CdpModule.argument_spec(
            cluster_id=dict(required=True, type='str'),
            vc_id=dict(type='str', aliases=['virtual_cluster_id'])
        ),
        supports_check_mode=True
    )

    result = DeVirtualClusterInfo(module)
    output = dict(changed=result.changed, cluster=result.virtual_clusters)

    if result.debug:
        output.update(sdk_out=result.log_out, sdk_out_lines=result.log_lines)

    module.exit_json(**output)


if __name__ == '__main__':
    main()
