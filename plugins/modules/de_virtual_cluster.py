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
module: de_virtual_cluster
short_description: Create or Delete CDE Virtual Cluster.
description:
    - Create or Delete CDE Virtual Cluster.
author:
  - "Anurag Patro (@anuragpatro)"
requirements:
  - cdpy
options:
  name:
    description:
      - The name of the CDE Virtual Cluster.
      - Required if I(state=present).
    type: str
  vc_id:
    description:
      - The identifier of the CDE Virtual Cluster.
      - Required if I(state=absent).
    type: str
  cluster_id:
    description:
      - The identifier of the Data Engineering Service.
      - Required if I(state=present).
      - Required if I(state=absent).
    type: str
  cpu_requests:
    description:
      - Maximum number of CPU cores requested for autoscaling.
      - Required if I(state=present).
    type: str
  memory_requests:
    description:
      - Maximum amount of memory requested for autoscaling - eg. 30Gi.
      - Required if I(state=present).
    type: str
  chart_value_overrides:
    description:
    type:
      - Chart overrides for creating a CDE Virtual Cluster.
    type: list
    elements: dict
    suboptions:
      chartName:
        description:
          - Name of the chart that has to be overridden, for eg- "dex-app", "dex-base".
        type: str
      overrides:
        description: 
          - Space separated key-value pairs for overriding chart values. The key and the value must be separated using 
          a colon(:) For eg- "airflow.enabled:true safari.enabled:true".
        type: str
  runtime_spot_component:
    description:
      - Used to describe where the Driver and the Executors would run. By default the Driver would run on on-demand 
      instances and the Executors on spot instances. Setting it to ALL will run both the Driver and the Executors on 
      spot instances whereas setting it to NONE should run both the Driver and the Executor on on-demand instances. 
      Currently applicable for aws services only. Use this option only on services with spot instances enabled.
    type: string
    choices:
      - ALL
      - NONE
  state:
    description: 
      - The state of the Data Warehouse Cluster.
    type: str
    default: present
    choices:
      - present
      - absent
  wait:
    description:
      - Flag to enable internal polling to wait for the Data Warehouse Cluster to achieve the declared state.
      - If set to FALSE, the module will return immediately.
    type: bool
    default: True
  force:
    description:
      - Flag to enable force deletion of the Data Warehouse Cluster.
      - This will not destroy the underlying cloud provider assets.
    type: bool
    default: False
  delay:
    description:
      - The internal polling interval (in seconds) while the module waits for the Data Warehouse Cluster to achieve the 
      declared state.
    type: int
    default: 15
    aliases:
      - polling_delay
  timeout:
    description:
      - The internal polling timeout (in seconds) while the module waits for the Data Warehouse Cluster to achieve the 
      declared state.
    type: int
    default: 3600
    aliases:
      - polling_timeout
extends_documentation_fragment:
  - cloudera.cloud.cdp_sdk_options
  - cloudera.cloud.cdp_auth_options
'''

EXAMPLES = r'''
# Note: These examples do not set authentication details.

# Create a Virtual Cluster
- cloudera.cloud.de_virtual_cluster:
    name: my-virtual-cluster
    cluster_id: my-cluster-id
    cpu_requests: 20
    memory_requests: 80
    
# Create a Virtual Cluster that doesn't utilise spot instances
- cloudera.cloud.de_virtual_cluster:
    name: my-virtual-cluster
    cluster_id: my-cluster-id
    cpu_requests: 20
    memory_requests: 80
    runtime_spot_component: NONE
    
# Delete a Virtual Cluster
- cloudera.cloud.de_virtual_cluster:
    state: absent
    cluster_id: my-cluster-id
    vc_id: my-virtual-cluster-id
'''

RETURN = r'''
---
vc:
  description: Description of the CDE Virtual Cluster
  type: dict
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
          description: Actual CPU request for the VC. This accounts for other dex apps(eg. livy, airflow), that run in 
          the virtual cluster.
          type: str
        actualMemoryRequests:
          description: Actual Memory request for the VC. This accounts for other dex apps(eg. livy, airflow), that run 
          in the virtual cluster.
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


class DeVirtualCluster(CdpModule):
    def __init__(self, module):
        super(DeVirtualCluster, self).__init__(module)

        # Set variables
        self.name = self._get_param('name')
        self.vc_id = self._get_param('vc_id')
        self.cluster_id = self._get_param('cluster_id')
        self.cpu_requests = self._get_param('cpu_requests')
        self.memory_requests = self._get_param('memory_requests')
        self.chart_value_overrides = self._get_param('chart_value_overrides')
        self.runtime_spot_component = self._get_param('runtime_spot_component')
        self.state = self._get_param('state')
        self.force = self._get_param('force')
        self.wait = self._get_param('wait')
        self.delay = self._get_param('delay')
        self.timeout = self._get_param('timeout')

        # Initialize return values
        self.virtual_cluster = dict()
        self.changed = False

        # Initialize internal values
        self.target = None

        # Execute logic process
        self.process()

    @CdpModule._Decorators.process_debug
    def process(self):

        # Check if VC exists
        self.target = self.cdpy.de.describe_vc(cluster_id=self.cluster_id, vc_id=self.vc_id)

        if self.state in ['present']:
            # If VC exists
            if self.target is not None:
                self.virtual_cluster = self.target
                # Fail if VC in a failed state
                if self.target['status'] in self.cdpy.sdk.FAILED_STATES:
                    self.module.fail_json(msg="Attempting to restart a failed virtual cluster.")

                # Check if VC is being created or has started
                elif self.target['status'] in self.cdpy.sdk.STARTED_STATES + self.cdpy.sdk.CREATION_STATES:
                    if not self.wait:
                        self.module.warn('Virtual Cluster already exists and reconciliation is not implemented yet.')
                    else:
                        # Wait till creation completes
                        self.virtual_cluster = self.cdpy.sdk.wait_for_state(
                            describe_func=self.cdpy.de.describe_vc,
                            params=dict(cluster_id=self.cluster_id, vc_id=self.vc_id),
                            state=self.cdpy.sdk.STARTED_STATES, delay=self.delay, timeout=self.timeout
                        )
            # If VC does not exist
            else:
                if not self.module.check_mode:
                    # Create VC
                    self.virtual_cluster = self.cdpy.de.create_vc(
                        name=self.name, cluster_id=self.cluster_id, cpu_requests=self.cpu_requests,
                        memory_requests=self.memory_requests, chart_value_overrides=self.chart_value_overrides,
                        runtime_spot_component=self.runtime_spot_component
                    )
                    self.changed = True
                    if self.wait:
                        self.virtual_cluster = self.cdpy.sdk.wait_for_state(
                            describe_func=self.cdpy.de.describe_vc,
                            params=dict(cluster_id=self.cluster_id, vc_id=self.vc_id),
                            state=self.cdpy.sdk.STARTED_STATES, delay=self.delay, timeout=self.timeout
                        )
                    else:
                        self.virtual_cluster = self.cdpy.de.describe_vc(cluster_id=self.cluster_id, vc_id=self.vc_id)
        elif self.state in ['absent']:
            if not self.module.check_mode:
                # If VC exists
                if self.target is not None:
                    # Warn if VC in termination cycle
                    if self.target['status'] not in self.cdpy.sdk.REMOVABLE_STATES:
                        self.module.warn("Cluster is not in a valid state for Delete operations: %s" % self.target['status'])
                    else:
                        # Delete VC
                        self.target = self.cdpy.de.delete_vc(cluster_id=self.cluster_id, vc_id=self.vc_id)
                        self.changed = True
                    # Wait
                    if self.wait:
                        self.target = self.cdpy.sdk.wait_for_state(
                            describe_func=self.cdpy.de.describe_vc,
                            params=dict(cluster_id=self.cluster_id, vc_id=self.vc_id),
                            field=None, delay=self.delay, timeout=self.timeout
                        )
                    self.virtual_cluster = self.target
        else:
            self.module.fail_json(msg="State %s is not valid for this module" % self.state)


def main():
    module = AnsibleModule(
        argument_spec=CdpModule.argument_spec(
            name=dict(type='str'),
            vc_id=dict(type='str', aliases=['virtual_cluster_id']),
            cluster_id=dict(type='str', required=True),
            cpu_requests=dict(type='str'),
            memory_requests=dict(type='str'),
            chart_value_overrides=dict(type='dict'),
            runtime_spot_component=dict(type='str', choices=['ALL', 'NONE']),
            state=dict(type='str', choices=['present', 'absent'], default='present'),
            force=dict(type='bool', default=False),
            wait=dict(type='bool', default=True),
            delay=dict(type='int', aliases=['polling_delay'], default=15),
            timeout=dict(type='int', aliases=['polling_timeout'], default=3600)
        ),
        supports_check_mode=True
    )

    result = DeVirtualCluster(module)
    output = dict(changed=result.changed, cluster=result.virtual_cluster)

    if result.debug:
        output.update(sdk_out=result.log_out, sdk_out_lines=result.log_lines)

    module.exit_json(**output)


if __name__ == '__main__':
    main()
