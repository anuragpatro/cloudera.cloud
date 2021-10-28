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
module: de_service
short_description: Create or Delete CDP Data Engineering Service.
description:
    - Create or Delete CDP Data Engineering Service.
author:
  - "Anurag Patro" (@anuragpatro)
requirements:
  - cdpy
options:
  name:
    description:
      - The name of the Data Engineering Service.
      - Required if I(state=present).
    type: str
  cluster_id:
    description:
      - The identifier of the Data Engineering Service.
      - Required if I(state=absent).
    type: str
  env:
    description:
      - The name of the target environment.
      - Required if I(state=present).
    type: str
    aliases:
      - environment
      - env_crn
  instance_type:
    description:
      - Instance type of the cluster for CDE service.
      - Required if I(state=present).
    type: str
  minimum_instances:
    description:
      - Minimum Instances for the CDE Service.
      - Required if I(state=present).
    type: int
  maximum_instances:
    description:
      - Maximum Instances for the CDE Service.
      - Required if I(state=present).
    type: int
  minimum_spot_instances:
    description:
      - Minimum Spot instances for the CDE Service.
    type: int
  maximum_spot_instances:
    description:
      - Maximum Spot Instances for the CDE Service.
    type: int
  initial_instances:
    description:
      - Initial Instances when the service is enabled.
    type: int
  initial_spot_instances:
    description:
      - Initial spot Instances when the service is enabled.
    type: int
  root_vol_size:
    description:
      - EBS volume size in GB.
    type: int
    default: 100
    aliases:
      - root_volume_size
  public_load_balancer:
    description:
      - Creates a CDE endpoint (Load Balancer) in a publicly accessible subnet. If set false, the endpoint will be 
      created in a private subnet and you will need to setup access to the endpoint manually in your cloud account.
    type: bool
    default: False
  enable_workload_analytics:
    description:
      - If set false, diagnostic information about job and query execution is sent to Cloudera Workload Manager. 
      Anonymization can be configured under Environments / Shared Resources / Telemetry. Refer documentation for more 
      info at https://docs.cloudera.com/workload-manager/cloud/index.html.
    type: bool
    default: False
  use_ssd:
    description:
      - Instance local storage (SSD) would be used for the workload filesystem (Example - spark local directory). 
      In case the workload requires more space than what's available in the instance storage, please use an instance 
      type with sufficient instance local storage or choose an instance type without SSD and configure the EBS volume size. 
      Currently supported only for aws services.
    type: bool
  chart_value_overrides:
    description:
      - Chart overrides for enabling a CDE service.
    type: list
    elements: dict
    suboptions:
      chartName:
        description:
          - Name of the chart that has to be overridden, for eg- "dex-app", "dex-base".
        type: str
      overrides:
        description: 
          - Space separated key-value pairs for overriding chart values. The key and the value must be separated using a 
          colon(:) For eg- "airflow.enabled:true safari.enabled:true".
        type: str
  authorized_ips:
    description:
      - List of CIDRs that would be allowed to access Kubernetes master API server.
    type: list
    elements: str
    aliases:
      - whitelist_ips
  tags:
    description:
      - User defined key-value tags associated with the CDE service and its resources.
    type: dict      
  validation_check:
    description:
      - Skip Validation check.
    type: bool
    default: False
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

# Enable a Data Engineering service
- cloudera.cloud.de_service:
    name: my-service
    env: an-aws-environment-name-or-crn
    instance_type: example-instance-m5.xlarge
    minimum_instances: 1
    maximum_instances: 10
    minimum_spot_instances: 1
    maximum_spot_instances: 10
    use_ssd: False
    authorized_ips: ['CIDR1','CIDR2']

# Disable a Data Engineering Service
- cloudera.cloud.de_service:
    state: absent
    cluster_id: my-cluster-id
'''

RETURN = r'''
---
service:
  description: Details for the Data Engineering Service.
  type: dict
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


class DeService(CdpModule):
    def __init__(self, module):
        super(DeService, self).__init__(module)

        # Set variables
        self.name = self._get_param('name')
        self.cluster_id = self._get_param('cluster_id')
        self.env = self._get_param('env')
        self.instance_type = self._get_param('instance_type')
        self.minimum_instances = self._get_param('minimum_instances')
        self.maximum_instances = self._get_param('maximum_instances')
        self.minimum_spot_instances = self._get_param('minimum_spot_instances')
        self.maximum_spot_instances = self._get_param('maximum_spot_instances')
        self.initial_instances = self._get_param('initial_instances')
        self.initial_spot_instances = self._get_param('initial_spot_instances')
        self.root_vol_size = self._get_param('root_vol_size')
        self.public_load_balancer = self._get_param('public_load_balancer')
        self.enable_workload_analytics = self._get_param('enable_workload_analytics')
        self.use_ssd = self._get_param('use_ssd')
        self.chart_value_overrides = self._get_param('chart_value_overrides')
        self.authorized_ips = self._get_param('authorized_ips')
        self.tags = self._get_param('tags')
        self.validation_check = self._get_param('validation_check')
        self.state = self._get_param('state')
        self.force = self._get_param('force')
        self.wait = self._get_param('wait')
        self.delay = self._get_param('delay')
        self.timeout = self. _get_param('timeout')

        # Initialize return values
        self.service = dict()
        self.changed = False

        # Initialize internal values
        self.target = None

        # Execute logic process
        self.process()

    @CdpModule._Decorators.process_debug
    def process(self):
        env_crn = self.cdpy.environments.resolve_environment_crn(self.env)

        # Check if Service exists
        if self.cluster_id is not None:
            existing = self.cdpy.de.describe_service(self.cluster_id)
        else:
            existing, self.cluster_id = self._describe_service_name()

        if self.state in ['present']:
            # Begin Exists
            if existing is not None:
                self.module.warn("Service is already present and reconciliation is not yet implemented.")
                if self.wait:
                    existing = self.cdpy.sdk.wait_for_state(
                        describe_func=self.cdpy.de.describe_service,
                        params=dict(cluster_id=self.cluster_id),
                        state=self.cdpy.sdk.STARTED_STATES + self.cdpy.sdk.STOPPED_STATES, delay=self.delay,
                        timeout=self.timeout
                    )
                self.service = existing
            else:
                if not self.module.check_mode:
                    # Begin Cluster Creation
                    if env_crn is None:
                        # Fail if wrong environment CRN
                        self.module.fail_json(msg="Could not retrieve CRN for CDP Environment %s" % self.env)
                    else:
                        # Enable Service
                        _ = self.cdpy.de.enable_service(
                            name=self.name, env_crn=env_crn, instance_type=self.instance_type,
                            minimum_instances=self.minimum_instances, maximum_instances=self.maximum_instances,
                            minimum_spot_instances=self.minimum_spot_instances,
                            maximum_spot_instances=self.maximum_spot_instances,
                            initial_instances=self.initial_instances,
                            initial_spot_instances=self.initial_spot_instances,
                            root_volume_size=self.root_vol_size, public_load_balancer=self.public_load_balancer,
                            enable_workload_analytics=self.enable_workload_analytics, use_ssd=self.use_ssd,
                            chart_value_overrides=self.chart_value_overrides, authorized_ips=self.authorized_ips,
                            tags=self.tags, validation_check=self.validation_check
                        )
                        self.changed = True
                        self.cluster_id = _['clusterId']
                        if self.wait:
                            self.service = self.cdpy.sdk.wait_for_state(
                                describe_func=self.cdpy.de.describe_service,
                                params=dict(clusterId=self.cluster_id),
                                state='ClusterCreationCompleted', delay=self.delay, timeout=self.timeout
                            )
                        else:
                            self.service = self.cdpy.de.describe_service(cluster_id=self.cluster_id)
                    # End Cluster Creation
        elif self.state in ['absent']:
            if existing is not None:
                # Begin Delete
                if self.module.check_mode:
                    self.service = self.target
                else:
                    # Fail if not in Removable States
                    if self.target['status'] not in self.cdpy.sdk.REMOVABLE_STATES:
                        self.module.warn(
                            "Service state is not valid for Delete operations: %s" % self.target['status'])
                    else:
                        # Delete Service
                        _ = self.cdpy.de.delete_service(cluster_id=self.cluster_id, force=self.force)
                        self.changed = True
                    if self.wait:
                        self.cdpy.sdk.wait_for_state(
                            describe_func=self.cdpy.dw.describe_service,
                            params=dict(cluster_id=self.cluster_id),
                            field=None, delay=self.delay, timeout=self.timeout
                        )
                    else:
                        self.cdpy.sdk.sleep(self.delay)
                        self.service = self.cdpy.de.describe_service(cluster_id=self.cluster_id)
                # End Delete
            else:
                # Warn if attempting to delete non-existent service
                self.module.warn("Service %s doesn't exist" % self.name)
        else:
            # Invalid State
            self.module.fail_json(msg='Invalid state: %s' % self.state)

        # if self.target is not None:
        #     # Begin Cluster exists
        #     if self.state == 'absent':
        #         # Begin Delete
        #         if self.module.check_mode:
        #             self.service = self.target
        #         else:
        #             self.changed = True
        #             if self.target['status'] not in self.cdpy.sdk.REMOVABLE_STATES:
        #                 self.module.warn("Cluster is not in a valid state for Delete operations: %s" % self.target['status'])
        #             else:
        #                 _ = self.cdpy.de.delete_service(cluster_id=self.cluster_id, force=self.force)
        #
        #             if self.wait:
        #                 self.cdpy.sdk.wait_for_state(
        #                     describe_func=self.cdpy.dw.describe_service,
        #                     params=dict(cluster_id=self.cluster_id),
        #                     field=None, delay=self.delay, timeout=self.timeout
        #                 )
        #             else:
        #                 self.cdpy.sdk.sleep(self.delay)
        #                 self.service = self.cdpy.de.describe_service(cluster_id=self.cluster_id)
        #         # End Delete
        #     elif self.state == 'present':
        #         # Begin Wait
        #         self.module.warn("Service is already present and reconciliation is not yet implemented.")
        #         if self.wait:
        #             self.target = self.cdpy.sdk.wait_for_state(
        #                 describe_func=self.cdpy.de.describe_service,
        #                 params=dict(cluster_id=self.cluster_id),
        #                 state=self.cdpy.sdk.STARTED_STATES + self.cdpy.sdk.STOPPED_STATES, delay=self.delay,
        #                 timeout=self.timeout
        #             )
        #         self.service = self.target
        #         # End Wait
        #     else:
        #         self.module.fail_json(msg="State %s is not valid for this module" % self.state)
        #     # End Cluster Exists
        # else:
        #     # Begin Cluster Not Found
        #     if self.state == 'absent':
        #         self.module.warn("Cluster %s already absent in Environment %s" % (self.name, self.env))
        #     elif self.state == 'present':
        #         if not self.module.check_mode:
        #             # Begin Cluster Creation
        #             self.changed = True
        #             if env_crn is None:
        #                 self.module.fail_json(msg="Could not retrieve CRN for CDP Environment %s" % self.env)
        #             else:
        #                 _ = self.cdpy.de.enable_service(
        #                     name=self.name, env_crn=env_crn, instance_type=self.instance_type,
        #                     minimum_instances=self.minimum_instances, maximum_instances=self.maximum_instances,
        #                     minimum_spot_instances=self.minimum_spot_instances,
        #                     maximum_spot_instances=self.maximum_spot_instances,
        #                     initial_instances=self.initial_instances, initial_spot_instances=self.initial_spot_instances,
        #                     root_volume_size=self.root_vol_size, public_load_balancer=self.public_load_balancer,
        #                     enable_workload_analytics=self.enable_workload_analytics, use_ssd=self.use_ssd,
        #                     chart_value_overrides=self.chart_value_overrides, authorized_ips=self.authorized_ips,
        #                     tags=self.tags, validation_check=self.validation_check
        #                 )
        #                 self.cluster_id = _['clusterId']
        #                 if self.wait:
        #                     self.service = self.cdpy.sdk.wait_for_state(
        #                         describe_func=self.cdpy.de.describe_service,
        #                         params=dict(clusterId=self.cluster_id),
        #                         state='ClusterCreationCompleted', delay=self.delay, timeout=self.timeout
        #                     )
        #                 else:
        #                     self.service = self.cdpy.de.describe_service(cluster_id=self.cluster_id)
        #             # End Cluster Creation
        #     else:
        #         self.module.fail_json(msg="Invalid state: %s" % self.state)
        #     # End Cluster Not Found

    def _get_cluster_id(self):
        result = self.cdpy.de.list_services(self.env, remove_deleted=True)
        cluster_id = [x['clusterId'] for x in result if x['name'] == self.name
                      and x['status'] in (self.cdpy.sdk.STARTED_STATES + self.cdpy.sdk.CREATION_STATES)][0]
        return cluster_id

    def _describe_service_name(self):
        result = self.cdpy.de.list_services(self.env, remove_deleted=True)
        cluster_id = [x['clusterId'] for x in result if x['name'] == self.name
                      and x['status'] in (self.cdpy.sdk.STARTED_STATES + self.cdpy.sdk.CREATION_STATES)][0]
        return self.cdpy.de.describe_service(cluster_id), cluster_id


def main():
    module = AnsibleModule(
        argument_spec=CdpModule.argument_spec(
            name=dict(type='str'),
            cluster_id=dict(type='str'),
            env=dict(type='str', aliases=['environment', 'env_crn']),
            instance_type=dict(type='str'),
            minimum_instances=dict(type='int'),
            maximum_instances=dict(type='int'),
            minimum_spot_instances=dict(type='int'),
            maximum_spot_instances=dict(type='int'),
            initial_instances=dict(type='int'),
            initial_spot_instances=dict(type='int'),
            root_vol_size=dict(type='int', default=100, aliases=['root_volume_size']),
            public_load_balancer=dict(type='bool', default=False),
            enable_workload_analytics=dict(type='bool', default=False),
            use_ssd=dict(type='bool'),
            chart_value_overrides=dict(type='dict'),
            authorized_ips=dict(type='list', aliases=['whitelist_ips']),
            tags=dict(type='dict'),
            validation_check=dict(type='bool', default=False),
            state=dict(type='str', choices=['present', 'absent'], default='present'),
            force=dict(type='bool', default=False),
            wait=dict(type='bool', default=True),
            delay=dict(type='int', aliases=['polling_delay'], default=15),
            timeout=dict(type='int', aliases=['polling_timeout'], default=3600)
        ),
        supports_check_mode=True
    )

    result = DeService(module)
    output = dict(changed=result.changed, cluster=result.service)

    if result.debug:
        output.update(sdk_out=result.log_out, sdk_out_lines=result.log_lines)

    module.exit_json(**output)


if __name__ == '__main__':
    main()
