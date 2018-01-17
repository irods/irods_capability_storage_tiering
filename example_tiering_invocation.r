{
   "rule-engine-instance-name": "irods_rule_engine_plugin-tiered_storage-instance",
   "rule-engine-operation": "apply_storage_tiering_policy",
   "delay-parameters": "<PLUSET>1s</PLUSET><EF>1h DOUBLE UNTIL SUCCESS OR 6 TIMES</EF>",
   "storage-tier-groups": [
       "example_group_g2",
       "example_group"
   ]
}
INPUT null
OUTPUT ruleExecOut
