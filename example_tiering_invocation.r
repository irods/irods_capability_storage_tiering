{
   "rule-engine-instance-name": "irods_rule_engine_plugin-storage_tiering-instance",
   "rule-engine-operation": "irods_policy_schedule_storage_tiering",
   "delay-parameters": "<INST_NAME>irods_rule_engine_plugin-storage_tiering-instance</INST_NAME><PLUSET>1s</PLUSET><EF>1h DOUBLE UNTIL SUCCESS OR 6 TIMES</EF>",
   "storage-tier-groups": [
       "example_group_g2",
       "example_group"
   ]
}
INPUT null
OUTPUT ruleExecOut
