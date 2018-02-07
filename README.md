# **iRODS Storage Tiering Rule Engine Plugin**

The storage tiering framework provides iRODS the capability of automatically moving data between identified tiers of storage withing a configured tiering group.

To define a storage tiering group, selected storage resources are labeled with metadata which define their place in the group and how long data should reside in that tier before being migrated to the next tier.

# **Rulebase Configuration**

To configure the storage tiering capability the rule engine plugin must be added as a as a new json object within the "rule_engines" array of the ```server_config.json``` file.  The ```plugin_name``` must be **irods_rule_engine_plugin-tiered_storage** and each instance name of the plugin must be unique.  By convention the plugin name plus the word **instance** is used.
```
"rule_engines": [
    {
         "instance_name": "irods_rule_engine_plugin-tiered_storage-instance",
         "plugin_name": "irods_rule_engine_plugin-tiered_storage",
         "plugin_specific_configuration": {
         }
    },
    {    
        "instance_name": "irods_rule_engine_plugin-irods_rule_language-instance",
        "plugin_name": "irods_rule_engine_plugin-irods_rule_language",
        "plugin_specific_configuration": {  
            <snip>
        },
        "shared_memory_instance": "irods_rule_language_rule_engine"
    },
]
```
# **Configuring Metadata Attributes**
A number of metadata attributes are used within the storage tiering capability which identify the tier group, the time of data may be at rest within the tier, the optional query and so on.  These attributes may already map to concepts within a given iRODS installation.  For that reason we have exposed them as configuration options within the storage tiering plugin specific configuration block.  For a default installation the following values are used.

```
"plugin_specific_configuration": {
    "access_time_attribute" : "irods::access_time",
    "storage_tiering_group_attribute" : "irods::storage_tier_group",
    "storage_tiering_time_attribute" : "irods::storage_tier_time",
    "storage_tiering_query_attribute" : "irods::storage_tier_query",
    "storage_tiering_verification_attribute" : "irods::storage_tier_verification",
    "storage_tiering_restage_delay_attribute" : "irods::storage_tier_restage_delay",
    "default_restage_delay_parameters" : "<PLUSET>1s</PLUSET><EF>1h DOUBLE UNTIL SUCCESS OR 6 TIMES</EF>",
    "time_check_string" : "TIME_CHECK_STRING"
}
```
# **Creating a Tier Group**
 
Tier groups are defined via metadata AVUs attached to the resources which participate in the group.

In iRODS terminology, the `attribute` is defined by a function in the rule base **storage_tiering_configuration.re**, which by default is named **irods::storage_tier_group**.  The `value` of the metadata triple is the name of the tier group, and the `unit` holds the numeric position of the resource within the group.  To define a tier group, simply choose a name and apply metadata to the selected root resources of given compositions.

For example:
```
imeta add -R fast_resc irods::storage_tier_group example_group 0
imeta add -R medium_resc irods::storage_tier_group example_group 1
imeta add -R slow_resc irods::storage_tier_group example_group 2 
```

This example defines three tiers of the group `example_group` where data will flow from tier 0 to tier 2 as it ages.  In this example `fast_resc` is a single resource, but it could have been set to `fast_tier_root_resc` and represent the root of a resource hierarchy consisting of many resources.


# **Setting Tiering Policy**

Once a tier group is defined, the age limit for each tier must also be configured via metadata.  Once a data object has remained unaccessed on a given tier for more than the configured time, it will be staged to the next tier in the group and then trimmed from the previous tier.  This is configured via the default attribute **irods::storage_tier_time** (which itself is defined in the **storage_tiering_configuration.re** rulebase).  In order to configure the tiering time, apply an AVU to the resource using the given attribute and a positive numeric value in seconds.

For example, to configure the `fast_resc` to hold data for only 30 minutes:
```
imeta add -R fast_resc irods::storage_tier_time 1800
```
We can then configure the `medium_resc` to hold data for 8 hours:
```
imeta add -R medium_resc irods::storage_tier_time 28800
```

# **Configuring Tiering Verification**

When a violating data object is identified for a given source resource, the object is replicated to the next resource in the tier.  In order to determine that this operation has succeeded before the source replica is trimmed, the storage tiering plugin provides the ability to perform three methods of verification of the destination relica.

In order of escalating computational complexity, first the system may just rely on the fact that no errors were caught and that the entry is properly registered in the catalog.  This is the default behavior, but would also be confiugred as such:
```
imeta add -R fast_resc irods::storage_tier_verification catalog
```

A more expensive but reliable method is to check the file size of the replica on the destination resource:
```
imeta add -R fast_resc irods::storage_tier_verification filesystem
```

And finally, the most computationally intensive but thorough method of verification is computing and comparing checksums.  Keep in mind that if no checksum is available for the source replica, such as no checksum was computed on ingest, the plugin will compute one for the source replica first.
```
imeta add -R fast_resc irods::storage_tier_verification checksum
```

# **Customizing the Violating Objects Query**

A tier within a tier group may identify data objects which are in violation by an alternate mechanism beyond the built-in time-based constraint.  This allows the data grid administrator to take additional context into account when identifying data objects to migrate.

Data objects which have been labeled via particular metadata, or within a specific collection, owned by a particular user, or belonging to a particular project may be identified through a custom query.  The default attribute **irods::storage_tier_query** is used to hold this custom query.  To configure the custom query, attach the query to the root resource of the tier within the tier group.  This query will be used in place of the default time-based query for that tier.  For efficiency this example query checks for the existence in the root resource's list of leaves by resource ID.

```
imeta set -R rnd1 irods::storage_tier_query "SELECT DATA_NAME, COLL_NAME WHERE META_DATA_ATTR_NAME = 'irods::access_time' AND META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING' AND DATA_RESC_ID IN ('10068', '10069')"
```

The example above implements the default query.  Note that the string `TIME_CHECK_STRING` is used in place of an actual time.  This string will be replaced by the storage tiering framework with the appropriately computed time given the previous parameters.
