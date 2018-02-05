
#include "irods_server_properties.hpp"
#include "irods_re_plugin.hpp"
#include "irods_storage_tiering.hpp"
#include "irods_query.hpp"
#include "irods_object_migrator.hpp"
#include "irods_virtual_path.hpp"
#include "irods_hierarchy_parser.hpp"
#include "irods_resource_manager.hpp"

#include "rsModAVUMetadata.hpp"

#include <boost/any.hpp>
#include <boost/regex.hpp>
#include <boost/exception/all.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>

#include "json.hpp"


extern irods::resource_manager resc_mgr;

int _delayExec(
    const char *inActionCall,
    const char *recoveryActionCall,
    const char *delayCondition,
    ruleExecInfo_t *rei );



namespace irods {

    storage_tiering_configuration::storage_tiering_configuration(
        const std::string& _instance_name ) {
        instance_name = _instance_name;
        bool success_flag = false;

        try {
            const auto& rule_engines = get_server_property<
                const std::vector<boost::any>&>(
                        std::vector<std::string>{
                        CFG_PLUGIN_CONFIGURATION_KW,
                        PLUGIN_TYPE_RULE_ENGINE});
            for ( const auto& elem : rule_engines ) {
                const auto& rule_engine = boost::any_cast< const std::unordered_map< std::string, boost::any >& >( elem );
                const auto& inst_name   = boost::any_cast< const std::string& >( rule_engine.at( CFG_INSTANCE_NAME_KW ) );
                if ( inst_name == _instance_name ) {
                    if(rule_engine.count(CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW) > 0) {
                        const auto& plugin_spec_cfg = boost::any_cast<const std::unordered_map<std::string, boost::any>&>(
                                rule_engine.at(CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW));
                        if(plugin_spec_cfg.find("access_time_attribute") != plugin_spec_cfg.end()) {
                            access_time_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("access_time_attribute"));
                        }

                        if(plugin_spec_cfg.find("storage_tiering_group_attribute") != plugin_spec_cfg.end()) {
                            storage_tiering_group_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("storage_tiering_group_attribute"));
                        }

                        if(plugin_spec_cfg.find("storage_tiering_time_attribute") != plugin_spec_cfg.end()) {
                            storage_tiering_time_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("storage_tiering_time_attribute"));
                        }

                        if(plugin_spec_cfg.find("storage_tiering_query_attribute") != plugin_spec_cfg.end()) {
                            storage_tiering_query_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("storage_tiering_query_attribute"));
                        }

                        if(plugin_spec_cfg.find("storage_tiering_verification_attribute") != plugin_spec_cfg.end()) {
                            storage_tiering_verification_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("storage_tiering_verification_attribute"));
                        }

                        if(plugin_spec_cfg.find("storage_tiering_restage_delay_attribute") != plugin_spec_cfg.end()) {
                            storage_tiering_restage_delay_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("storage_tiering_restage_delay_attribute"));
                        }

                        if(plugin_spec_cfg.find("default_restage_delay_parameters") != plugin_spec_cfg.end()) {
                            default_restage_delay_parameters = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("default_restage_delay_parameters"));
                        }

                        if(plugin_spec_cfg.find("time_check_string") != plugin_spec_cfg.end()) {
                            time_check_string = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("time_check_string"));
                        }
                    }
                    success_flag = true;
                } // if inst_name
            } // for rule_engines
        } catch ( const boost::bad_any_cast& e ) {
            THROW( INVALID_ANY_CAST, e.what() );
        } catch ( const std::out_of_range& e ) {
            THROW( KEY_NOT_FOUND, e.what() );
        }

        if(!success_flag) {
            THROW(
                SYS_INVALID_INPUT_PARAM,
                boost::format("failed to find configuration for storage_tiering plugin [%s]") %
                _instance_name);
        }
    } // ctor storage_tiering_configuration

    storage_tiering::storage_tiering(
            rsComm_t*          _comm,
            const std::string& _instance_name) :
        comm_(_comm),
        config_(_instance_name) {

    }

    // =-=-=-=-=-=-=-
    // Helper Functions
    void storage_tiering::update_access_time_for_data_object(
        const std::string& _logical_path) {

        auto ts = std::to_string(std::time(nullptr));
        modAVUMetadataInp_t avuOp{
            .arg0 = "set",
            .arg1 = "-d",
            .arg2 = const_cast<char*>(_logical_path.c_str()),
            .arg3 = const_cast<char*>(config_.access_time_attribute.c_str()),
            .arg4 = const_cast<char*>(ts.c_str()),
            .arg5 = ""};

        auto status = rsModAVUMetadata(comm_, &avuOp);
        if(status < 0) {
            THROW(
                status,
                boost::format("failed to set access time for [%s]") %
                _logical_path);
        }
    } // update_access_time_for_data_object

    std::string storage_tiering::get_metadata_for_resource(
        const std::string& _meta_attr_name,
        const std::string& _resource_name ) {
        std::string query_str {
            boost::str(
                    boost::format("SELECT META_RESC_ATTR_VALUE WHERE META_RESC_ATTR_NAME = '%s' and RESC_NAME = '%s'") %
                    _meta_attr_name %
                    _resource_name) };
        query qobj{comm_, query_str, 1};
        if(qobj.size() > 0) {
            return qobj.front()[0];
        }

        THROW(
            CAT_NO_ROWS_FOUND,
            boost::format("no results found for resc [%s] with attribute [%s]") %
            _resource_name % 
            _meta_attr_name);
    } // get_metadata_for_resource

    std::map<std::string, std::string> storage_tiering::get_tier_group_resources_and_indices(
        const std::string& _group_name) {
        std::map<std::string, std::string> resc_map;
        std::string query_str{
            boost::str(
                    boost::format(
                        "SELECT RESC_NAME, META_RESC_ATTR_UNITS WHERE META_RESC_ATTR_NAME = '%s' and META_RESC_ATTR_VALUE = '%s'") %
                    config_.storage_tiering_group_attribute %
                    _group_name) };
        for(auto row : query{comm_, query_str}) {
            std::string& resc_name = row[0];
            std::string& tier_idx  = row[1];

            try {
                boost::lexical_cast<int>(tier_idx);
            }
            catch(const boost::bad_lexical_cast& _e) {
                rodsLog(
                        LOG_ERROR,
                        "invalid tier index [%s] in group [%s]",
                        tier_idx.c_str(),
                        _group_name.c_str());
            }

            if(resc_map.find(tier_idx) != resc_map.end()) {
                rodsLog(
                        LOG_ERROR,
                        "multiple tiers defined for index [%s] in group [%s]",
                        tier_idx.c_str(),
                        _group_name.c_str());
            }

            resc_map[tier_idx] = resc_name;
        } // for row

        return resc_map;
    } // get_tier_group_resources_and_indices

    std::string storage_tiering::get_leaf_resources_string(
            const std::string& _resource_name) {
        std::string leaf_id_str;

        // if the resource has no children then simply return
        resource_ptr root_resc;
        error err = resc_mgr.resolve(_resource_name, root_resc);
        if(!err.ok()) {
            THROW(err.code(), err.result());
        }

        std::vector<resource_manager::leaf_bundle_t> leaf_bundles = 
            resc_mgr.gather_leaf_bundles_for_resc(_resource_name);
        for(const auto & bundle : leaf_bundles) {
            for(const auto & leaf_id : bundle) {
                leaf_id_str += 
                    "'" + boost::str(boost::format("%s") % leaf_id) + "',";
            } // for
        } // for

        // if there is no hierarchy
        if(leaf_id_str.empty()) {
            rodsLong_t resc_id;
            resc_mgr.hier_to_leaf_id(_resource_name, resc_id);
            leaf_id_str =
                "'" + boost::str(boost::format("%s") % resc_id) + "',";
        }

        return leaf_id_str;

    } // get_leaf_resources_string

    std::string storage_tiering::get_verification_for_resc(
            const std::string& _resource_name) {

        try {
            std::string ver = get_metadata_for_resource(
                                  config_.storage_tiering_verification_attribute,
                                  _resource_name);
            return ver;
        }
        catch(const exception& _e) {
            return "catalog"; 
        }

    } // get_verification_for_resc

    std::string storage_tiering::get_restage_delay_param_for_resc(
            const std::string& _resource_name) {

        try {
            std::string ver = get_metadata_for_resource(
                                  config_.storage_tiering_restage_delay_attribute,
                                  _resource_name);
            return ver;
        }
        catch(const exception& _e) {
            return config_.default_restage_delay_parameters;
        }

    } // get_restage_delay_param_for_resc

    std::map<std::string, std::string> storage_tiering::get_resource_map_for_group(
        const std::string& _group) {
        std::map<std::string, std::string> groups;
        try {
            std::string query_str{
                boost::str(
                        boost::format("SELECT META_RESC_ATTR_UNITS, RESC_NAME WHERE META_RESC_ATTR_VALUE = '%s' AND META_RESC_ATTR_NAME = '%s'") %
                        _group %
                        config_.storage_tiering_group_attribute)}; 
            for(const auto& g : query{comm_, query_str}) {
                groups[g[0]] = g[1];
            }

            return groups;
        }
        catch(const std::exception&) {
            return groups;
        }

    } // get_resource_map_for_group

    std::string storage_tiering::get_tier_time_for_resc(
        const std::string& _resource_name) {
        try {
            std::time_t now = std::time(nullptr);
            std::string offset_str = get_metadata_for_resource(
                                         config_.storage_tiering_time_attribute,
                                         _resource_name);
            std::time_t offset = boost::lexical_cast<std::time_t>(offset_str);
            return std::to_string(now - offset);
        }
        catch(const boost::bad_lexical_cast& _e) {
            THROW(
                INVALID_LEXICAL_CAST,
                _e.what());
        }
        catch(const exception&) {
            THROW(
                SYS_INVALID_INPUT_PARAM,
                boost::format("failed to get tiering time for resource [%s] : set attribute [%s] with value (tier time) in seconds") %
                _resource_name %
                config_.storage_tiering_time_attribute);
        }

    } // get_tier_time_for_resc

    std::string storage_tiering::get_violating_query_string_for_resource(
        const std::string& _resource_name) {
        const auto tier_time = get_tier_time_for_resc(_resource_name);
        try {
            std::string tmp_query_str = get_metadata_for_resource(
                                            config_.storage_tiering_query_attribute,
                                            _resource_name);
            size_t start_pos = tmp_query_str.find(config_.time_check_string);
            if(start_pos != std::string::npos) {
                tmp_query_str.replace(
                    start_pos,
                    config_.time_check_string.length(),
                    tier_time);
            }
            return tmp_query_str;
        }
        catch(const exception&) {
            const auto leaf_str = get_leaf_resources_string(_resource_name);
            return boost::str(
                       boost::format("SELECT DATA_NAME, COLL_NAME WHERE META_DATA_ATTR_NAME = '%s' AND META_DATA_ATTR_VALUE < '%s' AND DATA_RESC_ID IN (%s)") %
                       config_.access_time_attribute %
                       tier_time %
                       leaf_str);
        }
    } // get_violating_query_string_for_resource

    void storage_tiering::migrate_violating_objects_for_resource(
        const std::string& _source_resource,
        const std::string& _destination_resource) {
        try {
            const auto query_str = get_violating_query_string_for_resource(_source_resource);
            query qobj{comm_, query_str, 1};
            if(qobj.size() > 0) {
                const auto result = qobj.front();
                auto object_path = result[1];
                const auto& vps = get_virtual_path_separator();
                if( !boost::ends_with(object_path, vps)) {
                    object_path += vps;
                }
                object_path += result[0];

                const std::string verification_type{get_verification_for_resc(_destination_resource)};
                object_migrator mover(
                    comm_,
                    verification_type,
                    _source_resource,
                    _destination_resource,
                    object_path);
                mover();
            }
        }
        catch(const exception& _e) {
            // if nothing of interest is found, thats not an error
            if(CAT_NO_ROWS_FOUND == _e.code()) {
                return;
            }

            throw;
        }
        catch(const std::out_of_range& _e) {
            THROW(
                SYS_INVALID_INPUT_PARAM,
                boost::format("out of bounds index for storage tiering query on resource [%s] : both DATA_NAME, COLL_NAME are required") %
                _source_resource);
        }
    } // migrate_violating_objects_for_resource

    void storage_tiering::queue_restage_operation(
        ruleExecInfo_t*    _rei,
        const std::string& _restage_delay_params,
        const std::string& _verification_type,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const std::string& _object_path) {
        using json = nlohmann::json;

        json rule_obj;
        rule_obj["rule-engine-operation"] = "migrate_object_to_resource";
        rule_obj["verification-type"]     = _verification_type;
        rule_obj["source-resource"]       = _source_resource;
        rule_obj["destination-resource"]  = _destination_resource;
        rule_obj["object-path"]           = _object_path;

        const auto delay_err = _delayExec(
                                   rule_obj.dump().c_str(),
                                   "",
                                   _restage_delay_params.c_str(), 
                                   _rei);
        if(delay_err < 0) {
            THROW(
                delay_err,
                boost::format("restage failed for object [%s] from [%s] to [%s]") %
                _object_path %
                _source_resource %
                _destination_resource);
        }
    } // queue_restage_operation

   // =-=-=-=-=-=-=-
   // Application Functions
    void storage_tiering::apply_access_time(
            std::list<boost::any>& _args) {
        try {
            // NOTE:: 3rd parameter is the dataObjInp_t
            auto it = _args.begin();
            std::advance(it, 2);
            if(_args.end() == it) {
                THROW(SYS_INVALID_INPUT_PARAM, "invalid number of arguments");
            }

            auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
            update_access_time_for_data_object(obj_inp->objPath);
        }
        catch(const boost::bad_any_cast& _e) {
            THROW( INVALID_ANY_CAST, _e.what() );
        }
    } // apply_access_time

    void storage_tiering::restage_object_to_lowest_tier(
        ruleExecInfo_t*        _rei,
        std::list<boost::any>& _args) {
        try {
            // NOTE:: 3rd parameter is the dataObjInp_t
            auto it = _args.begin();
            std::advance(it, 2);
            if(_args.end() == it) {
                THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
            }

            auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
            const char* object_path = obj_inp->objPath;
            const char* source_resource_hier = getValByKey(
                                                   &obj_inp->condInput,
                                                   RESC_HIER_STR_KW);

            hierarchy_parser h_parse;
            h_parse.set_string(source_resource_hier);
            std::string source_resource;
            h_parse.first_resc(source_resource);

            const auto group_name = get_metadata_for_resource(
                                        config_.storage_tiering_group_attribute,
                                        source_resource);
            const auto idx_resc_map = get_tier_group_resources_and_indices(
                                          group_name);
            const auto& low_tier_resource_name = idx_resc_map.begin()->second;
            const auto  verification_type = get_verification_for_resc(
                                                  low_tier_resource_name);
            const auto restage_delay_params = get_restage_delay_param_for_resc(
                                                  source_resource);
            queue_restage_operation(
                    _rei,
                    restage_delay_params,
                    verification_type,
                    source_resource,
                    low_tier_resource_name,
                    object_path);
        }
        catch(const boost::bad_any_cast& _e) {
            THROW(INVALID_ANY_CAST, _e.what());
        }
    } // restage_object_to_lowest_tier

    void storage_tiering::apply_storage_tiering_policy(
        const std::string& _group) {
        const std::map<std::string, std::string> rescs = get_resource_map_for_group(
                                                             _group);
        if(rescs.empty()) {
            rodsLog(
                LOG_ERROR,
                "%s :: no resources found for group [%s]",
                __FUNCTION__,
                _group.c_str());
            return;
        }

        auto resc_itr = rescs.begin();
        for( ; resc_itr != rescs.end(); ++resc_itr) {
            auto next_itr = resc_itr;

            ++next_itr;
            if(rescs.end() == next_itr) {
                break;
            }

            migrate_violating_objects_for_resource(resc_itr->second, next_itr->second);
        } // for resc

    } // apply_storage_tiering_policy

    void storage_tiering::move_data_object(
            const std::string& _verification_type,
            const std::string& _source_resource,
            const std::string& _destination_resource,
            const std::string& _object_path) {
        irods::object_migrator mover(
            comm_,
            _verification_type,
            _source_resource,
            _destination_resource,
            _object_path);
        mover();

    } // move_data_object

}; // namespace irods
