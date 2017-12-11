
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
                }
            }
        } catch ( const boost::bad_any_cast& e ) {
            THROW( INVALID_ANY_CAST, e.what() );
        } catch ( const std::out_of_range& e ) {
            THROW( KEY_NOT_FOUND, e.what() );
        }

        std::stringstream msg;
        msg << "failed to find configuration for storage_tiering plugin ["
            << _instance_name << "]";
        rodsLog( LOG_ERROR, "%s", msg.str().c_str() );
        
        THROW( SYS_INVALID_INPUT_PARAM, msg.str() );

    } // ctor storage_tiering_configuration

    storage_tiering::storage_tiering(
            rsComm_t*          _comm,
            const std::string& _instance_name) :
        comm_(_comm),
        config(_instance_name) {

    }

    // =-=-=-=-=-=-=-
    // Helper Functions
    void storage_tiering::update_access_time_for_data_object(
            const std::string& _logical_path) {

        std::string ts = std::to_string(std::time(nullptr)); 

        modAVUMetadataInp_t avuOp;
        memset(&avuOp, 0, sizeof(avuOp));
        avuOp.arg0 = "set";
        avuOp.arg1 = "-d";
        avuOp.arg2 = (char*)_logical_path.c_str();
        avuOp.arg3 = (char*)config_.access_time_attribute.c_str();
        avuOp.arg4 = (char*)ts.c_str();
        avuOp.arg5 = "";

        int status = rsModAVUMetadata(comm_, &avuOp);
        if(status < 0) {
            std::string msg{"failed to set access time for ["};
            msg += _logical_path + "]";
            THROW(status, msg);
        }
    } // update_access_time_for_data_object

    void storage_tiering::get_object_and_collection_from_path(
            const std::string& _obj_path,
            std::string&       _coll,
            std::string&       _obj ) {
        namespace bfs = boost::filesystem;

        try {
            bfs::path p(_obj_path);
            _coll = p.parent_path().string();
            _obj  = p.filename().string();
        }
        catch(const bfs::filesystem_error& _e) {
            THROW(SYS_INVALID_FILE_PATH, _e.what());
        }
    } // get_object_and_collection_from_path

    std::string storage_tiering::get_metadata_for_resource(
            const std::string& _meta_attr_name,
            const std::string& _resc_name ) {

        try {
            std::string query_str {
                boost::str(
                        boost::format("SELECT META_RESC_ATTR_VALUE WHERE META_RESC_ATTR_NAME = '%s' and RESC_NAME = '%s'") %
                        _meta_attr_name %
                        _resc_name) };
            query query(comm_, query_str, 1);
            if(query.size() > 0) {
                return query.front()[0];
            }

            THROW(
                    CAT_NO_ROWS_FOUND,
                    boost::format("no results found for resc [%s] with attribute [%s]") %
                    _resc_name % 
                    _meta_attr_name);
        }
        catch(const exception& _e) {
            throw;
        }
    } // get_metadata_for_resource

    void storage_tiering::get_tier_group_resources_and_indicies(
            const std::string&                  _group_name,
            std::map<std::string, std::string>& _resc_map ) {

        try {
            std::string query_str{
                boost::str(
                        boost::format(
                            "SELECT RESC_NAME, META_RESC_ATTR_UNITS WHERE META_RESC_ATTR_NAME = '%s' and META_RESC_ATTR_VALUE = '%s'") %
                        config_.storage_tiering_group_attribute %
                        _group_name) };
            query query(comm_, query_str);
            for(auto row : query) {
                std::string& resc_name = row[0];
                std::string& tier_idx  = row[1];

                try {
                    int idx = boost::lexical_cast<int>(tier_idx);
                }
                catch(const boost::bad_lexical_cast& _e) {
                    rodsLog(
                            LOG_ERROR,
                            "invalid tier index [%s] in group [%s]",
                            tier_idx.c_str(),
                            _group_name.c_str());
                }

                if(_resc_map.find(tier_idx) != _resc_map.end()) {
                    rodsLog(
                            LOG_ERROR,
                            "multiple tiers defined for index [%s] in group [%s]",
                            tier_idx.c_str(),
                            _group_name.c_str());
                }

                _resc_map[tier_idx] = resc_name;
            } // for row

        }
        catch(const exception& _e) {
            throw;
        }
    } // get_tier_group_resources_and_indicies

    std::string storage_tiering::get_leaf_resources_string(
            const std::string& _resc_name) {
        std::string leaf_id_str;

        // if the resource has no children then simply return
        resource_ptr root_resc;
        error err = resc_mgr.resolve(_resc_name, root_resc);
        if(!err.ok()) {
            THROW(err.code(), err.result());
        }

        try {
            std::vector<resource_manager::leaf_bundle_t> leaf_bundles = 
                resc_mgr.gather_leaf_bundles_for_resc(_resc_name);
            for(const auto & bundle : leaf_bundles) {
                for(const auto & leaf_id : bundle) {
                    leaf_id_str += 
                        "'" + boost::str(boost::format("%s") % leaf_id) + "',";
                } // for
            } // for
        }
        catch( const exception & _e ) {
            throw;
        }

        // if there is no hierarchy
        if(leaf_id_str.empty()) {
            rodsLong_t resc_id;
            resc_mgr.hier_to_leaf_id(_resc_name, resc_id);
            leaf_id_str =
                "'" + boost::str(boost::format("%s") % resc_id) + "',";
        }

        return leaf_id_str;

    } // get_leaf_resources_string

    std::string storage_tiering::get_verification_for_resc(
            const std::string& _resc_name) {

        try {
            std::string ver = get_metadata_for_resource(
                    comm_,
                    config_.storage_tiering_verification_attribute,
                    _resc_name);
            return ver;
        }
        catch(const exception& _e) {
            return "catalog"; 
        }

    } // get_verification_for_resc

    std::string storage_tiering::get_restage_delay_param_for_resc(
            const std::string& _resc_name) {

        try {
            std::string ver = get_metadata_for_resource(
                    comm_,
                    config_.storage_tiering_restage_delay_attribute,
                    _resc_name);
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
            query query{comm_, query_str};
            for(const auto& g : query) {
                groups[g[0]] = g[1];
            }

            return groups;
        }
        catch(const std::exception&) {
            return groups;
        }

    } // get_resource_map_for_group

    std::string storage_tiering::get_tier_time_for_resc(
            const std::string& _resc_name) {
        try {
            std::time_t now = std::time(nullptr);
            std::string offset_str = get_metadata_for_resource(
                    comm_,
                    config_.storage_tiering_time_attribute,
                    _resc_name);
            std::time_t offset = boost::lexical_cast<std::time_t>(offset_str);
            return std::to_string(now - offset);
        }
        catch(const boost::bad_lexical_cast& _e) {
            THROW(
                    INVALID_LEXICAL_CAST,
                    _e.what());
        }
        catch(const exception&) {
            throw;
        }
    } // get_tier_time_for_resc

    void storage_tiering::tier_objects_for_resc(
            const std::string& _src_resc,
            const std::string& _dst_resc) {
        try {
            const std::string tier_time{get_tier_time_for_resc(comm_, _src_resc)};

            std::string query_str;
            try {
                query_str = get_metadata_for_resource(
                        comm_,
                        config_.storage_tiering_query_attribute,
                        _src_resc);
                size_t start_pos = query_str.find(config_.time_check_string);
                if(start_pos != std::string::npos) {
                    query_str.replace(start_pos, config_.time_check_string.length(), tier_time);
                }
            }
            catch(const exception&) {
                const std::string leaf_str{ get_leaf_resources_string(
                        comm_,
                        _src_resc)};
                query_str =
                    boost::str(
                            boost::format("SELECT DATA_NAME, COLL_NAME WHERE META_DATA_ATTR_NAME = '%s' AND META_DATA_ATTR_VALUE < '%s' AND DATA_RESC_ID IN (%s)") %
                            config_.access_time_attribute %
                            tier_time %
                            leaf_str);
            }

            query query{comm_, query_str, 1};
            if(query.size() > 0) {
                query::value_type result = query.front();

                std::string obj_path = result[1]; 
                const std::string vps = get_virtual_path_separator();
                if( !boost::ends_with(obj_path, vps)) {
                    obj_path += vps;
                }
                obj_path += result[0];

                const std::string verification_type{get_verification_for_resc(comm_,_src_resc)};
                object_migrator mover(
                    comm_,
                    verification_type,
                    _src_resc,
                    _dst_resc,
                    obj_path);
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
    } // tier_objects_for_resc

    void storage_tiering::queue_restage_operation(
        ruleExecInfo_t*    _rei,
        const std::string& _restage_delay_params,
        const std::string& _verification_type,
        const std::string& _src_resc,
        const std::string& _dst_resc,
        const std::string& _obj_path) {
        using json = nlohmann::json;

        json rule_obj;
        rule_obj["rule-engine-operation"] = "migrate_object_to_resource";
        rule_obj["verification-type"] = _verification_type;
        rule_obj["source-resc"]       = _src_resc;
        rule_obj["destination-resc"]  = _dst_resc;
        rule_obj["object-path"]       = _obj_path;

        int delay_err = _delayExec(
                rule_obj.dump().c_str(),
                "",
                _restage_delay_params.c_str(), 
                _rei);

    } // queue_restage_operation

   // =-=-=-=-=-=-=-
   // Application Functions

    void storage_tiering::apply_access_time(
            std::list<boost::any>& _args) {

        try {
            // NOTE:: 3rd parameter is the dataObjInp_t
            std::list<boost::any>::iterator it = _args.begin();
            std::advance(it, 2);
            if(_args.end() == it) {
                THROW(SYS_INVALID_INPUT_PARAM, "invalid number of arguments");
            }

            dataObjInp_t* obj_inp = boost::any_cast<dataObjInp_t*>(*it);
            update_access_time_for_data_object(comm_, obj_inp->objPath);
        }
        catch(const boost::bad_any_cast& _e) {
            THROW( INVALID_ANY_CAST, _e.what() );
        }
        catch(const exception& _e) {
            throw;
        }
    } // apply_access_time

    void storage_tiering::restage_object_to_lowest_tier(
            ruleExecInfo_t*        _rei,
            std::list<boost::any>& _args) {

        try {
            // NOTE:: 3rd parameter is the dataObjInp_t
            std::list<boost::any>::iterator it = _args.begin();
            std::advance(it, 2);
            if(_args.end() == it) {
                THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
            }

            dataObjInp_t* obj_inp = boost::any_cast<dataObjInp_t*>(*it);
            const char* obj_path = obj_inp->objPath;
            const char* src_resc_hier = getValByKey(
                    &obj_inp->condInput,
                    RESC_HIER_STR_KW);

            hierarchy_parser h_parse;
            h_parse.set_string(src_resc_hier);
            std::string src_resc;
            h_parse.first_resc(src_resc);

            const std::string group_name = get_metadata_for_resource(
                    _rei->rsComm, 
                    config_.storage_tiering_group_attribute,
                    src_resc);

            std::map<std::string, std::string> idx_resc_map;
            get_tier_group_resources_and_indicies(
                    _rei->rsComm, 
                    group_name,
                    idx_resc_map);

            const std::string low_tier_resc_name = idx_resc_map.begin()->second;
            const std::string verification_type  = get_verification_for_resc(
                    _rei->rsComm,
                    low_tier_resc_name);
            const std::string restage_delay_params{get_restage_delay_param_for_resc(
                    _rei->rsComm,
                    src_resc)};
            queue_restage_operation(
                    _rei,
                    restage_delay_params,
                    verification_type,
                    src_resc,
                    low_tier_resc_name,
                    obj_path);
        }
        catch(const boost::bad_any_cast& _e) {
            THROW(INVALID_ANY_CAST, _e.what());
        }
        catch(const exception& _e) {
            throw;
        }
    } // restage_object_to_lowest_tier

    void storage_tiering::apply_storage_tiering_policy(
            const std::string& _group) {
        const std::map<std::string, std::string> rescs = get_resource_map_for_group(
                comm_,
                _group);
        auto resc_itr = rescs.begin();
        for( ; resc_itr != rescs.end(); ++resc_itr) {
            auto next_itr = resc_itr;
            ++next_itr;
            tier_objects_for_resc(comm_, resc_itr->second, next_itr->second);
        } // for resc

    } // apply_storage_tiering_policy

    

}; // namespace irods
