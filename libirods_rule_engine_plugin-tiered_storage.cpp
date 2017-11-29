// =-=-=-=-=-=-=-
// irods includes
#include "irods_re_plugin.hpp"
#include "irods_re_serialization.hpp"
#include "irods_server_properties.hpp"
#include "irods_re_serialization.hpp"
#include "irods_resource_manager.hpp"
#include "irods_virtual_path.hpp"

#include "irods_query.hpp"

#include "rsModAVUMetadata.hpp"
#include "rsDataObjRepl.hpp"
#include "rsDataObjTrim.hpp"
#include "rsDataObjChksum.hpp"
#include "rsFileStat.hpp"
#include "physPath.hpp"

#undef LIST

// =-=-=-=-=-=-=-
// stl includes
#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <chrono>
#include <ctime>
#include <sstream>
#include <map>
#include <iostream>
#include <fstream>
#include <mutex>

// =-=-=-=-=-=-=-
// boost includes
#include <boost/any.hpp>
#include <boost/regex.hpp>
#include <boost/exception/all.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>

#include "json.hpp"

int _delayExec(
    const char *inActionCall,
    const char *recoveryActionCall,
    const char *delayCondition,
    ruleExecInfo_t *rei );

extern irods::resource_manager resc_mgr;

static const std::string VERIFY_CHECKSUM{"checksum"};
static const std::string VERIFY_FILESYSTEM{"filesystem"};
static const std::string VERIFY_CATALOG{"catalog"};

struct storage_tiering_configuration {
    std::string access_time_attribute{"irods::access_time"};
    std::string storage_tiering_group_attribute{"irods::storage_tier_group"};
    std::string storage_tiering_time_attribute{"irods::storage_tier_time"};
    std::string storage_tiering_query_attribute{"irods::storage_tier_query"};
    std::string storage_tiering_verification_attribute{"irods::storage_tier_verification"};
    std::string storage_tiering_restage_delay_attribute{"irods::storage_tier_restage_delay"};

    std::string default_restage_delay_parameters{"<PLUSET>1s</PLUSET><EF>1h DOUBLE UNTIL SUCCESS OR 6 TIMES</EF>"};
    std::string time_check_string{"TIME_CHECK_STRING"};
};

static std::string instance_name{};
static storage_tiering_configuration inst_cfg;

irods::error get_plugin_specific_configuration(
    const std::string& _instance_name ) {

    try {
        const auto& rule_engines = irods::get_server_property<
                                       const std::vector<boost::any>&>(
                                           std::vector<std::string>{
                                               irods::CFG_PLUGIN_CONFIGURATION_KW,
                                               irods::PLUGIN_TYPE_RULE_ENGINE});
        for ( const auto& elem : rule_engines ) {
            const auto& rule_engine = boost::any_cast< const std::unordered_map< std::string, boost::any >& >( elem );
            const auto& inst_name   = boost::any_cast< const std::string& >( rule_engine.at( irods::CFG_INSTANCE_NAME_KW ) );
            if ( inst_name == _instance_name ) {
                if(rule_engine.count(irods::CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW) > 0) {
                    const auto& plugin_spec_cfg = boost::any_cast<const std::unordered_map<std::string, boost::any>&>(
                                                      rule_engine.at(irods::CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW));
                    if(plugin_spec_cfg.find("access_time_attribute") != plugin_spec_cfg.end()) {
                        inst_cfg.access_time_attribute = boost::any_cast<std::string>(
                                plugin_spec_cfg.at("access_time_attribute"));
                    }

                    if(plugin_spec_cfg.find("storage_tiering_group_attribute") != plugin_spec_cfg.end()) {
                        inst_cfg.storage_tiering_group_attribute = boost::any_cast<std::string>(
                            plugin_spec_cfg.at("storage_tiering_group_attribute"));
                    }

                    if(plugin_spec_cfg.find("storage_tiering_time_attribute") != plugin_spec_cfg.end()) {
                        inst_cfg.storage_tiering_time_attribute = boost::any_cast<std::string>(
                            plugin_spec_cfg.at("storage_tiering_time_attribute"));
                    }

                    if(plugin_spec_cfg.find("storage_tiering_query_attribute") != plugin_spec_cfg.end()) {
                        inst_cfg.storage_tiering_query_attribute = boost::any_cast<std::string>(
                                plugin_spec_cfg.at("storage_tiering_query_attribute"));
                    }

                    if(plugin_spec_cfg.find("storage_tiering_verification_attribute") != plugin_spec_cfg.end()) {
                        inst_cfg.storage_tiering_verification_attribute = boost::any_cast<std::string>(
                            plugin_spec_cfg.at("storage_tiering_verification_attribute"));
                    }

                    if(plugin_spec_cfg.find("storage_tiering_restage_delay_attribute") != plugin_spec_cfg.end()) {
                        inst_cfg.storage_tiering_restage_delay_attribute = boost::any_cast<std::string>(
                            plugin_spec_cfg.at("storage_tiering_restage_delay_attribute"));
                    }

                    if(plugin_spec_cfg.find("default_restage_delay_parameters") != plugin_spec_cfg.end()) {
                        inst_cfg.default_restage_delay_parameters = boost::any_cast<std::string>(
                            plugin_spec_cfg.at("default_restage_delay_parameters"));
                    }

                    if(plugin_spec_cfg.find("time_check_string") != plugin_spec_cfg.end()) {
                        inst_cfg.time_check_string = boost::any_cast<std::string>(
                            plugin_spec_cfg.at("time_check_string"));
                    }
                }

                return SUCCESS();
            }
        }
    } catch ( const boost::bad_any_cast& e ) {
        return ERROR( INVALID_ANY_CAST, e.what() );
    } catch ( const std::out_of_range& e ) {
        return ERROR( KEY_NOT_FOUND, e.what() );
    }

    std::stringstream msg;
    msg << "failed to find configuration for storage_tiering plugin ["
        << _instance_name << "]";
    rodsLog( LOG_ERROR, "%s", msg.str().c_str() );
    return ERROR( SYS_INVALID_INPUT_PARAM, msg.str() );

} // get_plugin_specific_configuration


irods::error start(
    irods::default_re_ctx&,
    const std::string& _instance_name ) {
    instance_name = _instance_name;
    irods::error ret = get_plugin_specific_configuration( _instance_name );
    if( !ret.ok() ) {
        irods::log(PASS(ret));
    }

    return SUCCESS();
}

irods::error stop(
    irods::default_re_ctx&,
    const std::string& ) {
    return SUCCESS();
}

irods::error rule_exists(
    irods::default_re_ctx&,
    const std::string& _rn,
    bool&              _ret) {

    // TODO: compare to specific rule strings here
    std::set<std::string> rules{ "pep_api_data_obj_close_post",
                                 "pep_api_data_obj_put_post",
                                 "pep_api_data_obj_get_post"};
    _ret = rules.find(_rn) != rules.end();
    
    return SUCCESS();
}

irods::error list_rules(irods::default_re_ctx&, std::vector<std::string>&) {
    return SUCCESS();
}


// =-=-=-=-=-=-=-
// Helper Functions


void update_access_time_for_data_object(
    rsComm_t*          _comm,
    const std::string& _logical_path) {

    std::string ts = std::to_string(std::time(nullptr)); 
    
    modAVUMetadataInp_t avuOp;
    memset(&avuOp, 0, sizeof(avuOp));
    avuOp.arg0 = "set";
    avuOp.arg1 = "-d";
    avuOp.arg2 = (char*)_logical_path.c_str();
    avuOp.arg3 = (char*)inst_cfg.access_time_attribute.c_str();
    avuOp.arg4 = (char*)ts.c_str();
    avuOp.arg5 = "";

    int status = rsModAVUMetadata(_comm, &avuOp);
    if(status < 0) {
        std::string msg{"failed to set access time for ["};
        msg += _logical_path + "]";
        THROW(status, msg);
    }
} // update_access_time_for_data_object

void get_object_and_collection_from_path(
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

std::string get_metadata_for_resource(
    rsComm_t*          _comm, 
    const std::string& _meta_attr_name,
    const std::string& _resc_name ) {

    try {
        std::string query_str {
            boost::str(
            boost::format("SELECT META_RESC_ATTR_VALUE WHERE META_RESC_ATTR_NAME = '%s' and RESC_NAME = '%s'") %
            _meta_attr_name %
            _resc_name) };
        irods::query query(_comm, query_str, 1);
        if(query.size() > 0) {
            return query.front()[0];
        }

        THROW(
            CAT_NO_ROWS_FOUND,
            boost::format("no results found for resc [%s] with attribute [%s]") %
            _resc_name % 
            _meta_attr_name);
    }
    catch(const irods::exception& _e) {
        throw;
    }
} // get_metadata_for_resource

void get_tier_group_resources_and_indicies(
    rsComm_t*                           _comm, 
    const std::string&                  _group_name,
    std::map<std::string, std::string>& _resc_map ) {

    try {
        std::string query_str{
            boost::str(
            boost::format(
            "SELECT RESC_NAME, META_RESC_ATTR_UNITS WHERE META_RESC_ATTR_NAME = '%s' and META_RESC_ATTR_VALUE = '%s'") %
            inst_cfg.storage_tiering_group_attribute %
            _group_name) };
        irods::query query(_comm, query_str);
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
    catch(const irods::exception& _e) {
        throw;
    }
} // get_tier_group_resources_and_indicies

std::string get_leaf_resources_string(
    rsComm_t*                 _comm,
    const std::string&        _resc_name) {
    std::string leaf_id_str;

    // if the resource has no children then simply return
    irods::resource_ptr root_resc;
    irods::error err = resc_mgr.resolve(_resc_name, root_resc);
    if(!err.ok()) {
        THROW(err.code(), err.result());
    }

    try {
        std::vector<irods::resource_manager::leaf_bundle_t> leaf_bundles = 
            resc_mgr.gather_leaf_bundles_for_resc(_resc_name);
        for(const auto & bundle : leaf_bundles) {
            for(const auto & leaf_id : bundle) {
                leaf_id_str += 
                    "'" + boost::str(boost::format("%s") % leaf_id) + "',";
            } // for
        } // for
    }
    catch( const irods::exception & _e ) {
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

void replicate_object_to_resource(
    rsComm_t*          _comm,
    const std::string& _verification_type,
    const std::string& _src_resc,
    const std::string& _dst_resc,
    const std::string& _obj_path) {
   
    dataObjInp_t data_obj_inp;
    memset(&data_obj_inp, 0, sizeof(data_obj_inp));
    rstrcpy(data_obj_inp.objPath, _obj_path.c_str(), MAX_NAME_LEN);
    data_obj_inp.createMode = getDefFileMode();
    addKeyVal(&data_obj_inp.condInput, RESC_NAME_KW,      _src_resc.c_str());
    addKeyVal(&data_obj_inp.condInput, DEST_RESC_NAME_KW, _dst_resc.c_str());
    addKeyVal(&data_obj_inp.condInput, ADMIN_KW,          "" );
    if(VERIFY_CHECKSUM == _verification_type) {
        addKeyVal(&data_obj_inp.condInput, VERIFY_CHKSUM_KW, "" );
    }

    transferStat_t* trans_stat = NULL;
    int repl_err = rsDataObjRepl(_comm, &data_obj_inp, &trans_stat);
    free(trans_stat);
    if(repl_err < 0) {
        THROW(
            repl_err,
            boost::format("failed to migrate [%s] to [%s]") %
            _obj_path % _dst_resc);
    }
} // replicate_object_to_resource

void capture_replica_attributes(
        rsComm_t*          _comm,
        const std::string& _obj_path,
        const std::string& _resc_name,
        std::string&       _data_path,
        std::string&       _data_size,
        std::string&       _data_hier,
        std::string&       _data_checksum ) {
    try {
        std::string coll_name, obj_name;
        get_object_and_collection_from_path(
            _obj_path,
            coll_name,
            obj_name);

        std::string leaf_str = get_leaf_resources_string(
                                   _comm,
                                   _resc_name);

        std::string query_str{ boost::str(
                        boost::format("SELECT DATA_PATH, DATA_RESC_HIER, DATA_SIZE, DATA_CHECKSUM WHERE DATA_NAME = '%s' AND COLL_NAME = '%s' AND DATA_RESC_ID IN (%s)") %
                        obj_name %
                        coll_name %
                        leaf_str)};
        irods::query query(_comm, query_str, 1);
        if(query.size() > 0) {
            irods::query::value_type result = query.front();
            _data_path     = result[0];
            _data_hier     = result[1];
            _data_size     = result[2];
            _data_checksum = result[3];
        }
    }
    catch(const irods::exception&) {
        throw;
    }
} // capture_replica_attributes

rodsLong_t get_file_size_from_filesystem(
        rsComm_t*         _comm,
        const std::string _obj_path,
        const std::string _resc_hier,
        const std::string _file_path ) {
    fileStatInp_t stat_inp;
    memset(&stat_inp, 0, sizeof(stat_inp));
    rstrcpy(stat_inp.objPath,  _obj_path.c_str(),  sizeof(stat_inp.objPath));
    rstrcpy(stat_inp.rescHier, _resc_hier.c_str(), sizeof(stat_inp.rescHier));
    rstrcpy(stat_inp.fileName, _file_path.c_str(), sizeof(stat_inp.fileName));
    rodsStat_t *stat_out = nullptr;
    const int status_rsFileStat = rsFileStat(_comm, &stat_inp, &stat_out);
    if(status_rsFileStat < 0) {
        THROW(
            status_rsFileStat,
            boost::format("rsFileStat of objPath [%s] rescHier [%s] fileName [%s] failed with [%d]") %
            stat_inp.objPath %
            stat_inp.rescHier %
            stat_inp.fileName %
            status_rsFileStat);
        return status_rsFileStat;
    }

    const rodsLong_t size_in_vault = stat_out->st_size;
    free(stat_out);
    return size_in_vault;
} // get_file_size_from_filesystem

std::string compute_checksum_for_resc(
        rsComm_t*         _comm,
        const std::string _obj_path,
        const std::string _resc_name ) {
    // query if a checksum exists
    std::string coll_name, obj_name;
    get_object_and_collection_from_path(
        _obj_path,
        coll_name,
        obj_name);

    std::string query_str{ boost::str(
                    boost::format("SELECT DATA_CHECKSUM WHERE DATA_NAME = '%s' AND COLL_NAME = '%s' AND RESC_NAME = '%s'") %
                    obj_name %
                    coll_name %
                    _resc_name)};
    std::string data_checksum;
    irods::query query(_comm, query_str, 1);
    if(query.size() > 0) {
        irods::query::value_type result = query.front();
        data_checksum = result[0];
    }

    if(!data_checksum.empty()) {
        return data_checksum;
    }

    // no checksum, compute one
    dataObjInp_t data_obj_inp;
    memset(&data_obj_inp, 0, sizeof(data_obj_inp));
    rstrcpy(data_obj_inp.objPath, _obj_path.c_str(), MAX_NAME_LEN);
    addKeyVal(&data_obj_inp.condInput, RESC_NAME_KW, _resc_name.c_str());

    char* chksum = nullptr;
    int chksum_err = rsDataObjChksum(_comm, &data_obj_inp, &chksum);
    if(chksum_err < 0) {
        THROW(
            chksum_err,
            boost::format("rsDataObjChksum failed for [%s] on [%s]") %
            _obj_path %
            _resc_name);
    }

    return chksum;
} // compute_checksum_for_resc

bool verify_replica_on_resource(
    rsComm_t*          _comm,
    const std::string& _verification_type,
    const std::string& _dst_resc,
    const std::string& _obj_path,
    const std::string& _data_size,
    const std::string& _data_hier,
    const std::string& _data_path,
    const std::string& _data_checksum) {

    try {
        if(_verification_type.size() <= 0 || VERIFY_CATALOG == _verification_type) {
            // default verification type is 'catalog', were done as the query worked
            return true;
        }

        if(VERIFY_FILESYSTEM == _verification_type ||
           VERIFY_CHECKSUM   == _verification_type) {
            rodsLong_t fs_size = get_file_size_from_filesystem(
                                     _comm,
                                     _obj_path,
                                     _data_hier,
                                     _data_path);
            const rodsLong_t query_size = boost::lexical_cast<rodsLong_t>(_data_size);
            if(fs_size != query_size) {
                return false;
            }

            // replication handles checksum verification
            //
            return true;
        }

        THROW(
            SYS_INVALID_INPUT_PARAM,
            boost::format("invalid verification type [%s]") %
            _verification_type);

    }
    catch(const boost::bad_lexical_cast& _e) {
        THROW(
            INVALID_LEXICAL_CAST,
            _e.what());
    }
    catch(const irods::exception&) {
        throw;
    }
} // verify_replica_on_resource

void trim_replica_on_resource(
    rsComm_t*          _comm,
    const std::string& _obj_path,
    const std::string& _src_resc) {

    dataObjInp_t obj_inp;
    memset(&obj_inp, 0, sizeof(obj_inp));

    //obj_inp.oprType = UNREG_OPR;

    rstrcpy(obj_inp.objPath, _obj_path.c_str(), sizeof(obj_inp.objPath));
    addKeyVal(
        &obj_inp.condInput,
        RESC_NAME_KW,
        _src_resc.c_str());
    addKeyVal(
        &obj_inp.condInput,
        COPIES_KW,
        "1");

    int trim_err = rsDataObjTrim(_comm, &obj_inp);
    if(trim_err < 0) {
        THROW(
            trim_err,
            boost::format("failed to trim obj path [%s] src resc [%s]") %
            _obj_path %
            _src_resc);
    }
} // trim_replica_on_resource

void migrate_object_to_resource(
    rsComm_t*          _comm,
    const std::string& _verification_type,
    const std::string& _src_resc,
    const std::string& _dst_resc,
    const std::string& _obj_path) {

    try {
        if(VERIFY_CHECKSUM == _verification_type) {
            compute_checksum_for_resc(
                _comm,
                _obj_path,
                _src_resc);
        }

        replicate_object_to_resource(
            _comm,
            _verification_type,
            _src_resc,
            _dst_resc,
            _obj_path);

        std::string data_path;
        std::string data_size;
        std::string data_hier;
        std::string data_checksum;
        capture_replica_attributes(
            _comm,
            _obj_path,
            _dst_resc,
            data_path,
            data_size,
            data_hier,
            data_checksum );

        bool verified = verify_replica_on_resource(
                            _comm,
                            _verification_type,
                            _dst_resc,
                            _obj_path,
                            data_size,
                            data_hier,
                            data_path,
                            data_checksum);
        if(!verified) {
            THROW(
                SYS_INVALID_FILE_PATH,
                "replica not verified");
        }

        trim_replica_on_resource(
            _comm,
            _obj_path,
            _src_resc);
    }
    catch(const irods::exception&) {
        throw;
    }
} // migrate_object_to_resc

std::string get_verification_for_resc(
   rsComm_t*          _comm, 
   const std::string& _resc_name) {

    try {
        std::string ver = get_metadata_for_resource(
                              _comm,
                              inst_cfg.storage_tiering_verification_attribute,
                              _resc_name);
        return ver;
    }
    catch(const irods::exception& _e) {
        return "catalog"; 
    }

} // get_verification_for_resc

std::string get_restage_delay_param_for_resc(
   rsComm_t*          _comm, 
   const std::string& _resc_name) {

    try {
        std::string ver = get_metadata_for_resource(
                              _comm,
                              inst_cfg.storage_tiering_restage_delay_attribute,
                              _resc_name);
        return ver;
    }
    catch(const irods::exception& _e) {
        return inst_cfg.default_restage_delay_parameters;
    }

} // get_restage_delay_param_for_resc

std::map<std::string, std::string> get_resource_map_for_group(
    rsComm_t*          _comm,
    const std::string& _group) {
    std::map<std::string, std::string> groups;
    try {
        std::string query_str{
            boost::str(
            boost::format("SELECT META_RESC_ATTR_UNITS, RESC_NAME WHERE META_RESC_ATTR_VALUE = '%s' AND META_RESC_ATTR_NAME = '%s'") %
            _group %
            inst_cfg.storage_tiering_group_attribute)}; 
        irods::query query{_comm, query_str};
        for(const auto& g : query) {
            groups[g[0]] = g[1];
        }

        return groups;
    }
    catch(const std::exception&) {
        return groups;
    }

} // get_resource_map_for_group

std::string get_tier_time_for_resc(
    rsComm_t*          _comm,
    const std::string& _resc_name) {
    try {
        std::time_t now = std::time(nullptr);
        std::string offset_str = get_metadata_for_resource(
                                     _comm,
                                     inst_cfg.storage_tiering_time_attribute,
                                     _resc_name);
        std::time_t offset = boost::lexical_cast<std::time_t>(offset_str);
        return std::to_string(now - offset);
    }
    catch(const boost::bad_lexical_cast& _e) {
        THROW(
            INVALID_LEXICAL_CAST,
            _e.what());
    }
    catch(const irods::exception&) {
        throw;
    }
} // get_tier_time_for_resc

void tier_objects_for_resc(
    rsComm_t*          _comm,
    const std::string& _src_resc,
    const std::string& _dst_resc) {
    try {
        const std::string tier_time{get_tier_time_for_resc(_comm, _src_resc)};

        std::string query_str;
        try {
            query_str = get_metadata_for_resource(
                                    _comm,
                                    inst_cfg.storage_tiering_query_attribute,
                                    _src_resc);
            size_t start_pos = query_str.find(inst_cfg.time_check_string);
            if(start_pos != std::string::npos) {
                query_str.replace(start_pos, inst_cfg.time_check_string.length(), tier_time);
            }
        }
        catch(const irods::exception&) {
            const std::string leaf_str{ get_leaf_resources_string(
                                             _comm,
                                             _src_resc)};
            query_str =
                boost::str(
                boost::format("SELECT DATA_NAME, COLL_NAME WHERE META_DATA_ATTR_NAME = '%s' AND META_DATA_ATTR_VALUE < '%s' AND DATA_RESC_ID IN (%s)") %
                inst_cfg.access_time_attribute %
                tier_time %
                leaf_str);
        }

        irods::query query{_comm, query_str, 1};
        if(query.size() > 0) {
            irods::query::value_type result = query.front();

            std::string obj_path = result[1]; 
            const std::string vps = irods::get_virtual_path_separator();
            if( !boost::ends_with(obj_path, vps)) {
                obj_path += vps;
            }
            obj_path += result[0];

            const std::string verification_type{get_verification_for_resc(
                                                    _comm,
                                                    _src_resc)};
            migrate_object_to_resource(
                _comm,
                verification_type,
                _src_resc,
                _dst_resc,
                obj_path);
        }
    }
    catch(const irods::exception& _e) {
        // if nothing of interest is found, thats not an error
        if(CAT_NO_ROWS_FOUND == _e.code()) {
            return;
        }

        throw;
    }
} // tier_objects_for_resc

void queue_restage_operation(
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

void apply_access_time(
    rsComm_t*              _comm,
    std::list<boost::any>& _args) {
    namespace sp = irods::re_serialization;

    try {
        // NOTE:: 3rd parameter is the dataObjInp_t
        std::list<boost::any>::iterator it = _args.begin();
        std::advance(it, 2);
        if(_args.end() == it) {
           THROW(SYS_INVALID_INPUT_PARAM, "invalid number of arguments");
        }

        dataObjInp_t* obj_inp = boost::any_cast<dataObjInp_t*>(*it);
        update_access_time_for_data_object(_comm, obj_inp->objPath);
    }
    catch(const boost::bad_any_cast& _e) {
        THROW( INVALID_ANY_CAST, _e.what() );
    }
    catch(const irods::exception& _e) {
        throw;
    }
} // apply_access_time

void restage_object_to_lowest_tier(
    ruleExecInfo_t*        _rei,
    std::list<boost::any>& _args) {
    namespace sp = irods::re_serialization;

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

        irods::hierarchy_parser h_parse;
        h_parse.set_string(src_resc_hier);
        std::string src_resc;
        h_parse.first_resc(src_resc);

        const std::string group_name = get_metadata_for_resource(
                                           _rei->rsComm, 
                                           inst_cfg.storage_tiering_group_attribute,
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
    catch(const irods::exception& _e) {
        throw;
    }
} // restage_object_to_lowest_tier

void apply_storage_tiering_policy(
    rsComm_t* _comm,
    const std::string& _group) {
        const std::map<std::string, std::string> rescs = get_resource_map_for_group(
                                                             _comm,
                                                             _group);
        auto resc_itr = rescs.begin();
        for( ; resc_itr != rescs.end(); ++resc_itr) {
            auto next_itr = resc_itr;
            ++next_itr;
            tier_objects_for_resc(_comm, resc_itr->second, next_itr->second);
        } // for resc

} // apply_storage_tiering_policy

irods::error exec_rule(
    irods::default_re_ctx&,
    const std::string&     _rn,
    std::list<boost::any>& _args,
    irods::callback        _eff_hdlr) {

    ruleExecInfo_t* rei = nullptr;
    irods::error err = _eff_hdlr("unsafe_ms_ctx", &rei);
    if(!err.ok()) {
        return err;
    }

    try {
        std::set<std::string> access_time_rules{
                                  "pep_api_data_obj_close_post",
                                  "pep_api_data_obj_put_post",
                                  "pep_api_data_obj_get_post"};
        if(access_time_rules.find(_rn) != access_time_rules.end()) {
            apply_access_time(rei->rsComm, _args);
        }

        if(_rn == "pep_api_data_obj_get_post") {
            restage_object_to_lowest_tier(rei, _args);
        }
    }
    catch(const irods::exception& _e) {
        irods::log(_e);
        return irods::error(_e);
    }

    // additional magic goes here

    return err;

} // exec_rule

irods::error exec_rule_text(
    irods::default_re_ctx&,
    const std::string&     _rule_text,
    msParamArray_t*        _ms_params,
    const std::string&     _out_desc,
    irods::callback        _eff_hdlr) {

    try {
        using json = nlohmann::json;

        // skip the first line: @external
        json rule_obj = json::parse(_rule_text.substr(10));

        // if the rule text does not have our instance name, fail
        if(rule_obj["rule-engine-instance-name"] != instance_name) {
            return ERROR(
                    SYS_NOT_SUPPORTED,
                    "instance name not found");
        }

        if(rule_obj["rule-engine-operation"] == "apply_storage_tiering_policy") {
            ruleExecInfo_t* rei = nullptr;
            irods::error err = _eff_hdlr("unsafe_ms_ctx", &rei);
            if(!err.ok()) {
                return err;
            }

            json delay_obj;
            delay_obj["rule-engine-operation"] = rule_obj["rule-engine-operation"];
            delay_obj["storage-tier-groups"] = rule_obj["storage-tier-groups"];
            const std::string& delay_cond = rule_obj["delay-parameters"];

            int delay_err = _delayExec(
                                delay_obj.dump().c_str(),
                                "",
                                delay_cond.c_str(), 
                                rei);
            if(delay_err < 0) {
                return ERROR(
                        delay_err,
                        "delayExec failed");
            }
        }
        else {
            return ERROR(
                    SYS_NOT_SUPPORTED,
                    "supported rule name not found");
        }
    }
    catch(const std::domain_error& _e) {
        irods::log(LOG_ERROR, _e.what());
        return ERROR(
                   SYS_NOT_SUPPORTED,
                   _e.what());
    }
    catch(const irods::exception& _e) {
        return ERROR(
                _e.code(),
                _e.what());
    }

    return SUCCESS();
} // exec_rule_text

irods::error exec_rule_expression(
    irods::default_re_ctx&,
    const std::string&     _rule_text,
    msParamArray_t*        _ms_params,
    irods::callback        _eff_hdlr) {
    using json = nlohmann::json;

    json rule_obj = json::parse(_rule_text);
    if("apply_storage_tiering_policy" == rule_obj["rule-engine-operation"]) {
        ruleExecInfo_t* rei = nullptr;
        irods::error err = _eff_hdlr("unsafe_ms_ctx", &rei);
        if(!err.ok()) {
            return err;
        }

        try {
            for(const auto& group : rule_obj["storage-tier-groups"]) {
                apply_storage_tiering_policy(rei->rsComm, group); 
            }
        }
        catch(const irods::exception& _e) {
            return ERROR(
                    _e.code(),
                    _e.what());
        }
    }
    else if("migrate_object_to_resource" == rule_obj["rule-engine-operation"]) {
        ruleExecInfo_t* rei = nullptr;
        irods::error err = _eff_hdlr("unsafe_ms_ctx", &rei);
        if(!err.ok()) {
            return err;
        }

        try {
            migrate_object_to_resource(
                rei->rsComm,
                rule_obj["verification-type"],
                rule_obj["source-resc"],
                rule_obj["destination-resc"],
                rule_obj["object-path"]);

        }
        catch(const irods::exception& _e) {
            return ERROR(
                    _e.code(),
                    _e.what());
        }

    }
    else {
        return ERROR(
                SYS_NOT_SUPPORTED,
                "supported rule name not found");
    }

    return SUCCESS();
}

extern "C"
irods::pluggable_rule_engine<irods::default_re_ctx>* plugin_factory(
    const std::string& _inst_name,
    const std::string& _context ) {
    irods::pluggable_rule_engine<irods::default_re_ctx>* re = 
        new irods::pluggable_rule_engine<irods::default_re_ctx>(
                _inst_name,
                _context);
    
    re->add_operation<
        irods::default_re_ctx&,
        const std::string&>(
            "start",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&)>(start));

    re->add_operation<
        irods::default_re_ctx&,
        const std::string&>(
            "stop",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&)>(stop));

    re->add_operation<
        irods::default_re_ctx&,
        const std::string&,
        bool&>(
            "rule_exists",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&,
                    bool&)>(rule_exists));

    re->add_operation<
        irods::default_re_ctx&,
        std::vector<std::string>&>(
            "list_rules",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    std::vector<std::string>&)>(list_rules));

    re->add_operation<
        irods::default_re_ctx&,
        const std::string&,
        std::list<boost::any>&,
        irods::callback>(
            "exec_rule",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&,
                    std::list<boost::any>&,
                    irods::callback)>(exec_rule));

    re->add_operation<
        irods::default_re_ctx&,
        const std::string&,
        msParamArray_t*,
        const std::string&,
        irods::callback>(
            "exec_rule_text",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&,
                    msParamArray_t*,
                    const std::string&,
                    irods::callback)>(exec_rule_text));

    re->add_operation<
        irods::default_re_ctx&,
        const std::string&,
        msParamArray_t*,
        irods::callback>(
            "exec_rule_expression",
            std::function<
                irods::error(
                    irods::default_re_ctx&,
                    const std::string&,
                    msParamArray_t*,
                    irods::callback)>(exec_rule_expression));
    return re;

} // plugin_factory

