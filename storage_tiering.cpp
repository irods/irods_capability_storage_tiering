
#undef RODS_SERVER

#include <irods/irods_server_properties.hpp>
#include <irods/irods_re_plugin.hpp>
#include "storage_tiering.hpp"
#include "storage_tiering_utilities.hpp"
#include <irods/irods_query.hpp>
#include <irods/irods_virtual_path.hpp>
#include <irods/irods_hierarchy_parser.hpp>
#include <irods/irods_resource_manager.hpp>
#include <irods/irods_resource_backport.hpp>
#include <irods/query_processor.hpp>
#include "proxy_connection.hpp"

#include <irods/modAVUMetadata.h>
#include <irods/rsExecMyRule.hpp>
#include <irods/execMyRule.h>
#include <irods/rsOpenCollection.hpp>
#include <irods/rsReadCollection.hpp>
#include <irods/rsCloseCollection.hpp>

#include "data_verification_utilities.hpp"

#include "exec_as_user.hpp"

#include <boost/any.hpp>
#include <boost/regex.hpp>
#include <boost/exception/all.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/lexical_cast.hpp>

#include <nlohmann/json.hpp>

#include <charconv>
#include <random>
#include <system_error>
#include <tuple>

extern irods::resource_manager resc_mgr;

int _delayExec(
    const char *inActionCall,
    const char *recoveryActionCall,
    const char *delayCondition,
    ruleExecInfo_t *rei );



namespace irods {
    const std::string storage_tiering::policy::storage_tiering{"irods_policy_storage_tiering"};
    const std::string storage_tiering::policy::data_movement{"irods_policy_data_movement"};
    const std::string storage_tiering::policy::access_time{"irods_policy_apply_access_time"};

    const std::string storage_tiering::schedule::storage_tiering{"irods_policy_schedule_storage_tiering"};
    const std::string storage_tiering::schedule::data_movement{"irods_policy_schedule_data_object_movement"};

    storage_tiering::storage_tiering(
        rcComm_t*          _comm,
        ruleExecInfo_t*    _rei,
        const std::string& _instance_name) :
          comm_(_comm)
        , rei_(_rei)
        , config_(_instance_name) {

    }

    // =-=-=-=-=-=-=-
    // Helper Functions
    std::string storage_tiering::get_metadata_for_data_object(
        rcComm_t*          _comm,
        const std::string& _meta_attr_name,
        const std::string& _object_path ) {
        boost::filesystem::path p{_object_path};
        std::string coll_name = p.parent_path().string();
        std::string data_name = p.filename().string();


        std::string query_str {
            boost::str(
                boost::format("SELECT META_DATA_ATTR_VALUE WHERE META_DATA_ATTR_NAME = '%s' and DATA_NAME = '%s' AND COLL_NAME = '%s'") %
                    _meta_attr_name %
                    data_name %
                    coll_name) };
        query<rcComm_t> qobj{_comm, query_str, 1};
        if(qobj.size() > 0) {
            return qobj.front()[0];
        }

        THROW(
            CAT_NO_ROWS_FOUND,
            boost::format("no results found for object [%s] with attribute [%s]") %
            _object_path %
            _meta_attr_name);
    } // get_metadata_for_data_object

    std::string storage_tiering::get_metadata_for_resource(
        rcComm_t*          _comm,
        const std::string& _meta_attr_name,
        const std::string& _resource_name ) {
        std::string query_str {
            boost::str(
                    boost::format("SELECT META_RESC_ATTR_VALUE WHERE META_RESC_ATTR_NAME = '%s' and RESC_NAME = '%s'") %
                    _meta_attr_name %
                    _resource_name) };
        query<rcComm_t> qobj{_comm, query_str, 1};
        if(qobj.size() > 0) {
            return qobj.front()[0];
        }

        THROW(
            CAT_NO_ROWS_FOUND,
            boost::format("no results found for resc [%s] with attribute [%s]") %
            _resource_name %
            _meta_attr_name);
    } // get_metadata_for_resource

    void storage_tiering::get_metadata_for_resource(
        rcComm_t*           _comm,
        const std::string&  _meta_attr_name,
        const std::string&  _resource_name,
        metadata_results&   _results ) {
        std::string query_str {
            boost::str(
                    boost::format("SELECT META_RESC_ATTR_VALUE, META_RESC_ATTR_UNITS WHERE META_RESC_ATTR_NAME = '%s' and RESC_NAME = '%s'") %
                    _meta_attr_name %
                    _resource_name) };
        query<rcComm_t> qobj{_comm, query_str};
        if(qobj.size() > 0) {
            for( const auto& r : qobj) {
                _results.push_back(std::make_pair(r[0], r[1]));
            }

            return;
        }

        THROW(
            CAT_NO_ROWS_FOUND,
            boost::format("no results found for resc [%s] with attribute [%s]") %
            _resource_name %
            _meta_attr_name);
    } // get_metadata_for_resource

    resource_index_map storage_tiering::get_tier_group_resource_ids_and_indices(
        rcComm_t*          _comm,
        const std::string& _group_name) {
        resource_index_map resc_map;
        std::string query_str{
            boost::str(
                    boost::format(
                        "SELECT RESC_ID, META_RESC_ATTR_UNITS WHERE META_RESC_ATTR_NAME = '%s' and META_RESC_ATTR_VALUE = '%s'") %
                    config_.group_attribute %
                    _group_name) };
        for(auto row : query<rcComm_t>{_comm, query_str}) {
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
    } // get_tier_group_resource_ids_and_indices

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

    bool storage_tiering::get_preserve_replicas_for_resc(
          rcComm_t*          _comm
        , const std::string& _resource_name) {
        try {
            std::string pres = get_metadata_for_resource(
                                  _comm,
                                  config_.preserve_replicas,
                                  _resource_name);
            std::transform(pres.begin(), pres.end(), pres.begin(), [](unsigned char c) { return std::tolower(c); });
            return ("true" == pres);
        }
        catch(const exception& _e) {
            return false;
        }
    } // get_preserve_replicas_for_resc

    std::string storage_tiering::get_verification_for_resc(
          rcComm_t*          _comm
        , const std::string& _resource_name) {
        try {
            std::string ver = get_metadata_for_resource(
                                  _comm,
                                  config_.verification_attribute,
                                  _resource_name);
            return ver;
        }
        catch(const exception& _e) {
            return "catalog"; 
        }

    } // get_verification_for_resc

    std::string storage_tiering::get_restage_tier_resource_name(
        rcComm_t*          _comm,
        const std::string& _group_name) {
        const auto idx_resc_map = get_tier_group_resource_ids_and_indices(
                                      _comm,
                                      _group_name);
        std::string resc_list;
        for(const auto& itr : idx_resc_map) {
            resc_list += "'"+itr.second + "', ";
        }

        std::string query_str{
            boost::str(
                    boost::format(
                        "SELECT RESC_NAME WHERE META_RESC_ATTR_NAME = '%s' and META_RESC_ATTR_VALUE = 'true' and RESC_ID IN (%s)") %
                    config_.minimum_restage_tier %
                    resc_list) };
        query<rcComm_t> qobj{_comm, query_str, 1};
        if(qobj.size() > 0) {
            const auto& result = qobj.front();
            if(qobj.size() > 1) {
                rodsLog(
                    LOG_ERROR,
                    "multiple [%s] tags defined.  selecting resource [%s]",
                    config_.minimum_restage_tier.c_str(),
                    result[0].c_str());
            }

            return result[0];
        }
        else {
            std::string resc_name;
            irods::error err = resc_mgr.leaf_id_to_hier(
                                   std::strtoll(idx_resc_map.begin()->second.c_str(), 0, 0),
                                   resc_name);
            if(!err.ok()) {
                THROW(
                    err.code(),
                    err.result());
            }

            return resc_name;
        }

    } // get_restage_tier_resource_name

    auto storage_tiering::get_group_tier_for_resource(RcComm* _comm,
                                                      const std::string& _resource_name,
                                                      const std::string& _group_name) -> int
    {
        const auto query_string = fmt::format("select META_RESC_ATTR_UNITS where META_RESC_ATTR_NAME = '{}' and "
                                              "META_RESC_ATTR_VALUE = '{}' and RESC_NAME = '{}'",
                                              config_.group_attribute,
                                              _group_name,
                                              _resource_name);

        const auto query = irods::query{_comm, query_string};

        if (query.empty()) {
            THROW(CAT_NO_ROWS_FOUND,
                  fmt::format("Resource [{}] has no tier for group [{}].", _resource_name, _group_name));
        }

        if (query.size() > 1) {
            THROW(CONFIGURATION_ERROR,
                  fmt::format("Resource [{}] has multiple tiers for group [{}].", _resource_name, _group_name));
        }

        int tier_value{};
        const auto meta_resc_attr_units = query.front()[0];
        const auto [str_ptr, ec] = std::from_chars(
            meta_resc_attr_units.c_str(), meta_resc_attr_units.c_str() + meta_resc_attr_units.size(), tier_value);

        if (ec == std::errc::invalid_argument) {
            THROW(CONFIGURATION_ERROR,
                  fmt::format("Resource [{}] has invalid tier [{}] for group [{}]: not a number",
                              _resource_name,
                              meta_resc_attr_units,
                              _group_name));
        }
        else if (ec == std::errc::result_out_of_range) {
            THROW(CONFIGURATION_ERROR,
                  fmt::format("Resource [{}] has invalid tier [{}] for group [{}]: out of range",
                              _resource_name,
                              meta_resc_attr_units,
                              _group_name));
        }

        if (tier_value < 0) {
            THROW(CONFIGURATION_ERROR,
                  fmt::format("Resource [{}] has invalid tier [{}] for group [{}]: negative value",
                              _resource_name,
                              meta_resc_attr_units,
                              _group_name));
        }

        return tier_value;
    } // get_group_tier_for_resource

    std::string storage_tiering::get_data_movement_parameters_for_resource(
        rcComm_t*          _comm,
        const std::string& _resource_name) {
        std::string params = "<INST_NAME>" + config_.instance_name + "</INST_NAME>";

        // if metadata is not present an irods::exception is thrown
        // which is ignored in the presence of strong defaults
        std::string extras{config_.default_data_movement_parameters};
        try {
            extras = get_metadata_for_resource(
                         _comm,
                         config_.data_movement_parameters_attribute,
                         _resource_name);
        }
        catch(const exception&) {
        }
        params += extras;

        int min_time{config_.default_minimum_delay_time};
        try {
            std::string t0 = get_metadata_for_resource(
                                 _comm,
                                 config_.minimum_delay_time,
                                 _resource_name);
            min_time = boost::lexical_cast<int>(t0);
        }
        catch(const exception&) {
        }
        catch(const boost::bad_lexical_cast&){
        }

        int max_time{config_.default_maximum_delay_time};
        try {
            std::string t1 = get_metadata_for_resource(
                                _comm,
                                config_.maximum_delay_time,
                                _resource_name);
            max_time = boost::lexical_cast<int>(t1);
        }
        catch(const exception&) {
        }
        catch(const boost::bad_lexical_cast&){
        }

        std::string sleep_time{"1"};
        try {
            std::random_device rd;
            std::mt19937 gen(rd());
            std::uniform_int_distribution<> dis(min_time, max_time);
            sleep_time = boost::lexical_cast<std::string>(dis(gen));
        }
        catch(const boost::bad_lexical_cast&){
        }

        params += "<PLUSET>"+sleep_time+"s</PLUSET>";

        rodsLog(
            config_.data_transfer_log_level_value,
            "irods::storage_tiering :: delay params for [%s] - [%s]",
            _resource_name.c_str(),
            params.c_str());

        return params;

    } // get_data_movement_parameters_for_resource

    resource_index_map storage_tiering::get_resource_map_for_group(
        rcComm_t*          _comm,
        const std::string& _group) {

        resource_index_map groups;
        try {
            std::string query_str{
                boost::str(
                        boost::format("SELECT META_RESC_ATTR_UNITS, RESC_NAME WHERE META_RESC_ATTR_VALUE = '%s' AND META_RESC_ATTR_NAME = '%s'") %
                        _group %
                        config_.group_attribute)};
            query<rcComm_t> qobj{_comm, query_str};
            for(const auto& g : qobj) {
                groups[g[0]] = g[1];
            }

            return groups;
        }
        catch(const std::exception&) {
            return groups;
        }

    } // get_resource_map_for_group

    std::string storage_tiering::get_tier_time_for_resc(
        rcComm_t*          _comm,
        const std::string& _resource_name) {
        try {
            std::time_t now = std::time(nullptr);
            std::string offset_str = get_metadata_for_resource(
                                         _comm,
                                         config_.time_attribute,
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
                config_.time_attribute);
        }

    } // get_tier_time_for_resc

    storage_tiering::metadata_results storage_tiering::get_violating_queries_for_resource(
        rcComm_t*          _comm,
        const std::string& _resource_name) {

        const auto tier_time = get_tier_time_for_resc(_comm, _resource_name);
        try {
            metadata_results results;
            get_metadata_for_resource(
                 _comm,
                 config_.query_attribute,
                 _resource_name,
                 results);

            for(auto& q_itr : results) {
                auto& query_string   = q_itr.first;
                auto& query_type_str = q_itr.second;
                size_t start_pos = query_string.find(config_.time_check_string);
                if(start_pos != std::string::npos) {
                    query_string.replace(
                        start_pos,
                        config_.time_check_string.length(),
                        tier_time);
                }

                rodsLog(
                    config_.data_transfer_log_level_value,
                    "custom query for [%s] -  [%s], [%s]",
                    _resource_name.c_str(),
                    query_string.c_str(),
                    query_type_str.c_str());
            } // for

            return results;
        }
        catch(const exception&) {
            const auto leaf_str = get_leaf_resources_string(_resource_name);
            metadata_results results;
            results.push_back(
                std::make_pair(boost::str(
                boost::format("SELECT DATA_NAME, COLL_NAME, USER_NAME, USER_ZONE, DATA_REPL_NUM WHERE META_DATA_ATTR_NAME = '%s' AND META_DATA_ATTR_VALUE < '%s' AND META_DATA_ATTR_UNITS <> '%s' AND DATA_RESC_ID IN (%s)")
                % config_.access_time_attribute
                % tier_time
                % config_.migration_scheduled_flag
                % leaf_str), ""));
            rodsLog(
                config_.data_transfer_log_level_value,
                "use default query for [%s]",
                _resource_name.c_str());
            return results;
        }
    } // get_violating_queries_for_resource

    uint32_t storage_tiering::get_object_limit_for_resource(
        rcComm_t*          _comm,
        const std::string& _resource_name) {
        try {
            const auto object_limit = get_metadata_for_resource(
                                          _comm,
                                          config_.object_limit,
                                          _resource_name);
            if(object_limit.empty()) {
                return 0;
            }

            return boost::lexical_cast<uint32_t>(object_limit);
        }
        catch(const boost::bad_lexical_cast& _e) {
            THROW(
                INVALID_LEXICAL_CAST,
                _e.what());
        }
        catch(const irods::exception& _e) {
            if(CAT_NO_ROWS_FOUND == _e.code()) {
                return 0;
            }

            throw;
        }
    } // get_object_limit_for_resource

    bool storage_tiering::skip_object_in_lower_tier(
        rcComm_t*          _comm,
        const std::string& _object_path,
        const std::string& _partial_list) {
        boost::filesystem::path p{_object_path};
        std::string coll_name = p.parent_path().string();
        std::string data_name = p.filename().string();
        std::string qstr{boost::str(boost::format(
            "SELECT RESC_ID WHERE DATA_NAME = '%s' AND COLL_NAME = '%s' AND DATA_RESC_ID IN (%s)")
            % data_name % coll_name % _partial_list)};

        query<rcComm_t> qobj{_comm, qstr};
        bool skip = qobj.size() > 0;
        if(skip) {
            rodsLog(
                config_.data_transfer_log_level_value,
                "irods::storage_tiering - skipping migration for [%s] in resource list [%s]",
                _object_path.c_str(),
                _partial_list.c_str());
        }

        return skip;
    } // skip_object_in_lower_tier

    void storage_tiering::migrate_violating_data_objects(
        rcComm_t*          _comm,
        const std::string& _group_name,
        const std::string& _partial_list,
        const std::string& _source_resource,
        const std::string& _destination_resource) {
        using result_row = irods::query_processor<rcComm_t>::result_row;

        constexpr auto number_of_columns_required_from_query = 5;

        irods::thread_pool thread_pool{config_.number_of_scheduling_threads};
        try {
            std::map<std::string, uint8_t> object_is_processed;
            const bool preserve_replicas = get_preserve_replicas_for_resc(_comm, _source_resource);
            const auto query_limit       = get_object_limit_for_resource(_comm, _source_resource);
            const auto query_list        = get_violating_queries_for_resource(_comm, _source_resource);

            for(const auto& q_itr : query_list) {
                const auto  violating_query_type   = query<rcComm_t>::convert_string_to_query_type(q_itr.second);
                const auto& violating_query_string = q_itr.first;
                auto job = [&](const result_row& _results) {
                    rodsLog(
                        config_.data_transfer_log_level_value,
                        "found %ld objects for resc [%s] with query [%s] type [%d]",
                        _results.size(),
                        _source_resource.c_str(),
                        violating_query_string.c_str(),
                        violating_query_type);
                    if(_results.size() == 0) {
                        return;
                    }

                    // Log an error and continue if the violating query does not return exactly 5 items:
                    // DATA_NAME, COLL_NAME, USER_NAME, USER_ZONE, DATA_REPL_NUM
                    if (_results.size() != number_of_columns_required_from_query) {
                        rodsLog(LOG_ERROR,
                                fmt::format("Query on resource [{}] returned [{}] columns. Violating queries must "
                                            "select these 5 columns in order: [DATA_NAME, COLL_NAME, USER_NAME, "
                                            "USER_ZONE, DATA_REPL_NUM]. Violating query: [{}]",
                                            _source_resource,
                                            _results.size(),
                                            violating_query_string)
                                    .c_str());
                        return;
                    }

                    auto object_path = _results[1]; // coll name
                    const auto& vps  = get_virtual_path_separator();
                    if( !boost::ends_with(object_path, vps)) {
                        object_path += vps;
                    }
                    object_path += _results[0]; // data name

                    if(std::end(object_is_processed) !=
                       object_is_processed.find(object_path)) {
                        return;
                    }

                    object_is_processed[object_path] = 1;

                    auto proxy_conn = irods::proxy_connection();
                    rcComm_t* comm = proxy_conn.make(_results[2], _results[3]);

                    if(preserve_replicas) {
                        if(skip_object_in_lower_tier(
                               comm,
                               object_path,
                               _partial_list)) {
                            return;
                        }
                    }

                    queue_data_movement(
                        comm,
                        config_.instance_name,
                        _group_name,
                        object_path,
                        _results[2],
                        _results[3],
                        _results[4],
                        _source_resource,
                        _destination_resource,
                        get_verification_for_resc(comm, _destination_resource),
                        get_preserve_replicas_for_resc(comm, _source_resource),
                        get_data_movement_parameters_for_resource(comm, _source_resource));

                }; // job

                try {
                    irods::query_processor<rcComm_t> qp(violating_query_string, job, query_limit, violating_query_type);
                    auto future = qp.execute(thread_pool, *_comm);
                    auto errors = future.get();
                    if(errors.size() > 0) {
                        for(auto& e : errors) {
                            rodsLog(
                                LOG_ERROR,
                                "data movement scheduling failed - [%d]::[%s]",
                                std::get<0>(e),
                                std::get<1>(e).c_str());
                        }

                        THROW(
                            SYS_INVALID_OPR_TYPE,
                            boost::format(
                            "scheduling failed for [%d] objects for query [%s]")
                            % errors.size()
                            % violating_query_string.c_str());
                    }
                }
                catch(const exception& _e) {
                    // if nothing of interest is found, thats not an error
                    if(CAT_NO_ROWS_FOUND == _e.code()) {
                        rodsLog(
                            config_.data_transfer_log_level_value,
                            "no object found resc [%s] with query [%s] type [%d]",
                            _source_resource.c_str(),
                            violating_query_string.c_str(),
                            violating_query_type);

                        continue;
                    }
                    else {
                        irods::log(_e);
                    }
                }
            } // for qstr
        }
        catch(const std::out_of_range& _e) {
            THROW(
                SYS_INVALID_INPUT_PARAM,
                boost::format("out of bounds index for storage tiering query on resource [%s] : both DATA_NAME, COLL_NAME are required") %
                _source_resource);
        }
    } // migrate_violating_data_objects

    void storage_tiering::schedule_storage_tiering_policy(
        const std::string& _json,
        const std::string& _params) {
        const int delay_err = _delayExec(
                                  _json.c_str(),
                                  "",
                                  _params.c_str(),
                                  rei_);
        if(delay_err < 0) {
            THROW(
            delay_err,
            "delayExec failed");
        }

    } // schedule_storage_tiering_policy

    void storage_tiering::queue_data_movement(
        rcComm_t*          _comm,
        const std::string& _plugin_instance_name,
        const std::string& _group_name,
        const std::string& _object_path,
        const std::string& _user_name,
        const std::string& _user_zone,
        const std::string& _source_replica_number,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const std::string& _verification_type,
        const bool         _preserve_replicas,
        const std::string& _data_movement_params) {
        if(object_has_migration_metadata_flag(_comm, _user_name, _user_zone, _object_path)) {
            return;
        }

        set_migration_metadata_flag_for_object(_comm, _user_name, _user_zone, _object_path);

        nlohmann::json rule_obj =
        {
            {"policy_to_invoke", "irods_policy_enqueue_rule"}
          , {"parameters",
                {
                    {"rule-engine-operation",     policy::data_movement}
                  , {"rule-engine-instance-name", _plugin_instance_name}
                  , {"group-name",                _group_name}
                  , {"object-path",               _object_path}
                  , {"user-name",                 _user_name}
                  , {"user-zone",                 _user_zone}
                  , {"source-replica-number",     _source_replica_number}
                  , {"source-resource",           _source_resource}
                  , {"destination-resource",      _destination_resource}
                  , {"preserve-replicas",         _preserve_replicas}
                  , {"verification-type",         _verification_type}
                  , {"delay_conditions",          _data_movement_params}
                }
            }
         };

        execMyRuleInp_t exec_inp{};
        rstrcpy(exec_inp.myRule, rule_obj.dump().c_str(), META_STR_LEN);
        msParamArray_t* out_arr{};
        addKeyVal(
            &exec_inp.condInput
          , irods::KW_CFG_INSTANCE_NAME
          , "irods_rule_engine_plugin-cpp_default_policy-instance");

        auto err = rcExecMyRule(_comm, &exec_inp, &out_arr);

        if(err < 0) {
            THROW(
                err,
                boost::format("queue data movement failed for object [%s] from [%s] to [%s]") %
                _object_path %
                _source_resource %
                _destination_resource);
        }

        rodsLog(
            config_.data_transfer_log_level_value,
            "irods::storage_tiering migrating [%s] from [%s] to [%s]",
            _object_path.c_str(),
            _source_resource.c_str(),
            _destination_resource.c_str());

    } // queue_data_movement

    std::string storage_tiering::get_replica_number_for_resource(
        rcComm_t*          _comm,
        const std::string& _object_path,
        const std::string& _resource_name) {
        boost::filesystem::path p{_object_path};
        std::string coll_name = p.parent_path().string();
        std::string data_name = p.filename().string();

        std::string leaf_ids = get_leaf_resources_string(_resource_name);
        std::string qstr{boost::str(
            boost::format("SELECT DATA_REPL_NUM WHERE DATA_NAME = '%s' AND COLL_NAME = '%s' AND DATA_RESC_ID IN (%s)")
            % data_name
            % coll_name
            % leaf_ids)};

        query<rcComm_t> qobj{_comm, qstr};

        if(qobj.size() == 0) {
            THROW(
                CAT_NO_ROWS_FOUND,
                "failed to fetch user name and replica number");
        }

        return qobj.front()[0];

    } // get_replica_number_for_resource

    std::string storage_tiering::get_group_name_for_object(
        rcComm_t*          _comm,
        const std::string& _attribute_name,
        const std::string& _object_path) {
        boost::filesystem::path p{_object_path};
        std::string data_name = p.filename().string();
        std::string coll_name = p.parent_path().string();

        std::string qstr{boost::str(
            boost::format("SELECT META_DATA_ATTR_VALUE WHERE DATA_NAME = '%s' AND COLL_NAME = '%s' AND META_DATA_ATTR_NAME = '%s'")
            % data_name
            % coll_name
            % _attribute_name)};

        query<rcComm_t> qobj{_comm, qstr};

        if(qobj.size() == 0) {
            THROW(
                CAT_NO_ROWS_FOUND,
                "failed to fetch group name by object and resource");
        }

        return qobj.front()[0];

    } // get_group_name_for_object

    void storage_tiering::migrate_object_to_minimum_restage_tier(
        const std::string& _object_path,
        const std::string& _user_name,
        const std::string& _user_zone,
        const std::string& _source_resource) {

        try {
            const auto source_replica_number = get_replica_number_for_resource(
                                                   comm_,
                                                   _object_path,
                                                   _source_resource);
            const auto group_name = get_group_name_for_object(
                                        comm_,
                                        config_.group_attribute,
                                        _object_path);
            const auto low_tier_resource_name = get_restage_tier_resource_name(comm_, group_name);
            const auto source_resource_tier = get_group_tier_for_resource(comm_, _source_resource, group_name);
            const auto low_tier_resource_tier = get_group_tier_for_resource(comm_, low_tier_resource_name, group_name);

            // do not queue movement if data is on minimum tier or lower
            // TODO:: query for already queued movement?
            if (source_resource_tier <= low_tier_resource_tier) {
                rodsLog(
                    LOG_DEBUG,
                    fmt::format("Replica for object [{}] on resource [{}] (tier [{}]) already exists on the minimum "
                                "restage tier resource [{}] (tier [{}]) or an even lower tier. Skipping restage.",
                                _object_path,
                                _source_resource,
                                source_resource_tier,
                                low_tier_resource_name,
                                low_tier_resource_tier)
                        .c_str());
                return;
            }

            queue_data_movement(
                comm_,
                config_.instance_name,
                group_name,
                _object_path,
                _user_name,
                _user_zone,
                source_replica_number,
                _source_resource,
                low_tier_resource_name,
                get_verification_for_resc(comm_, low_tier_resource_name),
                get_preserve_replicas_for_resc(comm_, _source_resource),
                get_data_movement_parameters_for_resource(comm_, _source_resource));
        }
        catch(const exception& _e) {
            rodsLog(
                config_.data_transfer_log_level_value,
                "Failed to restage data object [%s] for resource [%s] Exception: [%s]",
                _object_path.c_str(),
                _source_resource.c_str(),
                _e.what());
        }
    } // migrate_object_to_minimum_restage_tier

    std::string storage_tiering::make_partial_list(
            resource_index_map::iterator _itr,
            resource_index_map::iterator _end) {
        ++_itr; // skip source resource

        std::string partial_list{};
        for(; _itr != _end; ++_itr) {
            partial_list += get_leaf_resources_string(_itr->second);
        }

        return partial_list;

    } // make_partial_list

    void storage_tiering::apply_policy_for_tier_group(
        const std::string& _group) {

        resource_index_map rescs = get_resource_map_for_group(
                                             comm_,
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
            const auto partial_list{make_partial_list(resc_itr, rescs.end())};
            auto next_itr = resc_itr;
            ++next_itr;
            if(rescs.end() == next_itr) {
                break;
            }
            migrate_violating_data_objects(
                comm_,
                _group,
                partial_list,
                resc_itr->second,
                next_itr->second);

        } // for resc

    } // apply_policy_for_tier_group

    void storage_tiering::set_migration_metadata_flag_for_object(
        rcComm_t*          _comm,
        const std::string& _user_name,
        const std::string& _user_zone,
        const std::string& _object_path) {
        auto access_time = get_metadata_for_data_object(
                               _comm,
                               config_.access_time_attribute,
                               _object_path);

        modAVUMetadataInp_t set_op{
           "set",
           "-d",
           const_cast<char*>(_object_path.c_str()),
           const_cast<char*>(config_.access_time_attribute.c_str()),
           const_cast<char*>(access_time.c_str()),
           const_cast<char*>(config_.migration_scheduled_flag.c_str())};

        auto status = exec_as_user(_comm, _user_name, _user_zone, [&set_op](auto comm) -> int {
                            return rcModAVUMetadata(comm, &set_op);
                            });
        if(status < 0) {
           THROW(
               status,
               boost::format("failed to set migration scheduled flag for [%s]")
               % _object_path);
        }
    } // set_migration_metadata_flag_for_object

    void storage_tiering::unset_migration_metadata_flag_for_object(
        rcComm_t*          _comm,
        const std::string& _user_name,
        const std::string& _user_zone,
        const std::string& _object_path) {
        auto access_time = get_metadata_for_data_object(
                               _comm,
                               config_.access_time_attribute,
                               _object_path);
        modAVUMetadataInp_t set_op{
           "set",
           "-d",
           const_cast<char*>(_object_path.c_str()),
           const_cast<char*>(config_.access_time_attribute.c_str()),
           const_cast<char*>(access_time.c_str()),
           nullptr};

        const auto status = exec_as_user(_comm, _user_name, _user_zone, [&set_op](auto comm) -> int {
                           return rcModAVUMetadata(comm, &set_op);
                           });
        if(status < 0) {
            THROW(
                status,
                boost::format("failed to unset migration scheduled flag for [%s]")
                % _object_path);
        }

    } // unset_migration_metadata_flag_for_object

    bool storage_tiering::object_has_migration_metadata_flag(
        rcComm_t*          _comm,
        const std::string& _user_name,
        const std::string& _user_zone,
        const std::string& _object_path) {
        boost::filesystem::path p{_object_path};
        std::string coll_name = p.parent_path().string();
        std::string data_name = p.filename().string();

        std::string query_str {
            boost::str(
                boost::format("SELECT META_DATA_ATTR_VALUE WHERE META_DATA_ATTR_NAME = '%s' and META_DATA_ATTR_UNITS = '%s' and DATA_NAME = '%s' AND COLL_NAME = '%s'")
                % config_.access_time_attribute
                % config_.migration_scheduled_flag
                % data_name
                % coll_name) };

        const auto status = exec_as_user(_comm, _user_name, _user_zone, [& query_str](auto& _comm) -> int {
                            query<rcComm_t> qobj{_comm, query_str, 1};
                            return qobj.size();
                           });

        return status > 0;

    } // object_has_migration_metadata_flag

    void storage_tiering::apply_tier_group_metadata_to_object(
        const std::string& _group_name,
        const std::string& _object_path,
        const std::string& _user_name,
        const std::string& _user_zone,
        const std::string& _source_replica_number,
        const std::string& _source_resource,
        const std::string& _destination_resource) {
        try {
            unset_migration_metadata_flag_for_object(comm_, _user_name, _user_zone, _object_path);
        }
        catch(const exception&) {
        }

        try {

            const auto destination_replica_number = get_replica_number_for_resource(
                                                        comm_,
                                                        _object_path,
                                                        _destination_resource);
            modAVUMetadataInp_t set_op{
                "set",
                "-d",
                const_cast<char*>(_object_path.c_str()),
                const_cast<char*>(config_.group_attribute.c_str()),
                const_cast<char*>(_group_name.c_str()),
                const_cast<char*>(destination_replica_number.c_str())};

            auto status = rcModAVUMetadata(comm_, &set_op);
            if(status < 0) {
                THROW(
                    status,
                    boost::format("failed to set tier group [%s] metadata for [%s]")
                    % _group_name
                    % _object_path);
            }
        }
        catch(const exception& _e) {
            THROW(
                _e.code(),
                _e.what());
        }

    } // apply_tier_group_metadata_to_object

}; // namespace irods

