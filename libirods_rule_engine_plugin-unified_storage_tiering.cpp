// =-=-=-=-=-=-=-
// irods includes
#include "irods_re_plugin.hpp"
#include "storage_tiering.hpp"
#include "irods_re_ruleexistshelper.hpp"
#include "storage_tiering_utilities.hpp"
#include "irods_resource_backport.hpp"

#include "rsModAVUMetadata.hpp"
#include "rsOpenCollection.hpp"
#include "rsReadCollection.hpp"
#include "rsCloseCollection.hpp"
#include "irods_virtual_path.hpp"
#include "rsDataObjRepl.hpp"
#include "rsDataObjTrim.hpp"
#include "physPath.hpp"
#include "apiNumber.h"
#include "data_verification_utilities.hpp"
#include "irods_server_api_call.hpp"

#undef LIST

// =-=-=-=-=-=-=-
// stl includes
#include <iostream>
#include <sstream>
#include <vector>
#include <string>

// =-=-=-=-=-=-=-
// boost includes
#include <boost/any.hpp>
#include <boost/exception/all.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/optional.hpp>

#include "json.hpp"

#include "objDesc.hpp"
extern l1desc_t L1desc[NUM_L1_DESC];

int _delayExec(
    const char *inActionCall,
    const char *recoveryActionCall,
    const char *delayCondition,
    ruleExecInfo_t *rei );

namespace {
    std::unique_ptr<irods::storage_tiering_configuration> config;
    std::map<int, std::tuple<std::string, std::string>> opened_objects;
    std::string plugin_instance_name{};

    std::tuple<int, std::string>
    get_index_and_resource(const dataObjInp_t* _inp) {
        int l1_idx{};
        dataObjInfo_t* obj_info{};
        for(const auto& l1 : L1desc) {
            if(FD_INUSE != l1.inuseFlag) {
                continue;
            }
            if(!strcmp(l1.dataObjInp->objPath, _inp->objPath)) {
                obj_info = l1.dataObjInfo;
                l1_idx = &l1 - L1desc;
            }
        }

        if(nullptr == obj_info) {
            THROW(
                SYS_INVALID_INPUT_PARAM,
                "no object found");
        }

        std::string resource_name;
        irods::error err = irods::get_resource_property<std::string>(
                               obj_info->rescId,
                               irods::RESOURCE_NAME,
                               resource_name);
        if(!err.ok()) {
            THROW(err.code(), err.result());
        }

        return std::make_tuple(l1_idx, resource_name);
    } // get_index_and_resource

    void replicate_object_to_resource(
        rsComm_t*          _comm,
        const std::string& _instance_name,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const std::string& _object_path) {

        dataObjInp_t data_obj_inp{};
        rstrcpy(data_obj_inp.objPath, _object_path.c_str(), MAX_NAME_LEN);
        data_obj_inp.createMode = getDefFileMode();
        addKeyVal(&data_obj_inp.condInput, RESC_NAME_KW,      _source_resource.c_str());
        addKeyVal(&data_obj_inp.condInput, DEST_RESC_NAME_KW, _destination_resource.c_str());

        if(_comm->clientUser.authInfo.authFlag >= LOCAL_PRIV_USER_AUTH) {
            addKeyVal(&data_obj_inp.condInput, ADMIN_KW, "true" );
        }

        transferStat_t* trans_stat{};
        const auto repl_err = irods::server_api_call(DATA_OBJ_REPL_AN, _comm, &data_obj_inp, &trans_stat);
        free(trans_stat);
        if(repl_err < 0) {
            THROW(repl_err,
                boost::format("failed to migrate [%s] to [%s]") %
                _object_path % _destination_resource);

        }
    } // replicate_object_to_resource

    void apply_data_retention_policy(
        ruleExecInfo_t*    _rei,
        const std::string& _instance_name,
        const std::string& _object_path,
        const std::string& _source_resource,
        const bool         _preserve_replicas) {
        if(_preserve_replicas) {
            return;
        }

        rsComm_t* comm = _rei->rsComm;

        dataObjInp_t obj_inp{};
        rstrcpy(
            obj_inp.objPath,
            _object_path.c_str(),
            sizeof(obj_inp.objPath));
        addKeyVal(
            &obj_inp.condInput,
            RESC_NAME_KW,
            _source_resource.c_str());
        addKeyVal(
            &obj_inp.condInput,
            COPIES_KW,
            "1");
        if(comm->clientUser.authInfo.authFlag >= LOCAL_PRIV_USER_AUTH) {
            addKeyVal(
                &obj_inp.condInput,
                ADMIN_KW,
                "true" );
        }

        const auto trim_err = rsDataObjTrim(comm, &obj_inp);
        if(trim_err < 0) {
            THROW(
                trim_err,
                boost::format("failed to trim obj path [%s] from resource [%s]") %
                _object_path %
                _source_resource);
        }

    } // apply_data_retention_policy

    void update_access_time_for_data_object(
        rsComm_t*          _comm,
        const std::string& _logical_path,
        const std::string& _attribute) {

        auto ts = std::to_string(std::time(nullptr));
        modAVUMetadataInp_t avuOp{
            "set",
            "-d",
            const_cast<char*>(_logical_path.c_str()),
            const_cast<char*>(_attribute.c_str()),
            const_cast<char*>(ts.c_str()),
            ""};

        auto status = rsModAVUMetadata(_comm, &avuOp);
        if(status < 0) {
            THROW(
                status,
                boost::format("failed to set access time for [%s]") %
                _logical_path);
        }
    } // update_access_time_for_data_object

    void apply_access_time_to_collection(
        rsComm_t*          _comm,
        int                _handle,
        const std::string& _attribute) {
        collEnt_t* coll_ent{nullptr};
        int err = rsReadCollection(_comm, &_handle, &coll_ent);
        while(err >= 0) {
            if(DATA_OBJ_T == coll_ent->objType) {
                const auto& vps = irods::get_virtual_path_separator();
                std::string lp{coll_ent->collName};
                lp += vps;
                lp += coll_ent->dataName;
                update_access_time_for_data_object(_comm, lp, _attribute);
            }
            else if(COLL_OBJ_T == coll_ent->objType) {
                collInp_t coll_inp;
                memset(&coll_inp, 0, sizeof(coll_inp));
                rstrcpy(
                    coll_inp.collName,
                    coll_ent->collName,
                    MAX_NAME_LEN);
                int handle = rsOpenCollection(_comm, &coll_inp);
                apply_access_time_to_collection(_comm, handle, _attribute);
                rsCloseCollection(_comm, &handle);
            }

            err = rsReadCollection(_comm, &_handle, &coll_ent);
        } // while
    } // apply_access_time_to_collection

    void set_access_time_metadata(
        rsComm_t*              _comm,
        const std::string& _object_path,
        const std::string& _collection_type,
        const std::string& _attribute) {
        if(_collection_type.size() == 0) {
            update_access_time_for_data_object(_comm, _object_path, _attribute);
        }
        else {
            // register a collection
            collInp_t coll_inp;
            memset(&coll_inp, 0, sizeof(coll_inp));
            rstrcpy(
                coll_inp.collName,
                _object_path.c_str(),
                MAX_NAME_LEN);
            int handle = rsOpenCollection(
                             _comm,
                             &coll_inp);
            if(handle < 0) {
                THROW(
                    handle,
                    boost::format("failed to open collection [%s]") %
                    _object_path);
            }

            apply_access_time_to_collection(_comm, handle, _attribute);
        }
    } // set_access_time_metadata

    void apply_access_time_policy(
        const std::string&           _rn,
        ruleExecInfo_t*              _rei,
        const std::list<boost::any>& _args) {

        try {
            if("pep_api_data_obj_put_post"  == _rn ||
               "pep_api_data_obj_repl_post" == _rn ||
               "pep_api_phy_path_reg_post"  == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
                const char* coll_type_ptr = getValByKey(&obj_inp->condInput, COLLECTION_KW);

                std::string object_path{obj_inp->objPath};
                std::string coll_type{};
                if(coll_type_ptr) {
                    coll_type = "true";
                }

                set_access_time_metadata(
                    _rei->rsComm,
                    object_path,
                    coll_type,
                    config->access_time_attribute);
            }
            else if("pep_api_data_obj_open_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
                int l1_idx{};
                std::string resource_name;
                try {
                    std::tie(l1_idx, resource_name) = get_index_and_resource(obj_inp);
                    opened_objects[l1_idx] = std::tie(obj_inp->objPath, resource_name);
                }
                catch(const irods::exception& _e) {
                    rodsLog(
                       LOG_ERROR,
                       "get_index_and_resource failed for [%s]",
                       obj_inp->objPath);
                }
            }
            else if("pep_api_data_obj_create_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
                int l1_idx{};
                std::string resource_name;
                try {
                    std::tie(l1_idx, resource_name) = get_index_and_resource(obj_inp);
                    opened_objects[l1_idx] = std::tie(obj_inp->objPath, resource_name);
                }
                catch(const irods::exception& _e) {
                    rodsLog(
                       LOG_ERROR,
                       "get_index_and_resource failed for [%s]",
                       obj_inp->objPath);
                }
            }
            else if("pep_api_data_obj_close_post" == _rn) {
                //TODO :: only for create/write events
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                const auto opened_inp = boost::any_cast<openedDataObjInp_t*>(*it);
                const auto l1_idx = opened_inp->l1descInx;
                if(opened_objects.find(l1_idx) != opened_objects.end()) {
                    std::string object_path, resource_name;
                    std::tie(object_path, resource_name) = opened_objects[l1_idx];

                    set_access_time_metadata(
                        _rei->rsComm,
                        object_path,
                        "",
                        config->access_time_attribute);
                }
            }
        } catch( const boost::bad_any_cast&) {
            // do nothing - no object to annotate
        }
    } // apply_access_time_policy

    void apply_data_movement_policy(
        ruleExecInfo_t*    _rei,
        const std::string& _instance_name,
        const std::string& _object_path,
        const std::string& _user_name,
        const std::string& _source_replica_number,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const bool         _preserve_replicas,
        const std::string& _verification_type) {

        replicate_object_to_resource(
            _rei->rsComm,
            _instance_name,
            _source_resource,
            _destination_resource,
            _object_path);

        auto verified = irods::verify_replica_for_destination_resource(
                            _rei->rsComm,
                            _instance_name,
                            _verification_type,
                            _object_path,
                            _source_resource,
                            _destination_resource);
        if(!verified) {
            THROW(
                UNMATCHED_KEY_OR_INDEX,
                boost::format("verification failed for [%s] on [%s]")
                % _object_path
                % _destination_resource);
        }

        apply_data_retention_policy(
                _rei,
                _instance_name,
                _object_path,
                _source_resource,
                _preserve_replicas);

    } // apply_data_movement_policy

    void apply_restage_movement_policy(
        const std::string &    _rn,
        ruleExecInfo_t*        _rei,
        std::list<boost::any>& _args) {
        try {
            std::string object_path;
            std::string source_resource;
            // NOTE:: 3rd parameter is the target
            if("pep_api_data_obj_get_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
                object_path = obj_inp->objPath;
                const char* source_hier = getValByKey(
                                              &obj_inp->condInput,
                                              RESC_HIER_STR_KW);
                if(!source_hier) {
                    THROW(SYS_INVALID_INPUT_PARAM, "resc hier is null");
                }

                irods::hierarchy_parser parser;
                parser.set_string(source_hier);
                parser.first_resc(source_resource);

                irods::storage_tiering st{_rei, plugin_instance_name};
                st.migrate_object_to_minimum_restage_tier(
                    object_path,
                    _rei->rsComm->clientUser.userName,
                    source_resource);
            }
            else if("pep_api_data_obj_open_post"   == _rn ||
                    "pep_api_data_obj_create_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
                int l1_idx{};
                std::string resource_name;
                try {
                    std::tie(l1_idx, resource_name) = get_index_and_resource(obj_inp);
                    opened_objects[l1_idx] = std::tie(obj_inp->objPath, resource_name);
                }
                catch(const irods::exception& _e) {
                    rodsLog(
                       LOG_ERROR,
                       "get_index_and_resource failed for [%s]",
                       obj_inp->objPath);
                }
            }
            else if("pep_api_data_obj_close_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    THROW(
                        SYS_INVALID_INPUT_PARAM,
                        "invalid number of arguments");
                }

                const auto opened_inp = boost::any_cast<openedDataObjInp_t*>(*it);
                const auto l1_idx = opened_inp->l1descInx;
                if(opened_objects.find(l1_idx) != opened_objects.end()) {
                    std::string object_path, resource_name;
                    std::tie(object_path, resource_name) = opened_objects[l1_idx];

                    irods::storage_tiering st{_rei, plugin_instance_name};
                    st.migrate_object_to_minimum_restage_tier(
                        object_path,
                        _rei->rsComm->clientUser.userName,
                        resource_name);
                }
            }
        }
        catch(const boost::bad_any_cast& _e) {
            THROW(
                INVALID_ANY_CAST,
                boost::str(boost::format(
                    "function [%s] rule name [%s]")
                    % __FUNCTION__ % _rn));
        }
    } // apply_restage_movement_policy

    void apply_tier_group_metadata_policy(
        irods::storage_tiering& _st,
        const std::string& _group_name,
        const std::string& _object_path,
        const std::string& _user_name,
        const std::string& _source_replica_number,
        const std::string& _source_resource,
        const std::string& _destination_resource) {
        _st.apply_tier_group_metadata_to_object(
            _group_name,
            _object_path,
            _user_name,
            _source_replica_number,
            _source_resource,
            _destination_resource);
    } // apply_tier_group_metadata_policy


} // namespace


irods::error start(
    irods::default_re_ctx&,
    const std::string& _instance_name ) {
    plugin_instance_name = _instance_name;
    RuleExistsHelper::Instance()->registerRuleRegex("pep_api_.*");
    config = std::make_unique<irods::storage_tiering_configuration>(plugin_instance_name);
    return SUCCESS();
} // start

irods::error stop(
    irods::default_re_ctx&,
    const std::string& ) {
    return SUCCESS();
} // stop

irods::error rule_exists(
    irods::default_re_ctx&,
    const std::string& _rn,
    bool&              _ret) {
    const std::set<std::string> rules{
                                    "pep_api_data_obj_create_post",
                                    "pep_api_data_obj_open_post",
                                    "pep_api_data_obj_close_post",
                                    "pep_api_data_obj_put_post",
                                    "pep_api_data_obj_repl_post",
                                    "pep_api_data_obj_get_post",
                                    "pep_api_phy_path_reg_post"};
    _ret = rules.find(_rn) != rules.end();

    return SUCCESS();
} // rule_exists

irods::error list_rules(irods::default_re_ctx&, std::vector<std::string>&) {
    return SUCCESS();
} // list_rules

irods::error exec_rule(
    irods::default_re_ctx&,
    const std::string&     _rn,
    std::list<boost::any>& _args,
    irods::callback        _eff_hdlr) {
    ruleExecInfo_t* rei{};
    const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);
    if(!err.ok()) {
        return err;
    }

    try {
        apply_access_time_policy(_rn, rei, _args);
        apply_restage_movement_policy(_rn, rei, _args);
    }
    catch(const  std::invalid_argument& _e) {
        irods::exception_to_rerror(
            SYS_NOT_SUPPORTED,
            _e.what(),
            rei->rsComm->rError);
        return ERROR(
                   SYS_NOT_SUPPORTED,
                   _e.what());
    }
    catch(const std::domain_error& _e) {
        irods::exception_to_rerror(
            INVALID_ANY_CAST,
            _e.what(),
            rei->rsComm->rError);
        return ERROR(
                   SYS_NOT_SUPPORTED,
                   _e.what());
    }
    catch(const irods::exception& _e) {
        irods::exception_to_rerror(
            _e,
            rei->rsComm->rError);
        return irods::error(_e);
    }

    return CODE(RULE_ENGINE_CONTINUE);
} // exec_rule

irods::error exec_rule_text(
    irods::default_re_ctx&,
    const std::string&     _rule_text,
    msParamArray_t*        _ms_params,
    const std::string&     _out_desc,
    irods::callback        _eff_hdlr) {
    using json = nlohmann::json;

    try {
        // skip the first line: @external
        std::string rule_text{_rule_text};
        if(_rule_text.find("@external") != std::string::npos) {
            rule_text = _rule_text.substr(10);
        }
        const auto rule_obj = json::parse(rule_text);
        const std::string& rule_engine_instance_name = rule_obj["rule-engine-instance-name"];
        // if the rule text does not have our instance name, fail
        if(plugin_instance_name != rule_engine_instance_name) {
            return ERROR(
                    SYS_NOT_SUPPORTED,
                    "instance name not found");
        }

        if(irods::storage_tiering::schedule::storage_tiering ==
           rule_obj["rule-engine-operation"]) {
            ruleExecInfo_t* rei{};
            const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);
            if(!err.ok()) {
                return err;
            }

            const std::string& params = rule_obj["delay-parameters"];

            json delay_obj;
            delay_obj["rule-engine-operation"] = irods::storage_tiering::policy::storage_tiering;
            delay_obj["storage-tier-groups"]   = rule_obj["storage-tier-groups"];

            irods::storage_tiering st{rei, plugin_instance_name};
            st.schedule_storage_tiering_policy(
                delay_obj.dump(),
                params);
        }
        else {
            return ERROR(
                    SYS_NOT_SUPPORTED,
                    "supported rule name not found");
        }
    }
    catch(const  std::invalid_argument& _e) {
        std::string msg{"Rule text is not valid JSON -- "};
        msg += _e.what();
        return ERROR(
                   SYS_NOT_SUPPORTED,
                   msg);
    }
    catch(const std::domain_error& _e) {
        std::string msg{"Rule text is not valid JSON -- "};
        msg += _e.what();
        return ERROR(
                   SYS_NOT_SUPPORTED,
                   msg);
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
    ruleExecInfo_t* rei{};
    const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);
    if(!err.ok()) {
        return err;
    }

    try {
        const auto rule_obj = json::parse(_rule_text);
        if(irods::storage_tiering::policy::storage_tiering ==
           rule_obj["rule-engine-operation"]) {
            try {
                irods::storage_tiering st{rei, plugin_instance_name};
                for(const auto& group : rule_obj["storage-tier-groups"]) {
                    st.apply_policy_for_tier_group(*rei->rsComm, group);
                }
            }
            catch(const irods::exception& _e) {
                printErrorStack(&rei->rsComm->rError);
                return ERROR(
                        _e.code(),
                        _e.what());
            }
        }
        else if(irods::storage_tiering::policy::data_movement ==
                rule_obj["rule-engine-operation"]) {
            try {
                // proxy for provided user name
                const std::string& user_name = rule_obj["user-name"];
                rstrcpy(
                    rei->rsComm->clientUser.userName,
                    user_name.c_str(),
                    NAME_LEN);

                apply_data_movement_policy(
                    rei,
                    plugin_instance_name,
                    rule_obj["object-path"],
                    rule_obj["user-name"],
                    rule_obj["source-replica-number"],
                    rule_obj["source-resource"],
                    rule_obj["destination-resource"],
                    rule_obj["preserve-replicas"],
                    rule_obj["verification-type"]);

                irods::storage_tiering st{rei, plugin_instance_name};
                apply_tier_group_metadata_policy(
                    st,
                    rule_obj["group-name"],
                    rule_obj["object-path"],
                    rule_obj["user-name"],
                    rule_obj["source-replica-number"],
                    rule_obj["source-resource"],
                    rule_obj["destination-resource"]);
            }
            catch(const irods::exception& _e) {
                printErrorStack(&rei->rsComm->rError);
                return ERROR(
                        _e.code(),
                        _e.what());
            }

        }
        else {
            printErrorStack(&rei->rsComm->rError);
            return ERROR(
                    SYS_NOT_SUPPORTED,
                    "supported rule name not found");
        }
    }
    catch(const  std::invalid_argument& _e) {
        return ERROR(
                   SYS_NOT_SUPPORTED,
                   _e.what());
    }
    catch(const std::domain_error& _e) {
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

} // exec_rule_expression

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

