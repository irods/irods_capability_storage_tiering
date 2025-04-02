#include "irods/private/storage_tiering/data_verification_utilities.hpp"
#include "irods/private/storage_tiering/storage_tiering.hpp"
#include "irods/private/storage_tiering/utilities.hpp"

#include <irods/apiNumber.h>
#include <irods/client_connection.hpp>
#include <irods/closeCollection.h>
#include <irods/dataObjRepl.h>
#include <irods/dataObjTrim.h>
#include <irods/escape_utilities.hpp>
#include <irods/irods_at_scope_exit.hpp>
#include <irods/irods_logger.hpp>
#include <irods/irods_query.hpp>
#include <irods/irods_re_plugin.hpp>
#include <irods/irods_re_ruleexistshelper.hpp>
#include <irods/irods_resource_backport.hpp>
#include <irods/irods_rs_comm_query.hpp>
#include <irods/irods_server_api_call.hpp>
#include <irods/irods_virtual_path.hpp>
#include <irods/modAVUMetadata.h>
#include <irods/openCollection.h>
#include <irods/physPath.hpp>
#include <irods/rcMisc.h>
#include <irods/readCollection.h>

#define IRODS_FILESYSTEM_ENABLE_SERVER_SIDE_API
#include <irods/filesystem.hpp>

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

#include <nlohmann/json.hpp>

#include <irods/objDesc.hpp>
extern l1desc_t L1desc[NUM_L1_DESC];

int _delayExec(
    const char *inActionCall,
    const char *recoveryActionCall,
    const char *delayCondition,
    ruleExecInfo_t *rei );

namespace {
    using log_re = irods::experimental::log::rule_engine;

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

    auto resource_hierarchy_has_good_replica(RcComm* _comm,
                                             const std::string& _object_path,
                                             const std::string& _root_resource) -> bool
    {
        namespace fs = irods::experimental::filesystem;

        const auto object_path = fs::path{irods::single_quotes_to_hex(_object_path)};

        const auto query_string = fmt::format("select DATA_ID where DATA_NAME = '{0}' and COLL_NAME = '{1}' and "
                                              "DATA_RESC_HIER like '{2};%' || = '{2}' and DATA_REPL_STATUS = '1'",
                                              object_path.object_name().c_str(),
                                              object_path.parent_path().c_str(),
                                              _root_resource);

        const auto query = irods::query{_comm, query_string};

        return query.size() > 0;
    } // resource_hierarchy_has_good_replica

    void replicate_object_to_resource(
        rcComm_t*          _comm,
        const std::string& _instance_name,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const std::string& _object_path) {
        // If the destination resource has a good replica of the data object, skip replication.
        if (resource_hierarchy_has_good_replica(_comm, _object_path, _destination_resource)) {
            return;
        }

        dataObjInp_t data_obj_inp{};
        const auto free_cond_input = irods::at_scope_exit{[&data_obj_inp] { clearKeyVal(&data_obj_inp.condInput); }};
        rstrcpy(data_obj_inp.objPath, _object_path.c_str(), MAX_NAME_LEN);
        data_obj_inp.createMode = getDefFileMode();
        addKeyVal(&data_obj_inp.condInput, RESC_NAME_KW,      _source_resource.c_str());
        addKeyVal(&data_obj_inp.condInput, DEST_RESC_NAME_KW, _destination_resource.c_str());

        addKeyVal(&data_obj_inp.condInput, ADMIN_KW, "");

        transferStat_t* trans_stat{};
        const auto repl_err = rcDataObjRepl(_comm, &data_obj_inp);
        free(trans_stat);
        if(repl_err < 0) {
            THROW(repl_err,
                boost::format("failed to migrate [%s] to [%s]") %
                _object_path % _destination_resource);

        }
    } // replicate_object_to_resource

    void apply_data_retention_policy(
        rcComm_t*          _comm,
        const std::string& _instance_name,
        const std::string& _object_path,
        const std::string& _source_resource,
        const bool         _preserve_replicas) {
        if(_preserve_replicas) {
            return;
        }

        dataObjInp_t obj_inp{};
        const auto free_cond_input = irods::at_scope_exit{[&obj_inp] { clearKeyVal(&obj_inp.condInput); }};
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
        addKeyVal(&obj_inp.condInput, ADMIN_KW, "");

        const auto trim_err = rcDataObjTrim(_comm, &obj_inp);
        if(trim_err < 0) {
            THROW(
                trim_err,
                boost::format("failed to trim obj path [%s] from resource [%s]") %
                _object_path %
                _source_resource);
        }

    } // apply_data_retention_policy

    void update_access_time_for_data_object(rcComm_t* _comm,
                                            const std::string& _logical_path,
                                            const std::string& _attribute)
    {
        auto ts = std::to_string(std::time(nullptr));
        modAVUMetadataInp_t avuOp{
            "set",
            "-d",
            const_cast<char*>(_logical_path.c_str()),
            const_cast<char*>(_attribute.c_str()),
            const_cast<char*>(ts.c_str()),
            ""};
        const auto free_cond_input = irods::at_scope_exit{[&avuOp] { clearKeyVal(&avuOp.condInput); }};

        addKeyVal(&avuOp.condInput, ADMIN_KW, "");

        auto status = rcModAVUMetadata(_comm, &avuOp);
        if (status < 0) {
            const auto msg = fmt::format("{}: failed to set access time for [{}]", __func__, _logical_path);
            log_re::error(msg);
            THROW(status, msg);
        }
    } // update_access_time_for_data_object

    void apply_access_time_to_collection(rcComm_t* _comm, int _handle, const std::string& _attribute)
    {
        collEnt_t* coll_ent{nullptr};
        int err = rcReadCollection(_comm, _handle, &coll_ent);
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
                int handle = rcOpenCollection(_comm, &coll_inp);
                apply_access_time_to_collection(_comm, handle, _attribute);
                rcCloseCollection(_comm, handle);
            }

            err = rcReadCollection(_comm, _handle, &coll_ent);
        } // while
    } // apply_access_time_to_collection

    void set_access_time_metadata(
        rsComm_t*              _comm,
        const std::string& _object_path,
        const std::string& _collection_type,
        const std::string& _attribute) {
        irods::experimental::client_connection conn;
        RcComm& comm = static_cast<RcComm&>(conn);
        if(_collection_type.size() == 0) {
            update_access_time_for_data_object(&comm, _object_path, _attribute);
        }
        else {
            // register a collection
            collInp_t coll_inp;
            memset(&coll_inp, 0, sizeof(coll_inp));
            rstrcpy(
                coll_inp.collName,
                _object_path.c_str(),
                MAX_NAME_LEN);
            int handle = rcOpenCollection(&comm, &coll_inp);
            if(handle < 0) {
                THROW(
                    handle,
                    boost::format("failed to open collection [%s]") %
                    _object_path);
            }

            apply_access_time_to_collection(&comm, handle, _attribute);
        }
    } // set_access_time_metadata

    void apply_access_time_policy(
        const std::string&           _rn,
        ruleExecInfo_t*              _rei,
        const std::list<boost::any>& _args) {
        namespace fs = irods::experimental::filesystem;

        try {
            if ("pep_api_data_obj_put_post" == _rn || "pep_api_data_obj_get_post" == _rn ||
                "pep_api_data_obj_repl_post" == _rn || "pep_api_phy_path_reg_post" == _rn)
            {
                auto it = _args.begin();
                std::advance(it, 2);
                if(_args.end() == it) {
                    rodsLog(LOG_ERROR, "%s:%d: Invalid number of arguments [PEP=%s].", __func__, __LINE__, _rn.c_str());
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

                set_access_time_metadata(_rei->rsComm, object_path, coll_type, config->access_time_attribute);
            }
            else if ("pep_api_touch_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if (_args.end() == it) {
                    log_re::error("{}:{}: Invalid number of arguments [PEP={}].", __func__, __LINE__, _rn.c_str());
                    THROW(SYS_INVALID_INPUT_PARAM, "invalid number of arguments");
                }

                const auto* inp = boost::any_cast<BytesBuf*>(*it);
                const auto json_input = nlohmann::json::parse(std::string_view(static_cast<char*>(inp->buf), inp->len));

                const auto& object_path = json_input.at("logical_path").get_ref<const std::string&>();

                // The touch only affects the collection itself and does not access the objects or collections within.
                // Therefore, no access_time update occurs if the touch was on a collection. Just return early.
                if (fs::server::is_collection(*_rei->rsComm, fs::path{object_path})) {
                    return;
                }

                set_access_time_metadata(_rei->rsComm, object_path, "", config->access_time_attribute);
            }
            else if ("pep_api_data_obj_open_post" == _rn || "pep_api_data_obj_create_post" == _rn ||
                     "pep_api_replica_open_post" == _rn)
            {
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
                    auto [l1_idx, resource_name] = get_index_and_resource(obj_inp);
                    opened_objects[l1_idx] = std::make_tuple(obj_inp->objPath, resource_name);
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
                    auto [object_path, resource_name] = opened_objects[l1_idx];

                    set_access_time_metadata(_rei->rsComm, object_path, "", config->access_time_attribute);
                }
            }
            else if ("pep_api_replica_close_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if (_args.end() == it) {
                    THROW(SYS_INVALID_INPUT_PARAM, "invalid number of arguments");
                }

                const auto* inp = boost::any_cast<BytesBuf*>(*it);
                const auto json_input = nlohmann::json::parse(std::string_view(static_cast<char*>(inp->buf), inp->len));

                // replica_close can be called multiple times on the same data object when a parallel transfer is being
                // executed. Only one of these replica_close calls will finalize the status of the data object. The
                // finalizing occurs by default, so the caller must provide the "update_status" member with a value of
                // false in order to not finalize the data object. If no such member is found or it is not false, then
                // we update the access_time. Else, we do not want to update the access_time for each replica_close
                // call, so just return early if this is found.
                if (const auto update_status_iter = json_input.find("update_status");
                    json_input.end() != update_status_iter) {
                    if (const auto update_status = update_status_iter->get<bool>(); !update_status) {
                        return;
                    }
                }

                const auto l1_idx = json_input.at("fd").get<int>();
                const auto opened_objects_iter = opened_objects.find(l1_idx);
                if (opened_objects_iter != opened_objects.end()) {
                    auto [object_path, resource_name] = std::get<1>(*opened_objects_iter);
                    set_access_time_metadata(_rei->rsComm, object_path, "", config->access_time_attribute);
                }
            }
        } catch( const boost::bad_any_cast&) {
            // do nothing - no object to annotate
        }
        catch (const nlohmann::json::exception& e) {
            THROW(SYS_LIBRARY_ERROR, fmt::format("{}: JSON exception caught: {}", __func__, e.what()));
        }
    } // apply_access_time_policy

    int apply_data_movement_policy(
        rcComm_t*          _comm,
        const std::string& _instance_name,
        const std::string& _object_path,
        const std::string& _source_replica_number,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const bool         _preserve_replicas,
        const std::string& _verification_type) {

        replicate_object_to_resource(
            _comm,
            _instance_name,
            _source_resource,
            _destination_resource,
            _object_path);

        auto verified = irods::verify_replica_for_destination_resource(
                            _comm,
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
                _comm,
                _instance_name,
                _object_path,
                _source_resource,
                _preserve_replicas);

        return 0;
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

                irods::experimental::client_connection conn;
                RcComm& comm = static_cast<RcComm&>(conn);

                irods::storage_tiering st{&comm, _rei, plugin_instance_name};

                st.migrate_object_to_minimum_restage_tier(object_path, source_resource);
            }
            else if ("pep_api_data_obj_open_post" == _rn || "pep_api_replica_open_post" == _rn) {
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
                    auto [l1_idx, resource_name] = get_index_and_resource(obj_inp);
                    opened_objects[l1_idx] = std::make_tuple(obj_inp->objPath, resource_name);
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
                    auto [object_path, resource_name] = opened_objects[l1_idx];

                    irods::experimental::client_connection conn;
                    RcComm& comm = static_cast<RcComm&>(conn);

                    irods::storage_tiering st{&comm, _rei, plugin_instance_name};
                    st.migrate_object_to_minimum_restage_tier(object_path, resource_name);
                }
            }
            else if ("pep_api_replica_close_post" == _rn) {
                auto it = _args.begin();
                std::advance(it, 2);
                if (_args.end() == it) {
                    THROW(SYS_INVALID_INPUT_PARAM, "invalid number of arguments");
                }

                const auto* inp = boost::any_cast<BytesBuf*>(*it);
                const auto json_input = nlohmann::json::parse(std::string_view(static_cast<char*>(inp->buf), inp->len));

                // replica_close can be called multiple times on the same data object when a parallel transfer is being
                // executed. Only one of these replica_close calls will finalize the status of the data object. The
                // finalizing occurs by default, so the caller must provide the "update_status" member with a value of
                // false in order to not finalize the data object. If no such member is found or it is not false, then
                // we schedule the restage. Else, we do not want to schedule a restage for each replica_close call, so
                // just return early if this is found.
                if (const auto update_status_iter = json_input.find("update_status");
                    json_input.end() != update_status_iter) {
                    if (const auto update_status = update_status_iter->get<bool>(); !update_status) {
                        return;
                    }
                }

                const auto l1_idx = json_input.at("fd").get<int>();
                const auto opened_objects_iter = opened_objects.find(l1_idx);
                if (opened_objects_iter != opened_objects.end()) {
                    auto [object_path, resource_name] = std::get<1>(*opened_objects_iter);

                    irods::experimental::client_connection conn;
                    RcComm& comm = static_cast<RcComm&>(conn);

                    irods::storage_tiering st{&comm, _rei, plugin_instance_name};
                    st.migrate_object_to_minimum_restage_tier(object_path, resource_name);
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

    int apply_tier_group_metadata_policy(
        irods::storage_tiering& _st,
        const std::string& _group_name,
        const std::string& _object_path,
        const std::string& _source_replica_number,
        const std::string& _source_resource,
        const std::string& _destination_resource) {
        _st.apply_tier_group_metadata_to_object(
            _group_name,
            _object_path,
            _source_replica_number,
            _source_resource,
            _destination_resource);
        return 0;
    } // apply_tier_group_metadata_policy


} // namespace

auto setup(irods::default_re_ctx&, const std::string& _instance_name) -> irods::error
{
    plugin_instance_name = _instance_name;
    RuleExistsHelper::Instance()->registerRuleRegex("pep_api_.*");
    config = std::make_unique<irods::storage_tiering_configuration>(plugin_instance_name);
    return SUCCESS();
} // setup

auto teardown(irods::default_re_ctx&, const std::string&) -> irods::error
{
    return SUCCESS();
} // teardown

auto start(irods::default_re_ctx&, const std::string&) -> irods::error
{
    return SUCCESS();
} // start

auto stop(irods::default_re_ctx&, const std::string&) -> irods::error
{
    return SUCCESS();
} // stop

irods::error rule_exists(
    irods::default_re_ctx&,
    const std::string& _rn,
    bool&              _ret) {
    const std::set<std::string> rules{"pep_api_data_obj_close_post",
                                      "pep_api_data_obj_create_post",
                                      "pep_api_data_obj_get_post",
                                      "pep_api_data_obj_open_post",
                                      "pep_api_data_obj_put_post",
                                      "pep_api_data_obj_repl_post",
                                      "pep_api_phy_path_reg_post",
                                      "pep_api_replica_close_post",
                                      "pep_api_replica_open_post",
                                      "pep_api_touch_post"};
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
    using json = nlohmann::json;

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

    ruleExecInfo_t* rei{};
    if (const auto err = _eff_hdlr("unsafe_ms_ctx", &rei); !err.ok()) {
        return err;
    }

    if (!rei || !rei->rsComm) {
        return ERROR(SYS_INTERNAL_NULL_INPUT_ERR, fmt::format("{}: null rei or RsComm in storage tiering.", __func__));
    }

    if (!irods::is_privileged_client(*rei->rsComm)) {
        const auto msg = fmt::format("{}: Only rodsadmins are allowed to execute storage tiering rules.", __func__);
        constexpr auto err = CAT_INSUFFICIENT_PRIVILEGE_LEVEL;
        addRErrorMsg(&rei->rsComm->rError, err, msg.c_str());
        return ERROR(err, msg);
    }

    try {
        // skip the first line: @external
        std::string rule_text{_rule_text};
        if(_rule_text.find("@external") != std::string::npos) {
            rule_text = _rule_text.substr(10);
        }
        const auto rule_obj = json::parse(rule_text);
        const auto& rule_engine_instance_name = rule_obj.at("rule-engine-instance-name").get_ref<const std::string&>();
        // if the rule text does not have our instance name, fail
        if(plugin_instance_name != rule_engine_instance_name) {
            return ERROR(
                    SYS_NOT_SUPPORTED,
                    "instance name not found");
        }

        if (const auto& rule_engine_operation = rule_obj.at("rule-engine-operation").get_ref<const std::string&>();
            irods::storage_tiering::schedule::storage_tiering == rule_engine_operation)
        {
            ruleExecInfo_t* rei{};
            const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);
            if(!err.ok()) {
                return err;
            }

            const auto& params = rule_obj.at("delay-parameters").get_ref<const std::string&>();

            json delay_obj;
            delay_obj["rule-engine-operation"] = irods::storage_tiering::policy::storage_tiering;

            const auto& storage_tier_groups = rule_obj.at("storage-tier-groups").get_ref<const json::array_t&>();
            delay_obj["storage-tier-groups"] = storage_tier_groups;

            irods::experimental::client_connection conn;
            RcComm& comm = static_cast<RcComm&>(conn);

            irods::storage_tiering st{&comm, rei, plugin_instance_name};
            st.schedule_storage_tiering_policy(delay_obj.dump(), params);
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
    catch(const json::exception& _e) {
        return ERROR(
                SYS_INVALID_INPUT_PARAM,
                "exec_rule_text - caught json exception");
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
        const auto rule_engine_operation_iter = rule_obj.find("rule-engine-operation");
        if (rule_obj.end() == rule_engine_operation_iter) {
            return CODE(RULE_ENGINE_CONTINUE);
        }

        const auto& rule_engine_operation = rule_engine_operation_iter->get_ref<const std::string&>();
        if (irods::storage_tiering::policy::storage_tiering == rule_engine_operation) {
            try {
                irods::experimental::client_connection conn;
                RcComm& comm = static_cast<RcComm&>(conn);

                irods::storage_tiering st{&comm, rei, plugin_instance_name};
                for (const auto& group : rule_obj.at("storage-tier-groups").get_ref<const json::array_t&>()) {
                    st.apply_policy_for_tier_group(group);
                }
            }
            catch(const irods::exception& _e) {
                printErrorStack(&rei->rsComm->rError);
                return ERROR(
                        _e.code(),
                        _e.what());
            }
        }
        else if (irods::storage_tiering::policy::data_movement == rule_engine_operation) {
            try {
                const auto& object_path = rule_obj.at("object-path").get_ref<const std::string&>();
                const auto& source_replica_number = rule_obj.at("source-replica-number").get_ref<const std::string&>();
                const auto& source_resource = rule_obj.at("source-resource").get_ref<const std::string&>();
                const auto& destination_resource = rule_obj.at("destination-resource").get_ref<const std::string&>();
                const auto preserve_replicas = rule_obj.at("preserve-replicas").get<bool>();
                const auto& verification_type = rule_obj.at("verification-type").get_ref<const std::string&>();

                irods::experimental::client_connection conn;
                RcComm& comm = static_cast<RcComm&>(conn);

                auto status = apply_data_movement_policy(&comm,
                                                         plugin_instance_name,
                                                         object_path,
                                                         source_replica_number,
                                                         source_resource,
                                                         destination_resource,
                                                         preserve_replicas,
                                                         verification_type);

                irods::storage_tiering st{&comm, rei, plugin_instance_name};

                const auto& group_name = rule_obj.at("group-name").get_ref<const std::string&>();
                status = apply_tier_group_metadata_policy(
                    st, group_name, object_path, source_replica_number, source_resource, destination_resource);
            }
            catch(const irods::exception& _e) {
                printErrorStack(&rei->rsComm->rError);
                return ERROR(
                        _e.code(),
                        _e.what());
            }
        }
        else {
            return CODE(RULE_ENGINE_CONTINUE);
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
    catch(const json::exception& _e) {
        return ERROR(
                SYS_INVALID_INPUT_PARAM,
                "exec_rule_expression - caught json exception");
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

    re->add_operation<irods::default_re_ctx&, const std::string&>("setup", std::function(setup));

    re->add_operation<irods::default_re_ctx&, const std::string&>("teardown", std::function(teardown));

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

