// nclude "irods_re_ruleexistshelper.hpp"
// =-=-=-=-=-=-=-
// irods includes
#include "irods_re_plugin.hpp"
#include "irods_storage_tiering.hpp"
#include "irods_re_ruleexistshelper.hpp"
#include "storage_tiering_utilities.hpp"

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

int _delayExec(
    const char *inActionCall,
    const char *recoveryActionCall,
    const char *delayCondition,
    ruleExecInfo_t *rei );

namespace {
    std::unique_ptr<irods::storage_tiering_configuration> config;
    std::string plugin_instance_name{};

    void apply_access_time_policy(
        ruleExecInfo_t*              _rei,
        const std::list<boost::any>& _args) {
        auto it = _args.begin();
        std::advance(it, 2);
        if(_args.end() == it) {
            THROW(
                SYS_INVALID_INPUT_PARAM,
                "invalid number of arguments");
        }

        try {
            auto obj_inp = boost::any_cast<dataObjInp_t*>(*it);
            const char* coll_type_ptr = getValByKey(&obj_inp->condInput, COLLECTION_KW);

            std::string obj_path{obj_inp->objPath};
            std::string coll_type{};
            if(coll_type_ptr) {
                coll_type = "true";
            }

            std::list<boost::any> args;
            args.push_back(boost::any(obj_path));
            args.push_back(boost::any(coll_type));
            args.push_back(boost::any(config->access_time_attribute));
            irods::invoke_policy(_rei, irods::storage_tiering::policy::access_time, args);
        } catch( const boost::bad_any_cast&) {
            // do nothing - no object to annotate
        }
    } // apply_access_time_policy

    void apply_data_movement_policy(
        ruleExecInfo_t*    _rei,
        const bool         _preserve_replicas,
        const std::string& _verification_type,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const std::string& _object_path) {

        std::list<boost::any> args;
        args.push_back(boost::any(_preserve_replicas));
        args.push_back(boost::any(_verification_type));
        args.push_back(boost::any(_source_resource));
        args.push_back(boost::any(_destination_resource));
        args.push_back(boost::any(_object_path));
        irods::invoke_policy(_rei, irods::storage_tiering::policy::data_movement, args);

    } // apply_data_movement_policy

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
                                    "pep_api_data_obj_put_post",
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
        apply_access_time_policy(rei, _args);

        if("pep_api_data_obj_get_post" == _rn) {
            irods::storage_tiering st{rei, plugin_instance_name};
            st.migrate_object_to_minimum_restage_tier(_args);
        }
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

    return err;

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
} // exec_rule_text

irods::error exec_rule_expression(
    irods::default_re_ctx&,
    const std::string&     _rule_text,
    msParamArray_t*        _ms_params,
    irods::callback        _eff_hdlr) {
    using json = nlohmann::json;

    try {
        const auto rule_obj = json::parse(_rule_text);

        if(irods::storage_tiering::policy::storage_tiering == rule_obj["rule-engine-operation"]) {
            ruleExecInfo_t* rei{};
            const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);
            if(!err.ok()) {
                return err;
            }

            try {
                irods::storage_tiering st{rei, plugin_instance_name};
                for(const auto& group : rule_obj["storage-tier-groups"]) {
                    st.apply_policy_for_tier_group(group);
                }
            }
            catch(const irods::exception& _e) {
                return ERROR(
                        _e.code(),
                        _e.what());
            }
        }
        else if(irods::storage_tiering::policy::data_movement == rule_obj["rule-engine-operation"]) {
            ruleExecInfo_t* rei{};
            const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);
            if(!err.ok()) {
                return err;
            }

            try {
                apply_data_movement_policy(
                    rei,
                    rule_obj["preserve-replicas"],
                    rule_obj["verification-type"],
                    rule_obj["source-resource"],
                    rule_obj["destination-resource"],
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

