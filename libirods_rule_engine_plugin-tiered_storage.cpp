// =-=-=-=-=-=-=-
// irods includes
#include "irods_re_plugin.hpp"
#include "irods_storage_tiering.hpp"

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

#include "json.hpp"

int _delayExec(
    const char *inActionCall,
    const char *recoveryActionCall,
    const char *delayCondition,
    ruleExecInfo_t *rei );

static std::string instance_name{};

irods::error start(
    irods::default_re_ctx&,
    const std::string& _instance_name ) {
    instance_name = _instance_name;
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
        irods::storage_tiering st(rei->rsComm, instance_name);
        std::set<std::string> access_time_rules{
                                  "pep_api_data_obj_close_post",
                                  "pep_api_data_obj_put_post",
                                  "pep_api_data_obj_get_post"};
        if(access_time_rules.find(_rn) != access_time_rules.end()) {
            st.apply_access_time(_args);
        }

        if(_rn == "pep_api_data_obj_get_post") {
            st.restage_object_to_lowest_tier(rei, _args);
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
            irods::storage_tiering st(rei->rsComm, instance_name);
            for(const auto& group : rule_obj["storage-tier-groups"]) {
                st.apply_storage_tiering_policy(group); 
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
            irods::storage_tiering st(rei->rsComm, instance_name);
            st.move_data_object(
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

