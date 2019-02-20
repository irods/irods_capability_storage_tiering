
#include "irods_query.hpp"
#include "irods_re_plugin.hpp"
#include "irods_re_ruleexistshelper.hpp"
#include "storage_tiering_utilities.hpp"

#include "rsDataObjTrim.hpp"
#include "physPath.hpp"

#include <boost/any.hpp>
#include <sstream>

namespace {
    void apply_data_replication_policy(
        ruleExecInfo_t*    _rei,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const std::string& _object_path) {
        std::list<boost::any> args;
        args.push_back(boost::any(_source_resource));
        args.push_back(boost::any(_destination_resource));
        args.push_back(boost::any(_object_path));
        irods::invoke_policy(_rei, "irods_policy_data_replication", args);

    } // apply_data_replication_policy

    void apply_data_verification_policy(
        ruleExecInfo_t*    _rei,
        const std::string& _verification_type,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const std::string& _object_path) {
        std::list<boost::any> args;
        args.push_back(boost::any(_verification_type));
        args.push_back(boost::any(_source_resource));
        args.push_back(boost::any(_destination_resource));
        args.push_back(boost::any(_object_path));
        irods::invoke_policy(_rei, "irods_policy_data_verification", args);

    } // apply_data_verification_policy

    void apply_data_retention_policy(
        ruleExecInfo_t*    _rei,
        const bool         _preserve_replicas,
        const std::string& _source_resource,
        const std::string& _object_path) {
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

    void apply_data_movement_policy(
        ruleExecInfo_t*    _rei,
        const bool         _preserve_replicas,
        const std::string& _verification_type,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const std::string& _object_path) {
        apply_data_replication_policy(
                _rei,
                _source_resource,
                _destination_resource,
                _object_path);

        apply_data_verification_policy(
                _rei,
                _verification_type,
                _source_resource,
                _destination_resource,
                _object_path);

        apply_data_retention_policy(
                _rei,
                _preserve_replicas,
                _source_resource,
                _object_path);
    } // apply_data_movement_policy

} // namespace

std::string plugin_instance_name{};

irods::error start(
    irods::default_re_ctx&,
    const std::string& _instance_name ) {
    plugin_instance_name = _instance_name;
    RuleExistsHelper::Instance()->registerRuleRegex("irods_policy_.*");
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
    _ret = "irods_policy_data_movement" == _rn;
    return SUCCESS();
}

irods::error list_rules(
    irods::default_re_ctx&,
    std::vector<std::string>& _rules) {
    _rules.push_back("irods_policy_data_movement");
    return SUCCESS();
}

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
        // Extract parameters from args
        auto it = _args.begin();
        bool        preserve_replicas{ boost::any_cast<bool>(*it) }; ++it;
        std::string verification_type{ boost::any_cast<std::string>(*it) }; ++it; 
        std::string source_resource{ boost::any_cast<std::string>(*it) }; ++it; 
        std::string destination_resource{ boost::any_cast<std::string>(*it) }; ++it;
        std::string object_path{ boost::any_cast<std::string>(*it) };

        apply_data_movement_policy(
            rei,
            preserve_replicas,
            verification_type,
            source_resource,
            destination_resource,
            object_path);
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
    catch(const boost::bad_any_cast& _e) {
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
    const std::string&,
    msParamArray_t*,
    const std::string&,
    irods::callback ) {
    return ERROR(
            RULE_ENGINE_CONTINUE,
            "exec_rule_text is not supported");
} // exec_rule_text

irods::error exec_rule_expression(
    irods::default_re_ctx&,
    const std::string&,
    msParamArray_t*,
    irods::callback) {
    return ERROR(
            RULE_ENGINE_CONTINUE,
            "exec_rule_expression is not supported");
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




