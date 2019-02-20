
#include "irods_query.hpp"
#include "irods_re_plugin.hpp"
#include "irods_re_ruleexistshelper.hpp"
#include "irods_virtual_path.hpp"

#include "rsModAVUMetadata.hpp"
#include "rsOpenCollection.hpp"
#include "rsReadCollection.hpp"
#include "rsCloseCollection.hpp"

#include <boost/any.hpp>

std::string plugin_instance_name{};

namespace {
    void update_access_time_for_data_object(
        rsComm_t*          _comm,
        const std::string& _logical_path,
        const std::string& _attribute) {

        auto ts = std::to_string(std::time(nullptr));
        modAVUMetadataInp_t avuOp{
            .arg0 = "set",
            .arg1 = "-d",
            .arg2 = const_cast<char*>(_logical_path.c_str()),
            .arg3 = const_cast<char*>(_attribute.c_str()),
            .arg4 = const_cast<char*>(ts.c_str()),
            .arg5 = ""};

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

    void apply_access_time_policy(
        rsComm_t*              _comm,
        std::list<boost::any>& _args) {
        try {
            if(_args.size() != 3) {
                THROW(
                    SYS_INVALID_INPUT_PARAM,
                    "invalid number of arguments");
            }
            auto it = _args.begin();
            auto obj_path  = boost::any_cast<std::string>(*it); ++it;
            auto coll_type = boost::any_cast<std::string>(*it); ++it;
            auto attribute = boost::any_cast<std::string>(*it);

            if(coll_type.size() == 0) {
                update_access_time_for_data_object(_comm, obj_path, attribute);
            }
            else {
                // register a collection
                collInp_t coll_inp;
                memset(&coll_inp, 0, sizeof(coll_inp));
                rstrcpy(
                    coll_inp.collName,
                    obj_path.c_str(),
                    MAX_NAME_LEN);
                int handle = rsOpenCollection(
                                 _comm,
                                 &coll_inp);
                if(handle < 0) {
                    THROW(
                        handle,
                        boost::format("failed to open collection [%s]") %
                        obj_path);
                }

                apply_access_time_to_collection(_comm, handle, attribute);
            }
        }
        catch(const boost::bad_any_cast& _e) {
            THROW( INVALID_ANY_CAST, _e.what() );
        }
    } // apply_access_time_policy

}

irods::error start(
    irods::default_re_ctx&,
    const std::string& _instance_name ) {
    plugin_instance_name = _instance_name;
    RuleExistsHelper::Instance()->registerRuleRegex("irods_policy_apply_access_time");
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
    _ret = "irods_policy_apply_access_time" == _rn;
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
    ruleExecInfo_t* rei{};
    const auto err = _eff_hdlr("unsafe_ms_ctx", &rei);
    if(!err.ok()) {
        return err;
    }

    try {
        apply_access_time_policy(rei->rsComm, _args);
    }
    catch(const  std::invalid_argument& _e) {
        irods::log(LOG_ERROR, _e.what());
        return ERROR(
                   SYS_NOT_SUPPORTED,
                   _e.what());
    }
    catch(const boost::bad_any_cast& _e) {
        irods::log(LOG_ERROR, _e.what());
        return ERROR(
                   SYS_NOT_SUPPORTED,
                   _e.what());
    }
    catch(const irods::exception& _e) {
        irods::log(_e);
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




