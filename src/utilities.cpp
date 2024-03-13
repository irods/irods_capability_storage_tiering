#include "irods/private/storage_tiering/utilities.hpp"

#include <irods/rcMisc.h>

#include <cstring>

namespace irods {
    std::string any_to_string(boost::any& _a) {
        if(_a.type() == typeid(std::string)) {
            return boost::any_cast<std::string>(_a);
        }
        else if(_a.type() == typeid(std::string*)) {
            return *boost::any_cast<std::string*>(_a);
        }
        else if(_a.type() == typeid(msParam_t*)) {
            msParam_t* msp = boost::any_cast<msParam_t*>(_a);
            if(std::strcmp(msp->type, STR_MS_T) == 0) {
                return static_cast<char*>(msp->inOutStruct);
            }
            else {
                rodsLog(
                    LOG_ERROR,
                    "not a string type [%s]",
                    msp->type);
            }
        }

        THROW(
           SYS_INVALID_INPUT_PARAM,
           boost::format("parameter is not a string [%s]")
           % _a.type().name());
    } // any_to_string

    void exception_to_rerror(
        const irods::exception& _exception,
        rError_t&               _error) {

        std::string msg;
        for(const auto& i : _exception.message_stack()) {
            msg += i;
        }

        addRErrorMsg(
            &_error,
            _exception.code(),
            msg.c_str());
    } // exception_to_rerror

    void exception_to_rerror(
        const int   _code,
        const char* _what,
        rError_t&   _error) {
        addRErrorMsg(
            &_error,
            _code,
            _what);
    } // exception_to_rerror

    static std::string collapse_error_stack(
                           rError_t& _error) {
        std::stringstream ss;
        for(int i = 0; i < _error.len; ++i) {
            rErrMsg_t* err_msg = _error.errMsg[i];
            if(err_msg->status != STDOUT_STATUS) {
                ss << "status: " << err_msg->status << " ";
            }

            ss << err_msg->msg << " - ";
        }
        return ss.str();
    } // collapse_error_stack

    void invoke_policy(
        ruleExecInfo_t*       _rei,
        const std::string&    _action,
        std::list<boost::any> _args) {
        irods::rule_engine_context_manager<
            irods::unit,
            ruleExecInfo_t*,
            irods::AUDIT_RULE> re_ctx_mgr(
                    irods::re_plugin_globals->global_re_mgr,
                    _rei);
        irods::error err = re_ctx_mgr.exec_rule(_action, irods::unpack(_args));
        if(!err.ok()) {
            if(_rei->status < 0) {
                std::string msg = collapse_error_stack(_rei->rsComm->rError);
                THROW(_rei->status, msg);
            }

            THROW(err.code(), err.result());
        }

    } // invoke_policy 

} // namespace irods

