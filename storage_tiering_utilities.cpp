
#include "storage_tiering_utilities.hpp"

#include "rcMisc.h"


namespace irods {

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

