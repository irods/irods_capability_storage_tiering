#ifndef IRODS_CAPABILITY_STORAGE_TIERING_UTILITIES_HPP
#define IRODS_CAPABILITY_STORAGE_TIERING_UTILITIES_HPP

#include <irods/irods_re_plugin.hpp>
#include <irods/irods_exception.hpp>
#include <irods/rodsError.h>

namespace irods {

    std::string any_to_string(boost::any& _a);

    void exception_to_rerror(
        const irods::exception& _exception,
        rError_t&               _error);

    void exception_to_rerror(
        const int   _code,
        const char* _what,
        rError_t&   _error);

    void invoke_policy(
        ruleExecInfo_t*       _rei,
        const std::string&    _action,
        std::list<boost::any> _args);

} // namespace irods

#endif // IRODS_CAPABILITY_STORAGE_TIERING_UTILITIES_HPP
