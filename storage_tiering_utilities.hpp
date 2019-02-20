
#include "irods_re_plugin.hpp"
#include "irods_exception.hpp"
#include "rodsError.h"

namespace irods {

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
