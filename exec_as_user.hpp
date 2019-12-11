

#include <functional>
#include "rcConnect.h"
#include "irods_at_scope_exit.hpp"

namespace irods {
    template <typename Function>
    int exec_as_user(rsComm_t& _comm, const std::string& _user_name, Function _func)
    {
        auto& user = _comm.clientUser;

        // need to be able to have a rodsuser/rodsuser 'switch hats'
        //if (user.authInfo.authFlag < LOCAL_PRIV_USER_AUTH) {
        //    THROW(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "Cannot switch user");
        //}

        const std::string old_user_name = user.userName;

        rstrcpy(user.userName, _user_name.data(), NAME_LEN);

        irods::at_scope_exit<std::function<void()>> at_scope_exit{[&user, &old_user_name] {
            rstrcpy(user.userName, old_user_name.c_str(), MAX_NAME_LEN);
        }};

        return _func(_comm);
    } // exec_as_user

} // namespace irods
