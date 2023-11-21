

#include <functional>
#include <irods/rcConnect.h>
#include <irods/irods_at_scope_exit.hpp>

namespace irods {
    template <typename Function>
    int exec_as_user(rcComm_t* _comm, const std::string& _user_name, const std::string& _user_zone, Function _func)
    {
        auto& user = _comm->clientUser;

        // need to be able to have a rodsuser/rodsuser 'switch hats'
        //if (user.authInfo.authFlag < LOCAL_PRIV_USER_AUTH) {
        //    THROW(CAT_INSUFFICIENT_PRIVILEGE_LEVEL, "Cannot switch user");
        //}

        const std::string old_user_name = user.userName;
        const std::string old_user_zone = user.rodsZone;

        rstrcpy(user.userName, _user_name.data(), NAME_LEN);
        rstrcpy(user.rodsZone, _user_zone.data(), NAME_LEN);

        rodsLog(
            LOG_DEBUG,
                "Executing as user [%s] fom zone [%s]",
                user.userName,
                user.rodsZone);

        irods::at_scope_exit<std::function<void()>> at_scope_exit{[&user, &old_user_name, &old_user_zone] {
            rstrcpy(user.userName, old_user_name.c_str(), MAX_NAME_LEN);
            rstrcpy(user.rodsZone, old_user_zone.c_str(), MAX_NAME_LEN);
        }};

        return _func(_comm);
    } // exec_as_user

} // namespace irods
