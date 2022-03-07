#ifndef MAKE_CONNECTION_HPP
#define MAKE_CONNECTION_HPP

#include <irods/rcConnect.h>

namespace irods {

    struct proxy_connection {
        rErrMsg_t err_msg;
        rcComm_t* conn;

        auto make(const std::string client = "") -> rcComm_t*
        {
            rodsEnv env{};
            _getRodsEnv(env);

            conn = _rcConnect(
                       env.rodsHost,
                       env.rodsPort,
                       env.rodsUserName,
                       env.rodsZone,
                       !client.empty() ?
                           client.c_str() :
                           env.rodsUserName,
                       env.rodsZone,
                       &err_msg,
                       0, 0);

            clientLogin(conn);

            return conn;
        } // make

        ~proxy_connection() { rcDisconnect(conn); }
    }; // proxy_connection

}


#endif // MAKE_CONNECTION_HPP
