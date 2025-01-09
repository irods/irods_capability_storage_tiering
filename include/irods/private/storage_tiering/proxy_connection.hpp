#ifndef IRODS_STORAGE_TIERING_PROXY_CONNECTION_HPP
#define IRODS_STORAGE_TIERING_PROXY_CONNECTION_HPP

#include <irods/rcConnect.h>

namespace irods {

    struct proxy_connection {
        rErrMsg_t err_msg;
        rcComm_t* conn;

        // Makes a proxy connection where the client is specified by the username in the parameters, and the proxy user
        // is the service account rodsadmin for the local server.
        auto make(const std::string& clientUser, const std::string& clientZone) -> rcComm_t*
        {
            rodsEnv env{};
            _getRodsEnv(env);

            // TODO(#296): Handle any errors which occur in _rcConnect or clientLogin.
            conn = _rcConnect(env.rodsHost,
                              env.rodsPort,
                              env.rodsUserName,
                              env.rodsZone,
                              clientUser.c_str(),
                              clientZone.c_str(),
                              &err_msg,
                              0,
                              0);

            clientLogin(conn);

            return conn;
        } // make

        // Makes a proxy connection where both the proxy and client users are the service account rodsadmin for the
        // local server.
        auto make_rodsadmin_connection() -> RcComm*
        {
            rodsEnv env{};
            _getRodsEnv(env);

            // TODO(#296): Handle any errors which occur in _rcConnect or clientLogin.
            conn = _rcConnect(env.rodsHost,
                              env.rodsPort,
                              env.rodsUserName,
                              env.rodsZone,
                              env.rodsUserName,
                              env.rodsZone,
                              &err_msg,
                              0,
                              0);

            clientLogin(conn);

            // Set the authFlag because auth plugin does not set it and the storage tiering plugin needs to know whether
            // the client connection is privileged. This proxy connection uses the local client environment which should
            // be the iRODS service account, a rodsadmin. If the local client environment is not a rodsadmin, the plugin
            // will not function properly because it uses the ADMIN_KW and the server does not allow non-rodsadmins to
            // use the ADMIN_KW.
            conn->clientUser.authInfo.authFlag = LOCAL_PRIV_USER_AUTH;

            return conn;
        } // make_rodsadmin_connection

        ~proxy_connection() { rcDisconnect(conn); }
    }; // proxy_connection

}


#endif // IRODS_STORAGE_TIERING_PROXY_CONNECTION_HPP
