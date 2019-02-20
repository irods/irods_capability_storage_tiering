
#include "rcConnect.h"
#include <string>

namespace irods {
    bool verify_replica_for_destination_resource(
        rsComm_t*          _comm,
        const std::string& _verification_type,
        const std::string& _object_path,
        const std::string& _source_resource,
        const std::string& _destination_resource);


} // namespace irods


