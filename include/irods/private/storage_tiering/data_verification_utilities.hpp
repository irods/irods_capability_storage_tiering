#ifndef IRODS_CAPABILITY_STORAGE_TIERING_DATA_VERIFICATION_UTILITIES_HPP
#define IRODS_CAPABILITY_STORAGE_TIERING_DATA_VERIFICATION_UTILITIES_HPP

#include <irods/rcConnect.h>

#include <string>

namespace irods {
    bool verify_replica_for_destination_resource(
        rcComm_t*          _comm,
        const std::string& _instance_name,
        const std::string& _verification_type,
        const std::string& _object_path,
        const std::string& _source_resource,
        const std::string& _destination_resource);


} // namespace irods

#endif // IRODS_CAPABILITY_STORAGE_TIERING_DATA_VERIFICATION_UTILITIES_HPP
