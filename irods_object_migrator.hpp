
#ifndef IRODS_OBJECT_MIGRATOR_HPP
#define IRODS_OBJECT_MIGRATOR_HPP

#include "rcConnect.h"

#include <string>

namespace irods {

    class object_migrator {
        rsComm_t*         comm_;
        bool              preserve_replicas_;
        const std::string verification_type_;
        const std::string source_resource_;
        const std::string destination_resource_;
        const std::string object_path_;

        void trim_replica_on_resource(
            const std::string& _object_path,
            const std::string& _source_resource);

        bool verify_replica_on_resource(
            const std::string& _verification_type,
            const std::string& _destination_resource,
            const std::string& _object_path,
            const std::string& _data_size,
            const std::string& _data_hierarchy,
            const std::string& _file_path,
            const std::string& _data_checksum);

        std::string compute_checksum_for_resc(
                const std::string& _object_path,
                const std::string& _resource_name );

        rodsLong_t get_file_size_from_filesystem(
                const std::string& _object_path,
                const std::string& _resource_hierarchy,
                const std::string& _file_path );

        void capture_replica_attributes(
                const std::string& _object_path,
                const std::string& _resource_name,
                std::string&       _data_path,
                std::string&       _data_size,
                std::string&       _data_hierarchy,
                std::string&       _data_checksum );

        void replicate_object_to_resource(
            const std::string& _verification_type,
            const std::string& _source_resource,
            const std::string& _destination_resource,
            const std::string& _object_path);
        
        std::string get_leaf_resources_string(
                const std::string& _resource_name);

        void get_object_and_collection_from_path(
            const std::string& _object_path,
            std::string&       _coll,
            std::string&       _obj );

    public:
        object_migrator();
        object_migrator(
            rsComm_t*          _comm,
            const bool         _preserve_replicas,
            const std::string& _verification_type,
            const std::string& _source_resource,
            const std::string& _destination_resource,
            const std::string& _object_path);
        object_migrator(const object_migrator&);
        ~object_migrator();
        void operator()();

    }; // class object_migrator

}; // namespace irods

#endif // IRODS_OBJECT_MIGRATOR_HPP
