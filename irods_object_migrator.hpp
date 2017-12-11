
#ifndef IRODS_OBJECT_MIGRATOR_HPP
#define IRODS_OBJECT_MIGRATOR_HPP

#include "rcConnect.h"

#include <string>

namespace irods {
static const std::string VERIFY_CHECKSUM{"checksum"};
static const std::string VERIFY_FILESYSTEM{"filesystem"};
static const std::string VERIFY_CATALOG{"catalog"};


    class object_migrator {
        rsComm_t*          comm_;
        const std::string verification_type_;
        const std::string src_resc_;
        const std::string dst_resc_;
        const std::string obj_path_;

        void trim_replica_on_resource(
            rsComm_t*          _comm,
            const std::string& _obj_path,
            const std::string& _src_resc);

        bool verify_replica_on_resource(
            rsComm_t*          _comm,
            const std::string& _verification_type,
            const std::string& _dst_resc,
            const std::string& _obj_path,
            const std::string& _data_size,
            const std::string& _data_hier,
            const std::string& _data_path,
            const std::string& _data_checksum);

        std::string compute_checksum_for_resc(
                rsComm_t*         _comm,
                const std::string _obj_path,
                const std::string _resc_name );

        rodsLong_t get_file_size_from_filesystem(
                rsComm_t*         _comm,
                const std::string _obj_path,
                const std::string _resc_hier,
                const std::string _file_path );

        void capture_replica_attributes(
                rsComm_t*          _comm,
                const std::string& _obj_path,
                const std::string& _resc_name,
                std::string&       _data_path,
                std::string&       _data_size,
                std::string&       _data_hier,
                std::string&       _data_checksum );

        void replicate_object_to_resource(
            rsComm_t*          _comm,
            const std::string& _verification_type,
            const std::string& _src_resc,
            const std::string& _dst_resc,
            const std::string& _obj_path);
        
        std::string get_leaf_resources_string(
                rsComm_t*          _comm,
                const std::string& _resc_name);

        void get_object_and_collection_from_path(
            const std::string& _obj_path,
            std::string&       _coll,
            std::string&       _obj );

    public:
        object_migrator();
        object_migrator(
            rsComm_t*          _comm,
            const std::string& _verification_type,
            const std::string& _src_resc,
            const std::string& _dst_resc,
            const std::string& _obj_path);
        object_migrator(const object_migrator&);
        ~object_migrator();
        void operator()();

    }; // class object_migrator

}; // namespace irods

#endif // IRODS_OBJECT_MIGRATOR_HPP
