
#undef RODS_SERVER

#include "irods_query.hpp"
#include "irods_resource_manager.hpp"
#include "physPath.hpp"
#include "data_verification_utilities.hpp"
#include "storage_tiering_configuration.hpp"

#include "dataObjChksum.h"
#include "fileStat.h"

#include <boost/lexical_cast.hpp>

extern irods::resource_manager resc_mgr;

namespace {
    static const std::string VERIFY_CHECKSUM{"checksum"};
    static const std::string VERIFY_FILESYSTEM{"filesystem"};
    static const std::string VERIFY_CATALOG{"catalog"};

    rodsLong_t get_file_size_from_filesystem(
        rcComm_t*          _comm,
        const std::string& _object_path,
        const std::string& _resource_hierarchy,
        const std::string& _file_path ) {
        fileStatInp_t stat_inp{};
        rstrcpy(stat_inp.objPath,  _object_path.c_str(),  sizeof(stat_inp.objPath));
        rstrcpy(stat_inp.rescHier, _resource_hierarchy.c_str(), sizeof(stat_inp.rescHier));
        rstrcpy(stat_inp.fileName, _file_path.c_str(), sizeof(stat_inp.fileName));
        rodsStat_t *stat_out{};
        const auto status_rsFileStat = rcFileStat(_comm, &stat_inp, &stat_out);
        if(status_rsFileStat < 0) {
            THROW(
                status_rsFileStat,
                boost::format("rsFileStat of objPath [%s] rescHier [%s] fileName [%s] failed with [%d]") %
                stat_inp.objPath %
                stat_inp.rescHier %
                stat_inp.fileName %
                status_rsFileStat);
            return status_rsFileStat;
        }

        const auto size_in_vault = stat_out->st_size;
        free(stat_out);
        return size_in_vault;
    } // get_file_size_from_filesystem

    std::string get_leaf_resources_string(
        const std::string& _resource_name) {
        std::string leaf_id_str;

        // if the resource has no children then simply return
        irods::resource_ptr root_resc;
        irods::error err = resc_mgr.resolve(_resource_name, root_resc);
        if(!err.ok()) {
            THROW(err.code(), err.result());
        }

        try {
            std::vector<irods::resource_manager::leaf_bundle_t> leaf_bundles =
                resc_mgr.gather_leaf_bundles_for_resc(_resource_name);
            for(const auto & bundle : leaf_bundles) {
                for(const auto & leaf_id : bundle) {
                    leaf_id_str +=
                        "'" + boost::str(boost::format("%s") % leaf_id) + "',";
                } // for
            } // for
        }
        catch( const irods::exception & _e ) {
            throw;
        }

        // if there is no hierarchy
        if(leaf_id_str.empty()) {
            rodsLong_t resc_id;
            resc_mgr.hier_to_leaf_id(_resource_name, resc_id);
            leaf_id_str =
                "'" + boost::str(boost::format("%s") % resc_id) + "',";
        }

        return leaf_id_str;

    } // get_leaf_resources_string

    void get_object_and_collection_from_path(
        const std::string& _object_path,
        std::string&       _collection_name,
        std::string&       _object_name ) {
        namespace bfs = boost::filesystem;

        try {
            bfs::path p(_object_path);
            _collection_name = p.parent_path().string();
            _object_name     = p.filename().string();
        }
        catch(const bfs::filesystem_error& _e) {
            THROW(SYS_INVALID_FILE_PATH, _e.what());
        }
    } // get_object_and_collection_from_path

    std::string compute_checksum_for_resource(
        rcComm_t*          _comm,
        const std::string& _object_path,
        const std::string& _resource_name ) {
        // query if a checksum exists
        std::string coll_name, obj_name;
        get_object_and_collection_from_path(
            _object_path,
            coll_name,
            obj_name);

        const auto query_str = boost::str(
                        boost::format("SELECT DATA_CHECKSUM WHERE DATA_NAME = '%s' AND COLL_NAME = '%s' AND RESC_NAME = '%s'") %
                        obj_name %
                        coll_name %
                        _resource_name);
        irods::query<rcComm_t> qobj(_comm, query_str, 1);
        if(qobj.size() > 0) {
            const auto& result = qobj.front();
            const auto& data_checksum = result[0];
            if(!data_checksum.empty()) {
                return data_checksum;
            }
        }

        // no checksum, compute one
        dataObjInp_t data_obj_inp{};
        rstrcpy(data_obj_inp.objPath, _object_path.c_str(), MAX_NAME_LEN);
        addKeyVal(&data_obj_inp.condInput, RESC_NAME_KW, _resource_name.c_str());

        char* chksum{};
        const auto chksum_err = rcDataObjChksum(_comm, &data_obj_inp, &chksum);
        if(chksum_err < 0) {
            THROW(
                chksum_err,
                boost::format("rsDataObjChksum failed for [%s] on [%s]") %
                _object_path %
                _resource_name);
        }

        return chksum;
    } // compute_checksum_for_resource

    void capture_replica_attributes(
        rcComm_t*          _comm,
        const std::string& _object_path,
        const std::string& _resource_name,
        std::string&       _file_path,
        std::string&       _data_size,
        std::string&       _data_hierarchy,
        std::string&       _data_checksum ) {
        std::string coll_name, obj_name;
        get_object_and_collection_from_path(
            _object_path,
            coll_name,
            obj_name);
        const auto leaf_str = get_leaf_resources_string(
                                   _resource_name);
        const auto query_str = boost::str(
                        boost::format("SELECT DATA_PATH, DATA_RESC_HIER, DATA_SIZE, DATA_CHECKSUM WHERE DATA_NAME = '%s' AND COLL_NAME = '%s' AND DATA_RESC_ID IN (%s)") %
                        obj_name %
                        coll_name %
                        leaf_str);
        irods::query<rcComm_t> qobj{_comm, query_str, 1};
        if(qobj.size() > 0) {
            const auto result = qobj.front();
            _file_path      = result[0];
            _data_hierarchy = result[1];
            _data_size      = result[2];
            _data_checksum  = result[3];
        }
    } // capture_replica_attributes
} // namespace


namespace irods {

    bool verify_replica_for_destination_resource(
        rcComm_t*          _comm,
        const std::string& _instance_name,
        const std::string& _verification_type,
        const std::string& _object_path,
        const std::string& _source_resource,
        const std::string& _destination_resource) {

        using stcfg = irods::storage_tiering_configuration;
        auto log_level = stcfg{_instance_name}.data_transfer_log_level_value;

        rodsLog(
            log_level,
            "%s - [%s] [%s] [%s] [%s]",
            __FUNCTION__,
            _verification_type.c_str(),
            _object_path.c_str(),
            _source_resource.c_str(),
            _destination_resource.c_str());

        try {
            std::string source_object_path;
            std::string source_data_size;
            std::string source_data_hierarchy;
            std::string source_file_path;
            std::string source_data_checksum;

            capture_replica_attributes(
                _comm,
                _object_path,
                _source_resource,
                source_file_path,
                source_data_size,
                source_data_hierarchy,
                source_data_checksum );

            rodsLog(
                log_level,
                "%s - source attributes: [%s] [%s] [%s] [%s]",
                __FUNCTION__,
                source_file_path.c_str(),
                source_data_size.c_str(),
                source_data_hierarchy.c_str(),
                source_data_checksum.c_str());

            std::string destination_object_path;
            std::string destination_data_size;
            std::string destination_data_hierarchy;
            std::string destination_file_path;
            std::string destination_data_checksum;

            capture_replica_attributes(
                _comm,
                _object_path,
                _destination_resource,
                destination_file_path,
                destination_data_size,
                destination_data_hierarchy,
                destination_data_checksum );

            rodsLog(
                log_level,
                "%s - destination attributes: [%s] [%s] [%s] [%s]",
                __FUNCTION__,
                destination_file_path.c_str(),
                destination_data_size.c_str(),
                destination_data_hierarchy.c_str(),
                destination_data_checksum.c_str());


            if(_verification_type.size() == 0 ||
               VERIFY_CATALOG == _verification_type) {
                // default verification type is 'catalog'
                // make sure catalog update was a success
                if(source_data_size == destination_data_size) {
                    rodsLog(
                        log_level,
                        "%s - verify catalog is a success",
                        __FUNCTION__);
                    return true;
                }
            }
            else if(VERIFY_FILESYSTEM == _verification_type) {
                const auto fs_size = get_file_size_from_filesystem(
                                         _comm,
                                         _object_path,
                                         destination_data_hierarchy,
                                         destination_file_path);
                const auto query_size = boost::lexical_cast<rodsLong_t>(source_data_size);
                auto match = (fs_size == query_size);
                rodsLog(
                    log_level,
                    "%s - verify filesystem: %d - %ld vs %ld",
                    __FUNCTION__,
                    match,
                    fs_size,
                    query_size);

                return match;
            }
            else if(VERIFY_CHECKSUM == _verification_type) {
                if(source_data_checksum.size() == 0) {
                    source_data_checksum = compute_checksum_for_resource(
                                               _comm,
                                               _object_path,
                                               _source_resource);
                }

                if(destination_data_checksum.size() == 0) {
                    destination_data_checksum = compute_checksum_for_resource(
                                                   _comm,
                                                   _object_path,
                                                   _destination_resource);
                }

                rodsLog(
                    log_level,
                    "%s - source checksum: [%s], destination checksum [%s]",
                    __FUNCTION__,
                    source_data_checksum.c_str(),
                    destination_data_checksum.c_str());

                auto match = (source_data_checksum == destination_data_checksum);

                rodsLog(
                    log_level,
                    "%s - verify checksum: %d",
                    __FUNCTION__,
                    match);

                return match;
            }
            else {
                THROW(
                    SYS_INVALID_INPUT_PARAM,
                    boost::format("invalid verification type [%s]") %
                    _verification_type);
            }
        }
        catch(const boost::bad_lexical_cast& _e) {
            THROW(
                INVALID_LEXICAL_CAST,
                _e.what());
        }

        return false;

    } // verify_replica_for_destination_resource

} // namespace irods


