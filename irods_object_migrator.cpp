
#include "irods_object_migrator.hpp"
#include "irods_exception.hpp"
#include "irods_resource_manager.hpp"

#include "irods_query.hpp"

#include "rsModAVUMetadata.hpp"
#include "rsDataObjRepl.hpp"
#include "rsDataObjTrim.hpp"
#include "rsDataObjChksum.hpp"
#include "rsFileStat.hpp"
#include "physPath.hpp"

#include <boost/lexical_cast.hpp>
    
extern irods::resource_manager resc_mgr;

namespace irods {

    object_migrator::object_migrator() :
        comm_{nullptr},
        preserve_replicas_{},
        verification_type_{},
        source_resource_{},
        destination_resource_{},
        object_path_{} {
    }

    object_migrator::object_migrator(
        rsComm_t*          _comm,
        const bool         _preserve_replicas,
        const std::string& _verification_type,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const std::string& _object_path) :
            comm_{_comm},
            preserve_replicas_{_preserve_replicas},
            verification_type_{_verification_type},
            source_resource_{_source_resource},
            destination_resource_{_destination_resource},
            object_path_{_object_path} {
    }

    object_migrator::object_migrator(const object_migrator& _rhs) :
            comm_{_rhs.comm_},
            preserve_replicas_{_rhs.preserve_replicas_},
            verification_type_{_rhs.verification_type_},
            source_resource_{_rhs.source_resource_},
            destination_resource_{_rhs.destination_resource_},
            object_path_{_rhs.object_path_} {
    }

    object_migrator::~object_migrator() {

    }

    void object_migrator::operator()() {
        if(VERIFY_CHECKSUM == verification_type_) {
            compute_checksum_for_resc(
                object_path_,
                source_resource_);
        }

        replicate_object_to_resource(
            verification_type_,
            source_resource_,
            destination_resource_,
            object_path_);

        std::string file_path;
        std::string data_size;
        std::string data_hierarchy;
        std::string data_checksum;
        capture_replica_attributes(
            object_path_,
            destination_resource_,
            file_path,
            data_size,
            data_hierarchy,
            data_checksum );

        bool verified = verify_replica_on_resource(
                            verification_type_,
                            destination_resource_,
                            object_path_,
                            data_size,
                            data_hierarchy,
                            file_path,
                            data_checksum);
        if(!verified) {
            THROW(
                SYS_INVALID_FILE_PATH,
                "replica not verified");
        }

        if(!preserve_replicas_) {
            trim_replica_on_resource(
                object_path_,
                source_resource_);
        }
    } // operator()

    void object_migrator::get_object_and_collection_from_path(
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


    std::string object_migrator::get_leaf_resources_string(
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


    void object_migrator::replicate_object_to_resource(
        const std::string& _verification_type,
        const std::string& _source_resource,
        const std::string& _destination_resource,
        const std::string& _object_path) {
       
        dataObjInp_t data_obj_inp{};
        rstrcpy(data_obj_inp.objPath, _object_path.c_str(), MAX_NAME_LEN);
        data_obj_inp.createMode = getDefFileMode();
        addKeyVal(&data_obj_inp.condInput, RESC_NAME_KW,      _source_resource.c_str());
        addKeyVal(&data_obj_inp.condInput, DEST_RESC_NAME_KW, _destination_resource.c_str());
        if(VERIFY_CHECKSUM == _verification_type) {
            addKeyVal(&data_obj_inp.condInput, VERIFY_CHKSUM_KW, "" );
        }

        if(comm_->clientUser.authInfo.authFlag >= LOCAL_PRIV_USER_AUTH) {
            addKeyVal(&data_obj_inp.condInput, ADMIN_KW, "true" );
        }

        transferStat_t* trans_stat{};
        const auto repl_err = rsDataObjRepl(comm_, &data_obj_inp, &trans_stat);
        free(trans_stat);
        if(repl_err < 0) {
            THROW(
                repl_err,
                boost::format("failed to migrate [%s] to [%s]") %
                _object_path % _destination_resource);
        }
    } // replicate_object_to_resource

    void object_migrator::capture_replica_attributes(
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
        query<rsComm_t> qobj{comm_, query_str, 1};
        if(qobj.size() > 0) {
            const auto result = qobj.front();
            _file_path      = result[0];
            _data_hierarchy = result[1];
            _data_size      = result[2];
            _data_checksum  = result[3];
        }
    } // capture_replica_attributes

    rodsLong_t object_migrator::get_file_size_from_filesystem(
        const std::string& _object_path,
        const std::string& _resource_hierarchy,
        const std::string& _file_path ) {
        fileStatInp_t stat_inp{};
        rstrcpy(stat_inp.objPath,  _object_path.c_str(),  sizeof(stat_inp.objPath));
        rstrcpy(stat_inp.rescHier, _resource_hierarchy.c_str(), sizeof(stat_inp.rescHier));
        rstrcpy(stat_inp.fileName, _file_path.c_str(), sizeof(stat_inp.fileName));
        rodsStat_t *stat_out{};
        const auto status_rsFileStat = rsFileStat(comm_, &stat_inp, &stat_out);
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

    std::string object_migrator::compute_checksum_for_resc(
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
        irods::query<rsComm_t> qobj(comm_, query_str, 1);
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
        const auto chksum_err = rsDataObjChksum(comm_, &data_obj_inp, &chksum);
        if(chksum_err < 0) {
            THROW(
                chksum_err,
                boost::format("rsDataObjChksum failed for [%s] on [%s]") %
                _object_path %
                _resource_name);
        }

        return chksum;
    } // compute_checksum_for_resc

    bool object_migrator::verify_replica_on_resource(
        const std::string& _verification_type,
        const std::string& _destination_resource,
        const std::string& _object_path,
        const std::string& _data_size,
        const std::string& _data_hierarchy,
        const std::string& _file_path,
        const std::string& _data_checksum) {

        try {
            if(_verification_type.size() == 0 || VERIFY_CATALOG == _verification_type) {
                // default verification type is 'catalog', were done as the query worked
                return true;
            }

            if(VERIFY_FILESYSTEM == _verification_type ||
               VERIFY_CHECKSUM   == _verification_type) {
                const auto fs_size = get_file_size_from_filesystem(
                                         _object_path,
                                         _data_hierarchy,
                                         _file_path);
                const auto query_size = boost::lexical_cast<rodsLong_t>(_data_size);
                
                // replication handles checksum verification
                return (fs_size == query_size);
            }

            THROW(
                SYS_INVALID_INPUT_PARAM,
                boost::format("invalid verification type [%s]") %
                _verification_type);

        }
        catch(const boost::bad_lexical_cast& _e) {
            THROW(
                INVALID_LEXICAL_CAST,
                _e.what());
        }
    } // verify_replica_on_resource

    void object_migrator::trim_replica_on_resource(
        const std::string& _object_path,
        const std::string& _source_resource) {

        dataObjInp_t obj_inp{};
        rstrcpy(
            obj_inp.objPath,
            _object_path.c_str(),
            sizeof(obj_inp.objPath));
        addKeyVal(
            &obj_inp.condInput,
            RESC_NAME_KW,
            _source_resource.c_str());
        addKeyVal(
            &obj_inp.condInput,
            COPIES_KW,
            "1");
        if(comm_->clientUser.authInfo.authFlag >= LOCAL_PRIV_USER_AUTH) {
            addKeyVal(
                &obj_inp.condInput,
                ADMIN_KW,
                "true" );
        }

        const auto trim_err = rsDataObjTrim(comm_, &obj_inp);
        if(trim_err < 0) {
            THROW(
                trim_err,
                boost::format("failed to trim obj path [%s] src resc [%s]") %
                _object_path %
                _source_resource);
        }
    } // trim_replica_on_resource


}; // namespace irods

