
#include "irods_object_migrator.hpp"
#include "irods_exception.hpp"
#include "irods_resource_manager.hpp"
#include "irods_virtual_path.hpp"

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
        verification_type_{},
        src_resc_{},
        dst_resc_{},
        obj_path_{} {
    }

    object_migrator::object_migrator(
        rsComm_t*          _comm,
        const std::string& _verification_type,
        const std::string& _src_resc,
        const std::string& _dst_resc,
        const std::string& _obj_path) :
            comm_{_comm},
            verification_type_{_verification_type},
            src_resc_{_src_resc},
            dst_resc_{_dst_resc},
            obj_path_{_obj_path} {
    }

    object_migrator::object_migrator(const object_migrator& _rhs) :
            comm_{_rhs.comm_},
            verification_type_{_rhs.verification_type_},
            src_resc_{_rhs.src_resc_},
            dst_resc_{_rhs.dst_resc_},
            obj_path_{_rhs.obj_path_} {
    }

    object_migrator::~object_migrator() {

    }

    void object_migrator::operator()() {
        try {
            if(VERIFY_CHECKSUM == verification_type_) {
                compute_checksum_for_resc(
                    comm_,
                    obj_path_,
                    src_resc_);
            }

            replicate_object_to_resource(
                comm_,
                verification_type_,
                src_resc_,
                dst_resc_,
                obj_path_);

            std::string data_path;
            std::string data_size;
            std::string data_hier;
            std::string data_checksum;
            capture_replica_attributes(
                comm_,
                obj_path_,
                dst_resc_,
                data_path,
                data_size,
                data_hier,
                data_checksum );

            bool verified = verify_replica_on_resource(
                                comm_,
                                verification_type_,
                                dst_resc_,
                                obj_path_,
                                data_size,
                                data_hier,
                                data_path,
                                data_checksum);
            if(!verified) {
                THROW(
                    SYS_INVALID_FILE_PATH,
                    "replica not verified");
            }

            trim_replica_on_resource(
                comm_,
                obj_path_,
                src_resc_);
        }
        catch(const irods::exception&) {
            throw;
        }
    } // operator()

    void object_migrator::get_object_and_collection_from_path(
        const std::string& _obj_path,
        std::string&       _coll,
        std::string&       _obj ) {
        namespace bfs = boost::filesystem;

        try {
            bfs::path p(_obj_path);
            _coll = p.parent_path().string();
            _obj  = p.filename().string();
        }
        catch(const bfs::filesystem_error& _e) {
            THROW(SYS_INVALID_FILE_PATH, _e.what());
        }
    } // get_object_and_collection_from_path


    std::string object_migrator::get_leaf_resources_string(
        rsComm_t*          _comm,
        const std::string& _resc_name) {
        std::string leaf_id_str;

        // if the resource has no children then simply return
        irods::resource_ptr root_resc;
        irods::error err = resc_mgr.resolve(_resc_name, root_resc);
        if(!err.ok()) {
            THROW(err.code(), err.result());
        }

        try {
            std::vector<irods::resource_manager::leaf_bundle_t> leaf_bundles = 
                resc_mgr.gather_leaf_bundles_for_resc(_resc_name);
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
            resc_mgr.hier_to_leaf_id(_resc_name, resc_id);
            leaf_id_str =
                "'" + boost::str(boost::format("%s") % resc_id) + "',";
        }

        return leaf_id_str;

    } // get_leaf_resources_string


    void object_migrator::replicate_object_to_resource(
        rsComm_t*          _comm,
        const std::string& _verification_type,
        const std::string& _src_resc,
        const std::string& _dst_resc,
        const std::string& _obj_path) {
       
        dataObjInp_t data_obj_inp;
        memset(&data_obj_inp, 0, sizeof(data_obj_inp));
        rstrcpy(data_obj_inp.objPath, _obj_path.c_str(), MAX_NAME_LEN);
        data_obj_inp.createMode = getDefFileMode();
        addKeyVal(&data_obj_inp.condInput, RESC_NAME_KW,      _src_resc.c_str());
        addKeyVal(&data_obj_inp.condInput, DEST_RESC_NAME_KW, _dst_resc.c_str());
        addKeyVal(&data_obj_inp.condInput, ADMIN_KW,          "" );
        if(VERIFY_CHECKSUM == _verification_type) {
            addKeyVal(&data_obj_inp.condInput, VERIFY_CHKSUM_KW, "" );
        }

        transferStat_t* trans_stat = NULL;
        int repl_err = rsDataObjRepl(_comm, &data_obj_inp, &trans_stat);
        free(trans_stat);
        if(repl_err < 0) {
            THROW(
                repl_err,
                boost::format("failed to migrate [%s] to [%s]") %
                _obj_path % _dst_resc);
        }
    } // replicate_object_to_resource

    void object_migrator::capture_replica_attributes(
            rsComm_t*          _comm,
            const std::string& _obj_path,
            const std::string& _resc_name,
            std::string&       _data_path,
            std::string&       _data_size,
            std::string&       _data_hier,
            std::string&       _data_checksum ) {
        try {
            std::string coll_name, obj_name;
            get_object_and_collection_from_path(
                _obj_path,
                coll_name,
                obj_name);

            std::string leaf_str = get_leaf_resources_string(
                                       _comm,
                                       _resc_name);

            std::string query_str{ boost::str(
                            boost::format("SELECT DATA_PATH, DATA_RESC_HIER, DATA_SIZE, DATA_CHECKSUM WHERE DATA_NAME = '%s' AND COLL_NAME = '%s' AND DATA_RESC_ID IN (%s)") %
                            obj_name %
                            coll_name %
                            leaf_str)};
            irods::query query(_comm, query_str, 1);
            if(query.size() > 0) {
                irods::query::value_type result = query.front();
                _data_path     = result[0];
                _data_hier     = result[1];
                _data_size     = result[2];
                _data_checksum = result[3];
            }
        }
        catch(const irods::exception&) {
            throw;
        }
    } // capture_replica_attributes

    rodsLong_t object_migrator::get_file_size_from_filesystem(
            rsComm_t*         _comm,
            const std::string _obj_path,
            const std::string _resc_hier,
            const std::string _file_path ) {
        fileStatInp_t stat_inp;
        memset(&stat_inp, 0, sizeof(stat_inp));
        rstrcpy(stat_inp.objPath,  _obj_path.c_str(),  sizeof(stat_inp.objPath));
        rstrcpy(stat_inp.rescHier, _resc_hier.c_str(), sizeof(stat_inp.rescHier));
        rstrcpy(stat_inp.fileName, _file_path.c_str(), sizeof(stat_inp.fileName));
        rodsStat_t *stat_out = nullptr;
        const int status_rsFileStat = rsFileStat(_comm, &stat_inp, &stat_out);
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

        const rodsLong_t size_in_vault = stat_out->st_size;
        free(stat_out);
        return size_in_vault;
    } // get_file_size_from_filesystem

    std::string object_migrator::compute_checksum_for_resc(
            rsComm_t*         _comm,
            const std::string _obj_path,
            const std::string _resc_name ) {
        // query if a checksum exists
        std::string coll_name, obj_name;
        get_object_and_collection_from_path(
            _obj_path,
            coll_name,
            obj_name);

        std::string query_str{ boost::str(
                        boost::format("SELECT DATA_CHECKSUM WHERE DATA_NAME = '%s' AND COLL_NAME = '%s' AND RESC_NAME = '%s'") %
                        obj_name %
                        coll_name %
                        _resc_name)};
        std::string data_checksum;
        irods::query query(_comm, query_str, 1);
        if(query.size() > 0) {
            irods::query::value_type result = query.front();
            data_checksum = result[0];
        }

        if(!data_checksum.empty()) {
            return data_checksum;
        }

        // no checksum, compute one
        dataObjInp_t data_obj_inp;
        memset(&data_obj_inp, 0, sizeof(data_obj_inp));
        rstrcpy(data_obj_inp.objPath, _obj_path.c_str(), MAX_NAME_LEN);
        addKeyVal(&data_obj_inp.condInput, RESC_NAME_KW, _resc_name.c_str());

        char* chksum = nullptr;
        int chksum_err = rsDataObjChksum(_comm, &data_obj_inp, &chksum);
        if(chksum_err < 0) {
            THROW(
                chksum_err,
                boost::format("rsDataObjChksum failed for [%s] on [%s]") %
                _obj_path %
                _resc_name);
        }

        return chksum;
    } // compute_checksum_for_resc

    bool object_migrator::verify_replica_on_resource(
        rsComm_t*          _comm,
        const std::string& _verification_type,
        const std::string& _dst_resc,
        const std::string& _obj_path,
        const std::string& _data_size,
        const std::string& _data_hier,
        const std::string& _data_path,
        const std::string& _data_checksum) {

        try {
            if(_verification_type.size() <= 0 || VERIFY_CATALOG == _verification_type) {
                // default verification type is 'catalog', were done as the query worked
                return true;
            }

            if(VERIFY_FILESYSTEM == _verification_type ||
               VERIFY_CHECKSUM   == _verification_type) {
                rodsLong_t fs_size = get_file_size_from_filesystem(
                                         _comm,
                                         _obj_path,
                                         _data_hier,
                                         _data_path);
                const rodsLong_t query_size = boost::lexical_cast<rodsLong_t>(_data_size);
                if(fs_size != query_size) {
                    return false;
                }

                // replication handles checksum verification
                //
                return true;
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
        catch(const irods::exception&) {
            throw;
        }
    } // verify_replica_on_resource

    void object_migrator::trim_replica_on_resource(
        rsComm_t*          _comm,
        const std::string& _obj_path,
        const std::string& _src_resc) {

        dataObjInp_t obj_inp;
        memset(&obj_inp, 0, sizeof(obj_inp));

        //obj_inp.oprType = UNREG_OPR;

        rstrcpy(obj_inp.objPath, _obj_path.c_str(), sizeof(obj_inp.objPath));
        addKeyVal(
            &obj_inp.condInput,
            RESC_NAME_KW,
            _src_resc.c_str());
        addKeyVal(
            &obj_inp.condInput,
            COPIES_KW,
            "1");

        int trim_err = rsDataObjTrim(_comm, &obj_inp);
        if(trim_err < 0) {
            THROW(
                trim_err,
                boost::format("failed to trim obj path [%s] src resc [%s]") %
                _obj_path %
                _src_resc);
        }
    } // trim_replica_on_resource


}; // namespace irods

