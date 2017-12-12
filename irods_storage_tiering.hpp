

#ifndef IRODS_STORAGE_TIERING_HPP
#define IRODS_STORAGE_TIERING_HPP

#include <list>
#include <boost/any.hpp>
#include <string>

#include "rcMisc.h"

namespace irods {
    struct storage_tiering_configuration {
        std::string access_time_attribute{"irods::access_time"};
        std::string storage_tiering_group_attribute{"irods::storage_tier_group"};
        std::string storage_tiering_time_attribute{"irods::storage_tier_time"};
        std::string storage_tiering_query_attribute{"irods::storage_tier_query"};
        std::string storage_tiering_verification_attribute{"irods::storage_tier_verification"};
        std::string storage_tiering_restage_delay_attribute{"irods::storage_tier_restage_delay"};

        std::string default_restage_delay_parameters{"<PLUSET>1s</PLUSET><EF>1h DOUBLE UNTIL SUCCESS OR 6 TIMES</EF>"};
        std::string time_check_string{"TIME_CHECK_STRING"};
        
        std::string instance_name{};

        storage_tiering_configuration(const std::string& _instance_name);
    };

    class storage_tiering {
        rsComm_t* comm_;
        storage_tiering_configuration config_;

        void update_access_time_for_data_object(
                const std::string& _logical_path);

        void get_object_and_collection_from_path(
                const std::string& _obj_path,
                std::string&       _coll,
                std::string&       _obj );

        std::string get_metadata_for_resource(
                const std::string& _meta_attr_name,
                const std::string& _resc_name );

        void get_tier_group_resources_and_indicies(
                const std::string&                  _group_name,
                std::map<std::string, std::string>& _resc_map );

        std::string get_leaf_resources_string(
                const std::string& _resc_name);

        std::string get_verification_for_resc(
                const std::string& _resc_name);

        std::string get_restage_delay_param_for_resc(
                const std::string& _resc_name);

        std::map<std::string, std::string> get_resource_map_for_group(
                const std::string& _group);
        
        std::string get_tier_time_for_resc(
                const std::string& _resc_name);
        
        void tier_objects_for_resc(
                const std::string& _src_resc,
                const std::string& _dst_resc);

        void queue_restage_operation(
                ruleExecInfo_t*    _rei,
                const std::string& _restage_delay_params,
                const std::string& _verification_type,
                const std::string& _src_resc,
                const std::string& _dst_resc,
                const std::string& _obj_path);

        public:
        storage_tiering(
            rsComm_t*          _comm,
            const std::string& _instance_name);

        void apply_access_time(
            std::list<boost::any>& _args);

        void restage_object_to_lowest_tier(
            ruleExecInfo_t*        _rei,
            std::list<boost::any>& _args);

        void apply_storage_tiering_policy(
            const std::string& _group);

        void move_data_object(
            const std::string& _verification_type,
            const std::string& _src_resc,
            const std::string& _dst_resc,
            const std::string& _obj_path);

    }; // class storage_tiering

}; // namespace irods

#endif // IRODS_STORAGE_TIERING_HPP

