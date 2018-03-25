

#ifndef IRODS_STORAGE_TIERING_HPP
#define IRODS_STORAGE_TIERING_HPP

#include <list>
#include <boost/any.hpp>
#include <string>

#include "rcMisc.h"

namespace irods {
    struct storage_tiering_configuration {
        std::string access_time_attribute{"irods::access_time"};
        std::string group_attribute{"irods::storage_tiering::group"};
        std::string time_attribute{"irods::storage_tiering::time"};
        std::string query_attribute{"irods::storage_tiering::query"};
        std::string verification_attribute{"irods::storage_tiering::verification"};
        std::string restage_delay_attribute{"irods::storage_tiering::restage_delay"};
        std::string minimum_restage_tier{"irods::storage_tiering::minimum_restage_tier"};
        std::string preserve_replicas{"irods::storage_tiering::preserve_replicas"};
        std::string object_limit{"irods::storage_tiering::object_limit"};

        std::string default_restage_delay_parameters{"<PLUSET>1s</PLUSET><EF>1h DOUBLE UNTIL SUCCESS OR 6 TIMES</EF>"};
        std::string time_check_string{"TIME_CHECK_STRING"};
        
        const std::string data_transfer_log_level_key{"data_transfer_log_level"};
        int data_transfer_log_level_value{LOG_DEBUG};

        std::string instance_name{};

        explicit storage_tiering_configuration(const std::string& _instance_name);
    };

    class storage_tiering {
        rsComm_t*                     comm_;
        storage_tiering_configuration config_;

        void update_access_time_for_data_object(
            const std::string& _logical_path);

        std::string get_metadata_for_resource(
            const std::string& _meta_attr_name,
            const std::string& _resource_name);

        typedef std::vector<std::pair<std::string, std::string>> metadata_results;
        void get_metadata_for_resource(
            const std::string& _meta_attr_name,
            const std::string& _resource_name,
            metadata_results&  _results);

        std::map<std::string, std::string>
        get_tier_group_resources_and_indices(
            const std::string& _group_name);

        std::string get_leaf_resources_string(
            const std::string& _resource_name);

        bool get_preserve_replicas_for_resc(
            const std::string& _source_resource);

        std::string get_verification_for_resc(
            const std::string& _resource_name);

        std::string get_restage_tier_resource_name(
            const std::string& _resource_name);

        std::string get_restage_delay_param_for_resc(
            const std::string& _resource_name);

        std::map<std::string, std::string> get_resource_map_for_group(
            const std::string& _group);
        
        std::string get_tier_time_for_resc(
            const std::string& _resource_name);
        
        metadata_results get_violating_queries_for_resource(
            const std::string& _resource_name);
        
        uint32_t get_object_limit_for_resource(
            const std::string& _resource_name);

        void migrate_violating_objects_for_resource(
            const std::string& _source_resource,
            const std::string& _destination_resource,
            ruleExecInfo_t*        _rei);

        public:
        storage_tiering(
            rsComm_t*          _comm,
            const std::string& _instance_name);

        void apply_access_time(
            std::list<boost::any>& _args);

        void restage_object_to_lowest_tier(
            std::list<boost::any>& _args,
            ruleExecInfo_t*        _rei);

        void apply_storage_tiering_policy(
            const std::string& _group,
            ruleExecInfo_t*    _rei);

        void queue_data_movement(
            const std::string& _restage_delay_params,
            const std::string& _verification_type,
            const std::string& _source_resource,
            const std::string& _destination_resource,
            const std::string& _object_path,
            ruleExecInfo_t*    _rei);

        void move_data_object(
            const bool         _preserve_replicas,
            const std::string& _verification_type,
            const std::string& _source_resource,
            const std::string& _destination_resource,
            const std::string& _object_path);

    }; // class storage_tiering

}; // namespace irods

#endif // IRODS_STORAGE_TIERING_HPP

