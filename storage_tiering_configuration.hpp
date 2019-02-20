#ifndef STORAGE_TIERING_CONFIGURATION_HPP
#define STORAGE_TIERING_CONFIGURATION_HPP

#include <string>
#include "rcMisc.h"

namespace irods {
    struct storage_tiering_configuration {
        std::string access_time_attribute{"irods::access_time"};
        std::string group_attribute{"irods::storage_tiering::group"};
        std::string time_attribute{"irods::storage_tiering::time"};
        std::string query_attribute{"irods::storage_tiering::query"};
        std::string verification_attribute{"irods::storage_tiering::verification"};
        std::string data_movement_parameters_attribute{"irods::storage_tiering::data_movement_parameters"};
        std::string minimum_restage_tier{"irods::storage_tiering::minimum_restage_tier"};
        std::string preserve_replicas{"irods::storage_tiering::preserve_replicas"};
        std::string object_limit{"irods::storage_tiering::object_limit"};

        std::string default_data_movement_parameters{"<INST_NAME>irods-rule-engine-plugin-storage-tiering-instance</INST_NAME><PLUSET>1s</PLUSET><EF>1h DOUBLE UNTIL SUCCESS OR 6 TIMES</EF>"};
        std::string time_check_string{"TIME_CHECK_STRING"};
        
        const std::string data_transfer_log_level_key{"data_transfer_log_level"};
        int data_transfer_log_level_value{LOG_DEBUG};

        const std::string instance_name{};

        explicit storage_tiering_configuration(const std::string& _instance_name);
    };
} // namespace irods

#endif // STORAGE_TIERING_CONFIGURATION_HPP
