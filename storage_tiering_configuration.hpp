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

        std::string minimum_delay_time{"irods::storage_tiering::minimum_delay_time_in_seconds"};
        std::string maximum_delay_time{"irods::storage_tiering::maximum_delay_time_in_seconds"};

        std::string migration_scheduled_flag{"irods::storage_tiering::migration_scheduled"};

        std::string time_check_string{"TIME_CHECK_STRING"};

        const std::string data_transfer_log_level_key{"data_transfer_log_level"};
        int data_transfer_log_level_value{LOG_DEBUG};

        int number_of_scheduling_threads{4};
        int default_minimum_delay_time{1};
        int default_maximum_delay_time{30};
        std::string default_data_movement_parameters{"<EF>60s DOUBLE UNTIL SUCCESS OR 5 TIMES</EF>"};

        const std::string instance_name{};
        explicit storage_tiering_configuration(const std::string& _instance_name);
    };
} // namespace irods

#endif // STORAGE_TIERING_CONFIGURATION_HPP
