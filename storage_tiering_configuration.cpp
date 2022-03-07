
#include "storage_tiering_configuration.hpp"
#include <irods/irods_server_properties.hpp>

namespace irods {
    storage_tiering_configuration::storage_tiering_configuration(
        const std::string& _instance_name ) :
        instance_name{_instance_name} {
        bool success_flag = false;

        try {
            const auto& rule_engines = get_server_property<
                const nlohmann::json&>(
                        std::vector<std::string>{
                        CFG_PLUGIN_CONFIGURATION_KW,
                        PLUGIN_TYPE_RULE_ENGINE});
            for ( const auto& rule_engine : rule_engines ) {
                const auto& inst_name = rule_engine.at( CFG_INSTANCE_NAME_KW ).get_ref<const std::string&>();
                if ( inst_name == _instance_name ) {
                    if(rule_engine.count(CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW) > 0) {
                        const auto& plugin_spec_cfg = rule_engine.at(CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW);
                        if(plugin_spec_cfg.find("access_time_attribute") != plugin_spec_cfg.end()) {
                            access_time_attribute = plugin_spec_cfg.at("access_time_attribute").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("group_attribute") != plugin_spec_cfg.end()) {
                            group_attribute = plugin_spec_cfg.at("group_attribute").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("time_attribute") != plugin_spec_cfg.end()) {
                            time_attribute = plugin_spec_cfg.at("time_attribute").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("query_attribute") != plugin_spec_cfg.end()) {
                            query_attribute = plugin_spec_cfg.at("query_attribute").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("verification_attribute") != plugin_spec_cfg.end()) {
                            verification_attribute = plugin_spec_cfg.at("verification_attribute").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("data_movement_parameters_attribute") != plugin_spec_cfg.end()) {
                            data_movement_parameters_attribute = plugin_spec_cfg.at("data_movement_parameters_attribute").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("minimum_restage_tier") != plugin_spec_cfg.end()) {
                            minimum_restage_tier = plugin_spec_cfg.at("minimum_restage_tier").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("preserve_replicas") != plugin_spec_cfg.end()) {
                            preserve_replicas = plugin_spec_cfg.at("preserve_replicas").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("object_limit") != plugin_spec_cfg.end()) {
                            object_limit = plugin_spec_cfg.at("object_limit").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("default_data_movement_parameters") != plugin_spec_cfg.end()) {
                            default_data_movement_parameters = plugin_spec_cfg.at("default_data_movement_parameters").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("minimum_delay_time") != plugin_spec_cfg.end()) {
                            default_data_movement_parameters = plugin_spec_cfg.at("minimum_delay_time").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("maximum_delay_time") != plugin_spec_cfg.end()) {
                            default_data_movement_parameters = plugin_spec_cfg.at("maximum_delay_time").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("time_check_string") != plugin_spec_cfg.end()) {
                            time_check_string = plugin_spec_cfg.at("time_check_string").get<std::string>();
                        }

                        if(plugin_spec_cfg.find("number_of_scheduling_threads") != plugin_spec_cfg.end()) {
                            number_of_scheduling_threads = plugin_spec_cfg.at("number_of_scheduling_threads").get<int>();
                        }

                        if(plugin_spec_cfg.find(data_transfer_log_level_key) != plugin_spec_cfg.end()) {
                            const std::string& val = plugin_spec_cfg.at(data_transfer_log_level_key).get_ref<const std::string&>();
                            if("LOG_NOTICE" == val) {
                                data_transfer_log_level_value = LOG_NOTICE;
                            }
                        }
                    }

                    success_flag = true;
                } // if inst_name
            } // for rule_engines
        } catch ( const boost::bad_any_cast& e ) {
            THROW( INVALID_ANY_CAST, e.what() );
        } catch ( const std::out_of_range& e ) {
            THROW( KEY_NOT_FOUND, e.what() );
        }

        if(!success_flag) {
            THROW(
                SYS_INVALID_INPUT_PARAM,
                boost::format("failed to find configuration for storage_tiering plugin [%s]") %
                _instance_name);
        }
    } // ctor storage_tiering_configuration

} // namepsace irods

