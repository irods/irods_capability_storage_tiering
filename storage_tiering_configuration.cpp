
#include "storage_tiering_configuration.hpp"
#include "irods_server_properties.hpp"

namespace irods {
    storage_tiering_configuration::storage_tiering_configuration(
        const std::string& _instance_name ) :
        instance_name{_instance_name} {
        bool success_flag = false;

        try {
            const auto& rule_engines = get_server_property<
                const std::vector<boost::any>&>(
                        std::vector<std::string>{
                        CFG_PLUGIN_CONFIGURATION_KW,
                        PLUGIN_TYPE_RULE_ENGINE});
            for ( const auto& elem : rule_engines ) {
                const auto& rule_engine = boost::any_cast< const std::unordered_map< std::string, boost::any >& >( elem );
                const auto& inst_name   = boost::any_cast< const std::string& >( rule_engine.at( CFG_INSTANCE_NAME_KW ) );
                if ( inst_name == _instance_name ) {
                    if(rule_engine.count(CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW) > 0) {
                        const auto& plugin_spec_cfg = boost::any_cast<const std::unordered_map<std::string, boost::any>&>(
                                rule_engine.at(CFG_PLUGIN_SPECIFIC_CONFIGURATION_KW));
                        if(plugin_spec_cfg.find("access_time_attribute") != plugin_spec_cfg.end()) {
                            access_time_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("access_time_attribute"));
                        }

                        if(plugin_spec_cfg.find("group_attribute") != plugin_spec_cfg.end()) {
                            group_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("group_attribute"));
                        }

                        if(plugin_spec_cfg.find("time_attribute") != plugin_spec_cfg.end()) {
                            time_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("time_attribute"));
                        }

                        if(plugin_spec_cfg.find("query_attribute") != plugin_spec_cfg.end()) {
                            query_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("query_attribute"));
                        }

                        if(plugin_spec_cfg.find("verification_attribute") != plugin_spec_cfg.end()) {
                            verification_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("verification_attribute"));
                        }

                        if(plugin_spec_cfg.find("data_movement_parameters_attribute") != plugin_spec_cfg.end()) {
                            data_movement_parameters_attribute = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("data_movement_parameters_attribute"));
                        }

                        if(plugin_spec_cfg.find("minimum_restage_tier") != plugin_spec_cfg.end()) {
                            minimum_restage_tier = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("minimum_restage_tier"));
                        }

                        if(plugin_spec_cfg.find("preserve_replicas") != plugin_spec_cfg.end()) {
                            preserve_replicas = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("preserve_replicas"));
                        }

                        if(plugin_spec_cfg.find("object_limit") != plugin_spec_cfg.end()) {
                            object_limit = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("object_limit"));
                        }

                        if(plugin_spec_cfg.find("default_data_movement_parameters") != plugin_spec_cfg.end()) {
                            default_data_movement_parameters = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("default_data_movement_parameters"));
                        }

                        if(plugin_spec_cfg.find("minimum_delay_time") != plugin_spec_cfg.end()) {
                            default_data_movement_parameters = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("minimum_delay_time"));
                        }

                        if(plugin_spec_cfg.find("maximum_delay_time") != plugin_spec_cfg.end()) {
                            default_data_movement_parameters = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("maximum_delay_time"));
                        }

                        if(plugin_spec_cfg.find("time_check_string") != plugin_spec_cfg.end()) {
                            time_check_string = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at("time_check_string"));
                        }

                        if(plugin_spec_cfg.find("number_of_scheduling_threads") != plugin_spec_cfg.end()) {
                            number_of_scheduling_threads = boost::any_cast<int>(
                                    plugin_spec_cfg.at("number_of_scheduling_threads"));
                        }

                        if(plugin_spec_cfg.find(data_transfer_log_level_key) != plugin_spec_cfg.end()) {
                            const std::string val = boost::any_cast<std::string>(
                                    plugin_spec_cfg.at(data_transfer_log_level_key));
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

