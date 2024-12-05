#include "irods/private/storage_tiering/configuration.hpp"

#include <irods/irods_server_properties.hpp>

#include <fmt/format.h>

namespace irods
{
	storage_tiering_configuration::storage_tiering_configuration(const std::string& _instance_name)
		: instance_name{_instance_name}
	{
		try {
			const auto& rule_engines = get_server_property<const nlohmann::json&>(
				std::vector<std::string>{KW_CFG_PLUGIN_CONFIGURATION, KW_CFG_PLUGIN_TYPE_RULE_ENGINE});

			for (const auto& rule_engine : rule_engines) {
				// Loop until the provided instance name is found.
				if (const auto& inst_name = rule_engine.at(KW_CFG_INSTANCE_NAME).get_ref<const std::string&>();
				    inst_name != _instance_name)
				{
					continue;
				}

				// Get the plugin-specific configuration stanza for this instance...
				const auto config = rule_engine.find(KW_CFG_PLUGIN_SPECIFIC_CONFIGURATION);
				if (rule_engine.end() == config) {
					// If no plugin_specific_configuration is defined, use the default configuration. There is no need
					// to check other configured REPs because this is the instance we were looking for - just return.
					return;
				}

				// Override defaults with configured values.

				if (const auto attr = config->find("access_time_attribute"); attr != config->end()) {
					access_time_attribute = attr->get<std::string>();
				}

				if (const auto attr = config->find("group_attribute"); attr != config->end()) {
					group_attribute = attr->get<std::string>();
				}

				if (const auto attr = config->find("time_attribute"); attr != config->end()) {
					time_attribute = attr->get<std::string>();
				}

				if (const auto attr = config->find("query_attribute"); attr != config->end()) {
					query_attribute = attr->get<std::string>();
				}

				if (const auto attr = config->find("verification_attribute"); attr != config->end()) {
					verification_attribute = attr->get<std::string>();
				}

				if (const auto attr = config->find("data_movement_parameters_attribute"); attr != config->end()) {
					data_movement_parameters_attribute = attr->get<std::string>();
				}

				if (const auto attr = config->find("minimum_restage_tier"); attr != config->end()) {
					minimum_restage_tier = attr->get<std::string>();
				}

				if (const auto attr = config->find("preserve_replicas"); attr != config->end()) {
					preserve_replicas = attr->get<std::string>();
				}

				if (const auto attr = config->find("object_limit"); attr != config->end()) {
					object_limit = attr->get<std::string>();
				}

				if (const auto attr = config->find("default_data_movement_parameters"); attr != config->end()) {
					default_data_movement_parameters = attr->get<std::string>();
				}

				if (const auto attr = config->find("minimum_delay_time"); attr != config->end()) {
					default_data_movement_parameters = attr->get<std::string>();
				}

				if (const auto attr = config->find("maximum_delay_time"); attr != config->end()) {
					default_data_movement_parameters = attr->get<std::string>();
				}

				if (const auto attr = config->find("time_check_string"); attr != config->end()) {
					time_check_string = attr->get<std::string>();
				}

				if (const auto attr = config->find("number_of_scheduling_threads"); attr != config->end()) {
					number_of_scheduling_threads = attr->get<int>();
				}

				if (const auto attr = config->find(data_transfer_log_level_key); attr != config->end()) {
					const std::string& val = attr->get_ref<const std::string&>();
					if ("LOG_NOTICE" == val) {
						data_transfer_log_level_value = LOG_NOTICE;
					}
				}

				// Only one configuration is considered for a given instance of a REP, so just return here.
				return;
			}
		}
		catch (const boost::bad_any_cast& e) {
			THROW(INVALID_ANY_CAST, e.what());
		}
		catch (const std::out_of_range& e) {
			THROW(KEY_NOT_FOUND, e.what());
		}

		// No valid configuration was found for the given instance name if we reach this point, which is an error.
		THROW(SYS_INVALID_INPUT_PARAM,
		      fmt::format("Failed to find configuration for storage_tiering plugin [{}].", _instance_name));
	} // storage_tiering_configuration constructor
} //namespace irods
