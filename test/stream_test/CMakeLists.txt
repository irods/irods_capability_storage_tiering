set(
	OPERATIONS
	read
	create
)

foreach(operation IN LISTS OPERATIONS)
	set(target_name "irods_test_${operation}_object")
	add_executable(
		${target_name}
		"${CMAKE_CURRENT_SOURCE_DIR}/irods_test_${operation}_object.cpp"
	)
	target_link_libraries(
		${target_name}
		PRIVATE
		irods_client
		irods_common
		irods_plugin_dependencies
		"${IRODS_EXTERNALS_FULLPATH_BOOST}/lib/libboost_program_options.so"
	)
	target_include_directories(
		${target_name}
		PRIVATE
		"${IRODS_EXTERNALS_FULLPATH_BOOST}/include"
	)
	target_compile_definitions(
		${target_name}
		PRIVATE
		${IRODS_COMPILE_DEFINITIONS}
		${IRODS_COMPILE_DEFINITIONS_PRIVATE}
	)
	install(
		TARGETS
		${target_name}
		RUNTIME
		DESTINATION "${CMAKE_INSTALL_SBINDIR}"
		COMPONENT "${IRODS_POLICY_PACKAGE_COMPONENT}"
		PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
	)
endforeach()
