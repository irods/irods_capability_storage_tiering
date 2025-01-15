set(_IRODS_TEST_EXECUTABLES_BUILD_DOCSTR "Build and package executables made for testing.")

# Building the test executables is enabled by default for Debug builds.
if (CMAKE_BUILD_TYPE STREQUAL "Debug")
	set(_IRODS_TEST_EXECUTABLES_BUILD_DEFAULT YES)
else()
	set(_IRODS_TEST_EXECUTABLES_BUILD_DEFAULT NO)
endif()

set(IRODS_TEST_EXECUTABLES_BUILD ${_IRODS_TEST_EXECUTABLES_BUILD_DEFAULT} CACHE BOOL "${_IRODS_TEST_EXECUTABLES_BUILD_DOCSTR}")

if (NOT IRODS_TEST_EXECUTABLES_BUILD)
	return()
endif()

add_subdirectory(stream_test)
