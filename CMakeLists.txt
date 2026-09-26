cmake_minimum_required(VERSION 3.20)

project(
  RankLens
  VERSION 1.1.0
  DESCRIPTION "Cross-layer observability and performance diagnosis for MPI workloads"
  LANGUAGES CXX
)

include(CTest)
include(GNUInstallDirs)

option(RANKLENS_BUILD_EXAMPLES "Build RankLens MPI examples" ON)
option(RANKLENS_BUILD_TESTS "Build and register RankLens tests" ON)

find_package(MPI REQUIRED COMPONENTS CXX)

include(CheckCXXSourceCompiles)
set(_ranklens_required_libraries "${CMAKE_REQUIRED_LIBRARIES}")
set(CMAKE_REQUIRED_LIBRARIES MPI::MPI_CXX)
check_cxx_source_compiles(
  "#include <mpi.h>
   int main() {
     MPI_Request request = MPI_REQUEST_NULL;
     return MPI_Psend_init(nullptr, 1, 0, MPI_BYTE, 0, 0, MPI_COMM_WORLD,
                           MPI_INFO_NULL, &request);
   }"
  RANKLENS_HAVE_MPI_PARTITIONED
)
set(CMAKE_REQUIRED_LIBRARIES "${_ranklens_required_libraries}")
unset(_ranklens_required_libraries)

configure_file(
  include/ranklens/version.h.in
  ${CMAKE_CURRENT_BINARY_DIR}/generated/ranklens/version.h
  @ONLY
)

add_subdirectory(src/interceptor)

if(RANKLENS_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(BUILD_TESTING AND RANKLENS_BUILD_TESTS)
  find_package(Python3 3.9 REQUIRED COMPONENTS Interpreter)

  add_test(
    NAME ranklens_python_unit
    COMMAND ${Python3_EXECUTABLE} -m unittest discover -s tests -v
  )
  set_tests_properties(
    ranklens_python_unit
    PROPERTIES
      ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/python"
      WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  )

  if(RANKLENS_BUILD_EXAMPLES)
    add_test(
      NAME ranklens_mpi_integration
      COMMAND
        ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/mpi_integration.py
        --mpiexec ${MPIEXEC_EXECUTABLE}
        --np-flag=${MPIEXEC_NUMPROC_FLAG}
        --library $<TARGET_FILE:ranklens_mpi>
        --executable $<TARGET_FILE:ranklens_ping_pong>
        --output ${CMAKE_CURRENT_BINARY_DIR}/integration-results
    )
    set_tests_properties(
      ranklens_mpi_integration
      PROPERTIES ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/python"
    )
    add_test(NAME ranklens_nonblocking_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/mpi_nonblocking.py
      ${MPIEXEC_EXECUTABLE} $<TARGET_FILE:ranklens_mpi> $<TARGET_FILE:ranklens_nonblocking>)
    add_test(NAME ranklens_completion_families_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/mpi_completion_families.py
      ${MPIEXEC_EXECUTABLE} $<TARGET_FILE:ranklens_mpi> $<TARGET_FILE:ranklens_completion_families>)
    add_test(NAME ranklens_persistent_requests_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/mpi_persistent_requests.py
      ${MPIEXEC_EXECUTABLE} $<TARGET_FILE:ranklens_mpi> $<TARGET_FILE:ranklens_persistent_requests>)
    add_test(NAME ranklens_thread_multiple_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/mpi_thread_multiple.py
      ${MPIEXEC_EXECUTABLE} $<TARGET_FILE:ranklens_mpi> $<TARGET_FILE:ranklens_thread_multiple>)
    add_test(NAME ranklens_controlled_errors_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/mpi_controlled_errors.py
      ${MPIEXEC_EXECUTABLE} $<TARGET_FILE:ranklens_mpi> $<TARGET_FILE:ranklens_controlled_errors>)
    add_test(NAME ranklens_request_saturation_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/mpi_request_saturation.py
      ${MPIEXEC_EXECUTABLE} $<TARGET_FILE:ranklens_mpi> $<TARGET_FILE:ranklens_request_saturation>)
    add_test(NAME ranklens_unavailable_consumer_integration COMMAND ${Python3_EXECUTABLE}
      ${CMAKE_CURRENT_SOURCE_DIR}/tests/mpi_unavailable_consumer.py
      ${MPIEXEC_EXECUTABLE} $<TARGET_FILE:ranklens_mpi> $<TARGET_FILE:ranklens_ping_pong>)
    if(RANKLENS_HAVE_MPI_PARTITIONED)
      add_test(NAME ranklens_partitioned_requests_integration COMMAND ${Python3_EXECUTABLE}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/mpi_partitioned_requests.py
        ${MPIEXEC_EXECUTABLE} $<TARGET_FILE:ranklens_mpi> $<TARGET_FILE:ranklens_partitioned_requests>)
      set_tests_properties(ranklens_partitioned_requests_integration PROPERTIES
        ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/python"
        TIMEOUT 60)
    endif()
    set_tests_properties(ranklens_mpi_integration ranklens_nonblocking_integration
      ranklens_completion_families_integration ranklens_persistent_requests_integration
      ranklens_thread_multiple_integration ranklens_controlled_errors_integration
      ranklens_request_saturation_integration ranklens_unavailable_consumer_integration
      PROPERTIES TIMEOUT 60)
    set_tests_properties(ranklens_nonblocking_integration ranklens_completion_families_integration
      ranklens_persistent_requests_integration ranklens_thread_multiple_integration
      ranklens_controlled_errors_integration ranklens_request_saturation_integration
      ranklens_unavailable_consumer_integration
      PROPERTIES ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/python")
  endif()
endif()

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/generated/ranklens/version.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/ranklens
)
