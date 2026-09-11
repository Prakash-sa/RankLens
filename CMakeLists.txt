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
    set_tests_properties(ranklens_mpi_integration ranklens_nonblocking_integration
      ranklens_completion_families_integration ranklens_persistent_requests_integration
      PROPERTIES TIMEOUT 60)
    set_tests_properties(ranklens_nonblocking_integration ranklens_completion_families_integration
      ranklens_persistent_requests_integration
      PROPERTIES ENVIRONMENT "PYTHONPATH=${CMAKE_CURRENT_SOURCE_DIR}/python")
  endif()
endif()

install(
  FILES ${CMAKE_CURRENT_BINARY_DIR}/generated/ranklens/version.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/ranklens
)
