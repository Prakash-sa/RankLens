add_library(ranklens_mpi SHARED ranklens_mpi.cpp)
add_library(RankLens::mpi ALIAS ranklens_mpi)

find_package(Threads REQUIRED)

target_compile_features(ranklens_mpi PRIVATE cxx_std_17)
target_compile_options(
  ranklens_mpi
  PRIVATE
    $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall;-Wextra;-Wpedantic>
)
target_include_directories(
  ranklens_mpi
  PRIVATE
    ${PROJECT_SOURCE_DIR}/include
    ${PROJECT_BINARY_DIR}/generated
)
target_link_libraries(ranklens_mpi PRIVATE MPI::MPI_CXX Threads::Threads)
if(RANKLENS_HAVE_MPI_PARTITIONED)
  target_compile_definitions(ranklens_mpi PRIVATE RANKLENS_HAVE_MPI_PARTITIONED=1)
endif()
if(BUILD_TESTING AND RANKLENS_BUILD_TESTS)
  target_compile_definitions(ranklens_mpi PRIVATE RANKLENS_ENABLE_TEST_FAULTS=1)
endif()

set_target_properties(
  ranklens_mpi
  PROPERTIES
    OUTPUT_NAME ranklens_mpi
    CXX_VISIBILITY_PRESET default
    VISIBILITY_INLINES_HIDDEN OFF
)

install(
  TARGETS ranklens_mpi
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
