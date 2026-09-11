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
