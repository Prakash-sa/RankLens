add_executable(ranklens_ping_pong ping_pong.cpp)
target_compile_features(ranklens_ping_pong PRIVATE cxx_std_17)
target_link_libraries(ranklens_ping_pong PRIVATE MPI::MPI_CXX)

