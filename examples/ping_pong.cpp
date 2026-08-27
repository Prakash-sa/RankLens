#include <mpi.h>

#include <array>
#include <chrono>
#include <iostream>
#include <thread>

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);

  int rank = 0;
  int size = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  if (size < 2) {
    if (rank == 0) {
      std::cerr << "ranklens_ping_pong requires at least two ranks\n";
    }
    MPI_Finalize();
    return 2;
  }

  std::array<double, 128> payload{};
  for (int iteration = 0; iteration < 20; ++iteration) {
    if (rank == 0) {
      MPI_Send(payload.data(), static_cast<int>(payload.size()), MPI_DOUBLE, 1,
               iteration, MPI_COMM_WORLD);
      MPI_Recv(payload.data(), static_cast<int>(payload.size()), MPI_DOUBLE, 1,
               iteration, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    } else if (rank == 1) {
      MPI_Recv(payload.data(), static_cast<int>(payload.size()), MPI_DOUBLE, 0,
               iteration, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Send(payload.data(), static_cast<int>(payload.size()), MPI_DOUBLE, 0,
               iteration, MPI_COMM_WORLD);
    }

    double local = static_cast<double>(rank + iteration);
    double global = 0.0;
    MPI_Allreduce(&local, &global, 1, MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);
    MPI_Bcast(&global, 1, MPI_DOUBLE, 0, MPI_COMM_WORLD);
    MPI_Barrier(MPI_COMM_WORLD);
  }

  if (rank == 0) {
    std::cout << "RankLens ping-pong example completed on " << size << " ranks\n";
  }

  MPI_Finalize();
  return 0;
}

