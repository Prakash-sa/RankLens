#include <mpi.h>

#include <array>
#include <iostream>

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);

  int rank = -1;
  int size = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  if (size != 2) {
    MPI_Finalize();
    return 2;
  }

  constexpr int request_pairs = 4;
  const int peer = 1 - rank;
  std::array<int, request_pairs> sent{};
  std::array<int, request_pairs> received{};
  std::array<MPI_Request, request_pairs * 2> requests{};
  requests.fill(MPI_REQUEST_NULL);
  for (int item = 0; item < request_pairs; ++item) {
    sent[item] = rank * 100 + item;
    received[item] = -1;
    MPI_Irecv(&received[item], 1, MPI_INT, peer, 500 + item, MPI_COMM_WORLD,
              &requests[item]);
  }
  for (int item = 0; item < request_pairs; ++item) {
    MPI_Isend(&sent[item], 1, MPI_INT, peer, 500 + item, MPI_COMM_WORLD,
              &requests[request_pairs + item]);
  }
  MPI_Waitall(static_cast<int>(requests.size()), requests.data(), MPI_STATUSES_IGNORE);

  for (int item = 0; item < request_pairs; ++item) {
    if (received[item] != peer * 100 + item) MPI_Abort(MPI_COMM_WORLD, 10 + item);
  }
  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) std::cout << "request-saturation=ok\n";
  MPI_Finalize();
  return 0;
}
