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

  constexpr int partitions = 6;
  constexpr int values_per_partition = 2;
  constexpr int values = partitions * values_per_partition;
  const int peer = 1 - rank;
  std::array<int, values> sent{};
  std::array<int, values> received{};
  for (int item = 0; item < values; ++item) {
    sent[item] = rank * 100 + item;
    received[item] = -1;
  }

  MPI_Request requests[2] = {MPI_REQUEST_NULL, MPI_REQUEST_NULL};
  int result = MPI_Precv_init(received.data(), partitions, values_per_partition,
                              MPI_INT, peer, 401, MPI_COMM_WORLD, MPI_INFO_NULL,
                              &requests[0]);
  if (result == MPI_SUCCESS) {
    result = MPI_Psend_init(sent.data(), partitions, values_per_partition,
                            MPI_INT, peer, 401, MPI_COMM_WORLD, MPI_INFO_NULL,
                            &requests[1]);
  }
  if (result != MPI_SUCCESS) MPI_Abort(MPI_COMM_WORLD, 10);

  MPI_Startall(2, requests);
  MPI_Pready(0, requests[1]);
  MPI_Pready_range(1, 2, requests[1]);
  int ready_list[] = {3, 4, 5};
  MPI_Pready_list(3, ready_list, requests[1]);
  int arrived = 0;
  MPI_Parrived(requests[0], 0, &arrived);
  MPI_Waitall(2, requests, MPI_STATUSES_IGNORE);

  for (int item = 0; item < values; ++item) {
    if (received[item] != peer * 100 + item) MPI_Abort(MPI_COMM_WORLD, 20 + item);
  }
  MPI_Request_free(&requests[0]);
  MPI_Request_free(&requests[1]);

  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) std::cout << "partitioned-requests=ok\n";
  MPI_Finalize();
  return 0;
}
