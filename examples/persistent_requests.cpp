#include <mpi.h>

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

  const int peer = 1 - rank;
  int sent = rank + 100;
  int received = -1;
  MPI_Request requests[2] = {MPI_REQUEST_NULL, MPI_REQUEST_NULL};

  MPI_Recv_init(&received, 1, MPI_INT, peer, 301, MPI_COMM_WORLD, &requests[0]);
  MPI_Send_init(&sent, 1, MPI_INT, peer, 301, MPI_COMM_WORLD, &requests[1]);
  for (int round = 0; round < 3; ++round) {
    sent = rank + 100 + round;
    received = -1;
    MPI_Startall(2, requests);
    MPI_Waitall(2, requests, MPI_STATUSES_IGNORE);
    if (received != peer + 100 + round) MPI_Abort(MPI_COMM_WORLD, 10 + round);
  }
  MPI_Request_free(&requests[0]);
  MPI_Request_free(&requests[1]);

  int cancelled_value = -1;
  MPI_Request cancelled = MPI_REQUEST_NULL;
  MPI_Recv_init(&cancelled_value, 1, MPI_INT, peer, 302, MPI_COMM_WORLD, &cancelled);
  MPI_Start(&cancelled);
  MPI_Cancel(&cancelled);
  MPI_Wait(&cancelled, MPI_STATUS_IGNORE);
  MPI_Request_free(&cancelled);

  MPI_Barrier(MPI_COMM_WORLD);
  if (rank == 0) std::cout << "persistent-requests=ok\n";
  MPI_Finalize();
  return 0;
}
