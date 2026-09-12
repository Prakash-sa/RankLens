#include <mpi.h>

#include <iostream>
#include <vector>

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

  std::vector<char> buffer(MPI_BSEND_OVERHEAD + sizeof(int));
  MPI_Buffer_attach(buffer.data(), static_cast<int>(buffer.size()));
  MPI_Request buffered[2] = {MPI_REQUEST_NULL, MPI_REQUEST_NULL};
  MPI_Recv_init(&received, 1, MPI_INT, peer, 303, MPI_COMM_WORLD, &buffered[0]);
  MPI_Bsend_init(&sent, 1, MPI_INT, peer, 303, MPI_COMM_WORLD, &buffered[1]);
  sent = rank + 200;
  received = -1;
  MPI_Startall(2, buffered);
  MPI_Waitall(2, buffered, MPI_STATUSES_IGNORE);
  if (received != peer + 200) MPI_Abort(MPI_COMM_WORLD, 30);
  MPI_Request_free(&buffered[0]);
  MPI_Request_free(&buffered[1]);
  void* detached = nullptr;
  int detached_size = 0;
  MPI_Buffer_detach(&detached, &detached_size);

  MPI_Request synchronous[2] = {MPI_REQUEST_NULL, MPI_REQUEST_NULL};
  MPI_Recv_init(&received, 1, MPI_INT, peer, 304, MPI_COMM_WORLD, &synchronous[0]);
  MPI_Ssend_init(&sent, 1, MPI_INT, peer, 304, MPI_COMM_WORLD, &synchronous[1]);
  sent = rank + 300;
  received = -1;
  MPI_Startall(2, synchronous);
  MPI_Waitall(2, synchronous, MPI_STATUSES_IGNORE);
  if (received != peer + 300) MPI_Abort(MPI_COMM_WORLD, 40);
  MPI_Request_free(&synchronous[0]);
  MPI_Request_free(&synchronous[1]);

  MPI_Request ready_recv = MPI_REQUEST_NULL;
  MPI_Request ready_send = MPI_REQUEST_NULL;
  MPI_Recv_init(&received, 1, MPI_INT, peer, 305, MPI_COMM_WORLD, &ready_recv);
  MPI_Rsend_init(&sent, 1, MPI_INT, peer, 305, MPI_COMM_WORLD, &ready_send);
  sent = rank + 400;
  received = -1;
  MPI_Start(&ready_recv);
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Start(&ready_send);
  MPI_Wait(&ready_recv, MPI_STATUS_IGNORE);
  MPI_Wait(&ready_send, MPI_STATUS_IGNORE);
  if (received != peer + 400) MPI_Abort(MPI_COMM_WORLD, 50);
  MPI_Request_free(&ready_recv);
  MPI_Request_free(&ready_send);

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
