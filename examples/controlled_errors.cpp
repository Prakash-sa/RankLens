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

  MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
  int value = rank + 700;
  const int invalid_peer = size;
  const int failed_send =
      MPI_Send(&value, 1, MPI_INT, invalid_peer, 901, MPI_COMM_WORLD);
  if (failed_send == MPI_SUCCESS) MPI_Abort(MPI_COMM_WORLD, 3);

  const int peer = 1 - rank;
  const int failed_datatype_send =
      MPI_Send(&value, 1, MPI_DATATYPE_NULL, peer, 903, MPI_COMM_WORLD);
  if (failed_datatype_send == MPI_SUCCESS) MPI_Abort(MPI_COMM_WORLD, 5);

  MPI_Request failed_request = MPI_REQUEST_NULL;
  const int failed_isend = MPI_Isend(&value, 1, MPI_DATATYPE_NULL, peer, 904,
                                     MPI_COMM_WORLD, &failed_request);
  if (failed_isend == MPI_SUCCESS) MPI_Abort(MPI_COMM_WORLD, 6);
  failed_request = MPI_REQUEST_NULL;
  const int failed_irecv = MPI_Irecv(&value, 1, MPI_DATATYPE_NULL, peer, 904,
                                     MPI_COMM_WORLD, &failed_request);
  if (failed_irecv == MPI_SUCCESS) MPI_Abort(MPI_COMM_WORLD, 7);

  int reduction = 0;
  const int failed_allreduce = MPI_Allreduce(
      &value, &reduction, 1, MPI_DATATYPE_NULL, MPI_SUM, MPI_COMM_WORLD);
  if (failed_allreduce == MPI_SUCCESS) MPI_Abort(MPI_COMM_WORLD, 8);

  int received = -1;
  MPI_Request requests[2] = {MPI_REQUEST_NULL, MPI_REQUEST_NULL};
  MPI_Irecv(&received, 1, MPI_INT, peer, 902, MPI_COMM_WORLD, &requests[0]);
  MPI_Isend(&value, 1, MPI_INT, peer, 902, MPI_COMM_WORLD, &requests[1]);
  MPI_Waitall(2, requests, MPI_STATUSES_IGNORE);
  if (received != peer + 700) MPI_Abort(MPI_COMM_WORLD, 9);

  MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_ARE_FATAL);
  if (rank == 0) std::cout << "controlled-errors=ok\n";
  MPI_Finalize();
  return 0;
}
