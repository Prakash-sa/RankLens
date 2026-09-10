#include <mpi.h>
#include <iostream>

int main(int argc, char** argv) {
  MPI_Init(&argc, &argv);
  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  if (size != 2) { MPI_Finalize(); return 2; }
  MPI_Comm reversed;
  MPI_Comm_split(MPI_COMM_WORLD, 0, 1 - rank, &reversed);
  int local;
  MPI_Comm_rank(reversed, &local);
  int sent = rank + 10, received = -1;
  MPI_Request requests[2];
  MPI_Irecv(&received, 1, MPI_INT, MPI_ANY_SOURCE, 7, reversed, &requests[0]);
  MPI_Isend(&sent, 1, MPI_INT, 1 - local, 7, reversed, &requests[1]);
  MPI_Waitall(2, requests, MPI_STATUSES_IGNORE);
  if (received != 11 - rank) MPI_Abort(MPI_COMM_WORLD, 3);
  // A freed datatype must not invalidate telemetry for an outstanding receive.
  MPI_Datatype datatype;
  MPI_Type_contiguous(1, MPI_INT, &datatype);
  MPI_Type_commit(&datatype);
  MPI_Irecv(&received, 1, datatype, 1 - local, 8, reversed, &requests[0]);
  MPI_Isend(&sent, 1, MPI_INT, 1 - local, 8, reversed, &requests[1]);
  MPI_Type_free(&datatype);
  MPI_Wait(&requests[1], MPI_STATUS_IGNORE);
  int done = 0;
  while (!done) MPI_Test(&requests[0], &done, MPI_STATUS_IGNORE);
  if (received != 11 - rank) MPI_Abort(MPI_COMM_WORLD, 4);
  MPI_Send(&sent, 1, MPI_INT, MPI_PROC_NULL, 0, reversed);
  MPI_Comm_free(&reversed);
  if (rank == 0) std::cout << "checksum=21\n";
  MPI_Finalize();
  return 0;
}
