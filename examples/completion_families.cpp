#include <mpi.h>

#include <array>
#include <iostream>

namespace {

void post_pair(int rank, int tag, int& received, std::array<MPI_Request, 2>& requests) {
  const int peer = 1 - rank;
  static thread_local int sent;
  sent = rank * 1000 + tag;
  received = -1;
  MPI_Irecv(&received, 1, MPI_INT, peer, tag, MPI_COMM_WORLD, &requests[0]);
  MPI_Isend(&sent, 1, MPI_INT, peer, tag, MPI_COMM_WORLD, &requests[1]);
}

void verify(int rank, int tag, int received) {
  const int expected = (1 - rank) * 1000 + tag;
  if (received != expected) MPI_Abort(MPI_COMM_WORLD, tag);
}

}  // namespace

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

  std::array<MPI_Request, 2> requests{};
  int received = -1;

  post_pair(rank, 101, received, requests);
  int flag = 0;
  while (!flag) MPI_Testall(2, requests.data(), &flag, MPI_STATUSES_IGNORE);
  verify(rank, 101, received);

  post_pair(rank, 102, received, requests);
  for (int remaining = 2; remaining > 0; --remaining) {
    int index = MPI_UNDEFINED;
    MPI_Waitany(2, requests.data(), &index, MPI_STATUS_IGNORE);
    if (index == MPI_UNDEFINED) MPI_Abort(MPI_COMM_WORLD, 102);
  }
  verify(rank, 102, received);

  post_pair(rank, 103, received, requests);
  int completed_total = 0;
  while (completed_total < 2) {
    int outcount = 0;
    int indices[2] = {MPI_UNDEFINED, MPI_UNDEFINED};
    MPI_Waitsome(2, requests.data(), &outcount, indices, MPI_STATUSES_IGNORE);
    if (outcount == MPI_UNDEFINED) break;
    completed_total += outcount;
  }
  if (completed_total != 2) MPI_Abort(MPI_COMM_WORLD, 103);
  verify(rank, 103, received);

  post_pair(rank, 104, received, requests);
  completed_total = 0;
  while (completed_total < 2) {
    int index = MPI_UNDEFINED;
    flag = 0;
    MPI_Testany(2, requests.data(), &index, &flag, MPI_STATUS_IGNORE);
    if (flag && index != MPI_UNDEFINED) ++completed_total;
  }
  verify(rank, 104, received);

  post_pair(rank, 105, received, requests);
  completed_total = 0;
  while (completed_total < 2) {
    int outcount = 0;
    int indices[2] = {MPI_UNDEFINED, MPI_UNDEFINED};
    MPI_Testsome(2, requests.data(), &outcount, indices, MPI_STATUSES_IGNORE);
    if (outcount == MPI_UNDEFINED) break;
    completed_total += outcount;
  }
  if (completed_total != 2) MPI_Abort(MPI_COMM_WORLD, 105);
  verify(rank, 105, received);

  if (rank == 0) std::cout << "completion-families=ok\n";
  MPI_Finalize();
  return 0;
}
