#include <mpi.h>

#include <array>
#include <atomic>
#include <iostream>
#include <thread>
#include <vector>

namespace {

constexpr int kThreadCount = 4;
constexpr int kRounds = 64;

void exchange_loop(int rank, int thread_index, std::atomic<bool>& failed) {
  const int peer = 1 - rank;
  for (int round = 0; round < kRounds; ++round) {
    int sent = rank * 100000 + thread_index * 1000 + round;
    int received = -1;
    const int tag = 5000 + thread_index * 100 + round;
    std::array<MPI_Request, 2> requests{MPI_REQUEST_NULL, MPI_REQUEST_NULL};
    MPI_Irecv(&received, 1, MPI_INT, peer, tag, MPI_COMM_WORLD, &requests[0]);
    MPI_Isend(&sent, 1, MPI_INT, peer, tag, MPI_COMM_WORLD, &requests[1]);
    MPI_Waitall(static_cast<int>(requests.size()), requests.data(), MPI_STATUSES_IGNORE);
    const int expected = peer * 100000 + thread_index * 1000 + round;
    if (received != expected) {
      failed.store(true, std::memory_order_relaxed);
      return;
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  int provided = MPI_THREAD_SINGLE;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

  int rank = -1;
  int size = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);
  if (size != 2) {
    MPI_Finalize();
    return 2;
  }
  if (provided < MPI_THREAD_MULTIPLE) {
    if (rank == 0) std::cout << "thread-multiple=unsupported\n";
    MPI_Finalize();
    return 77;
  }

  std::atomic<bool> failed{false};
  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);
  for (int thread_index = 0; thread_index < kThreadCount; ++thread_index) {
    threads.emplace_back(exchange_loop, rank, thread_index, std::ref(failed));
  }
  for (auto& thread : threads) thread.join();

  const int local_failed = failed.load(std::memory_order_relaxed) ? 1 : 0;
  int global_failed = 0;
  MPI_Allreduce(&local_failed, &global_failed, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
  if (global_failed != 0) MPI_Abort(MPI_COMM_WORLD, 90);

  if (rank == 0) std::cout << "thread-multiple=ok\n";
  MPI_Finalize();
  return 0;
}
