#include <mpi.h>

#include <array>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <system_error>
#include <vector>

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
#include <sys/resource.h>
#if defined(__linux__)
#include <sched.h>
#endif
#endif

namespace ranklens {
namespace {

using Clock = std::chrono::steady_clock;

struct OperationStats {
  std::uint64_t calls = 0;
  std::uint64_t duration_ns = 0;
  std::uint64_t payload_bytes = 0;
};

std::string json_escape(const std::string& value) {
  std::ostringstream escaped;
  for (const unsigned char character : value) {
    switch (character) {
      case '"':
        escaped << "\\\"";
        break;
      case '\\':
        escaped << "\\\\";
        break;
      case '\b':
        escaped << "\\b";
        break;
      case '\f':
        escaped << "\\f";
        break;
      case '\n':
        escaped << "\\n";
        break;
      case '\r':
        escaped << "\\r";
        break;
      case '\t':
        escaped << "\\t";
        break;
      default:
        if (character < 0x20) {
          escaped << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                  << static_cast<int>(character) << std::dec;
        } else {
          escaped << character;
        }
    }
  }
  return escaped.str();
}

bool env_enabled(const char* name, bool default_value) {
  const char* raw = std::getenv(name);
  if (raw == nullptr) {
    return default_value;
  }
  const std::string value(raw);
  return value != "0" && value != "false" && value != "FALSE" && value != "off";
}

std::string env_value(const char* name) {
  const char* value = std::getenv(name);
  return value ? value : "";
}

std::uint64_t event_limit() {
  const std::string value = env_value("RANKLENS_MAX_EVENTS");
  if (value.empty()) return 100000;
  try { return std::min<std::uint64_t>(std::stoull(value), 10000000); }
  catch (...) { return 100000; }
}

// Translate communicator-local peers without introducing MPI collectives.
int world_peer(MPI_Comm communicator, int peer) {
  if (peer < 0 || communicator == MPI_COMM_WORLD) return peer;
  MPI_Group group = MPI_GROUP_NULL, world = MPI_GROUP_NULL;
  int translated = MPI_UNDEFINED, inter = 0;
  PMPI_Comm_test_inter(communicator, &inter);
  if (inter) PMPI_Comm_remote_group(communicator, &group);
  else PMPI_Comm_group(communicator, &group);
  PMPI_Comm_group(MPI_COMM_WORLD, &world);
  if (group != MPI_GROUP_NULL && world != MPI_GROUP_NULL)
    PMPI_Group_translate_ranks(group, 1, &peer, world, &translated);
  if (group != MPI_GROUP_NULL) PMPI_Group_free(&group);
  if (world != MPI_GROUP_NULL) PMPI_Group_free(&world);
  return translated;
}

std::uint64_t elapsed_ns(Clock::time_point start, Clock::time_point end) {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count());
}

long long payload_size(int count, MPI_Datatype datatype) {
  if (count <= 0 || datatype == MPI_DATATYPE_NULL) {
    return 0;
  }
  int datatype_size = 0;
  if (PMPI_Type_size(datatype, &datatype_size) != MPI_SUCCESS || datatype_size < 0) {
    return 0;
  }
  return static_cast<long long>(count) * static_cast<long long>(datatype_size);
}

std::string hostname() {
  std::array<char, 256> buffer{};
#if defined(_WIN32)
  return "unknown";
#else
  if (gethostname(buffer.data(), buffer.size() - 1) != 0) {
    return "unknown";
  }
  return std::string(buffer.data());
#endif
}

int process_id() {
#if defined(_WIN32)
  return _getpid();
#else
  return static_cast<int>(getpid());
#endif
}

class Recorder {
 public:
  Recorder(int rank, int world_size)
      : rank_(rank),
        world_size_(world_size),
        host_(hostname()),
        pid_(process_id()),
        started_at_(Clock::now()),
        trace_events_(env_enabled("RANKLENS_TRACE_EVENTS", true)) {
    const char* configured_output = std::getenv("RANKLENS_OUTPUT_DIR");
    output_directory_ = configured_output == nullptr ? "ranklens-results" : configured_output;

    std::error_code error;
    std::filesystem::create_directories(output_directory_, error);
    if (error) {
      disabled_ = true;
      return;
    }

    if (trace_events_) {
      event_stream_.open(rank_path("events.jsonl"), std::ios::out | std::ios::trunc);
      if (!event_stream_) {
        trace_events_ = false;
      }
    }
    snapshot(false);
  }

  void record(const char* operation, Clock::time_point start, Clock::time_point end,
              long long bytes, int peer, int tag, int error_code, long long communicator = 0,
              long long request_id = -1) {
    if (disabled_) {
      return;
    }

    const std::uint64_t duration = elapsed_ns(start, end);
    const std::uint64_t timestamp = elapsed_ns(started_at_, start);
    const std::uint64_t safe_bytes = bytes > 0 ? static_cast<std::uint64_t>(bytes) : 0;

    std::lock_guard<std::mutex> lock(mutex_);
    OperationStats& stats = operations_[operation];
    ++stats.calls;
    stats.duration_ns += duration;
    stats.payload_bytes += safe_bytes;
    mpi_time_ns_ += duration;

    const std::string op(operation);
    if (op == "MPI_Send" || op == "MPI_Isend") {
      bytes_sent_ += safe_bytes;
    } else if (op == "MPI_Recv" || op == "MPI_Irecv_complete") {
      bytes_received_ += safe_bytes;
    }

    if (trace_events_ && event_stream_ && events_written_ < max_events_) {
      ++events_written_;
      event_stream_ << "{\"schema_version\":1,\"timestamp_ns\":" << timestamp
                    << ",\"rank\":" << rank_ << ",\"operation\":\""
                    << json_escape(operation) << "\",\"duration_ns\":" << duration
                    << ",\"payload_bytes\":" << safe_bytes << ",\"peer\":" << peer
                    << ",\"tag\":" << tag << ",\"error_code\":" << error_code
                    << ",\"communicator\":" << communicator << ",\"request_id\":" << request_id << "}\n";
    } else if (trace_events_) {
      ++events_dropped_;
    }
    if (elapsed_ns(last_snapshot_, end) >= 1000000000) {
      snapshot(false);
      last_snapshot_ = end;
    }
  }

  void finalize() {
    if (disabled_) {
      return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    snapshot(true);
    if (event_stream_) {
      event_stream_.flush();
      event_stream_.close();
    }

  }

 private:
  // Atomic checkpoints keep the previous valid summary if a job dies during a write.
  void snapshot(bool complete) {
    const std::uint64_t runtime_ns = elapsed_ns(started_at_, Clock::now());
    if (event_stream_) event_stream_.flush();
    std::ofstream summary(rank_path("summary.json.tmp"), std::ios::out | std::ios::trunc);
    if (!summary) {
      return;
    }

    summary << "{\n"
            << "  \"schema_version\": 1,\n"
            << "  \"complete\": " << (complete ? "true" : "false") << ",\n"
            << "  \"context\": {\"run_id\": \"" << json_escape(env_value("RANKLENS_RUN_ID"))
            << "\", \"slurm_job_id\": \"" << json_escape(env_value("SLURM_JOB_ID"))
            << "\", \"slurm_step_id\": \"" << json_escape(env_value("SLURM_STEP_ID"))
            << "\", \"flux_job_id\": \"" << json_escape(env_value("FLUX_JOB_ID"))
            << "\", \"traceparent\": \"" << json_escape(env_value("TRACEPARENT"))
            << "\", \"events_dropped\": \"" << events_dropped_
            << "\", \"tracing_enabled\": \"" << (trace_events_ ? "true" : "false") << "\"";
#if !defined(_WIN32)
    struct rusage usage {};
    if (getrusage(RUSAGE_SELF, &usage) == 0) {
      const auto rss = static_cast<std::uint64_t>(usage.ru_maxrss);
      summary << ", \"max_rss_bytes\": \"" << rss
#if !defined(__APPLE__)
          * 1024
#endif
          << "\", \"user_cpu_us\": \"" << usage.ru_utime.tv_sec * 1000000LL + usage.ru_utime.tv_usec
          << "\", \"system_cpu_us\": \"" << usage.ru_stime.tv_sec * 1000000LL + usage.ru_stime.tv_usec << "\"";
    }
#endif
#if defined(__linux__)
    cpu_set_t cpus;
    if (sched_getaffinity(0, sizeof(cpus), &cpus) == 0) {
      summary << ", \"cpu_affinity\": \"";
      bool first = true;
      for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu) if (CPU_ISSET(cpu, &cpus)) {
        if (!first) summary << ',';
        summary << cpu;
        first = false;
      }
      summary << "\"";
    }
#endif
    summary << "},\n"
            << "  \"rank\": " << rank_ << ",\n"
            << "  \"world_size\": " << world_size_ << ",\n"
            << "  \"hostname\": \"" << json_escape(host_) << "\",\n"
            << "  \"pid\": " << pid_ << ",\n"
            << "  \"runtime_ns\": " << runtime_ns << ",\n"
            << "  \"mpi_time_ns\": " << mpi_time_ns_ << ",\n"
            << "  \"mpi_calls\": " << total_calls() << ",\n"
            << "  \"bytes_sent\": " << bytes_sent_ << ",\n"
            << "  \"bytes_received\": " << bytes_received_ << ",\n"
            << "  \"operations\": {\n";

    std::size_t index = 0;
    for (const auto& [name, stats] : operations_) {
      summary << "    \"" << json_escape(name) << "\": {\"calls\": " << stats.calls
              << ", \"duration_ns\": " << stats.duration_ns
              << ", \"payload_bytes\": " << stats.payload_bytes << "}";
      summary << (++index == operations_.size() ? "\n" : ",\n");
    }
    summary << "  }\n}\n";
    summary.close();
    if (summary) {
      std::error_code error;
      std::filesystem::rename(rank_path("summary.json.tmp"), rank_path("summary.json"), error);
    }
  }

 private:
  std::filesystem::path rank_path(const char* suffix) const {
    std::ostringstream filename;
    filename << "rank-" << std::setw(5) << std::setfill('0') << rank_ << '-' << suffix;
    return output_directory_ / filename.str();
  }

  std::uint64_t total_calls() const {
    std::uint64_t result = 0;
    for (const auto& [name, stats] : operations_) {
      (void)name;
      result += stats.calls;
    }
    return result;
  }

  int rank_ = 0;
  int world_size_ = 1;
  std::string host_;
  int pid_ = 0;
  Clock::time_point started_at_;
  Clock::time_point last_snapshot_ = Clock::now();
  std::uint64_t max_events_ = event_limit();
  std::uint64_t events_written_ = 0;
  std::uint64_t events_dropped_ = 0;
  bool trace_events_ = true;
  bool disabled_ = false;
  std::filesystem::path output_directory_;
  std::ofstream event_stream_;
  std::mutex mutex_;
  std::map<std::string, OperationStats> operations_;
  std::uint64_t mpi_time_ns_ = 0;
  std::uint64_t bytes_sent_ = 0;
  std::uint64_t bytes_received_ = 0;
};

std::unique_ptr<Recorder> recorder;

struct Pending {
  long long id;
  bool receive;
  MPI_Group peers;
  int peer;
  int tag;
};
std::mutex pending_mutex;
std::map<MPI_Fint, Pending> pending;
long long next_request_id = 0;

long long remember(MPI_Request request, bool receive, MPI_Comm comm, int peer, int tag) noexcept {
  if (request == MPI_REQUEST_NULL) return -1;
  MPI_Group group = MPI_GROUP_NULL;
  try {
    std::lock_guard<std::mutex> lock(pending_mutex);
    if (pending.size() >= 65536) return -1;
    int inter = 0;
    PMPI_Comm_test_inter(comm, &inter);
    if (inter) PMPI_Comm_remote_group(comm, &group);
    else PMPI_Comm_group(comm, &group);
    auto key = PMPI_Request_c2f(request);
    auto previous = pending.find(key);
    if (previous != pending.end() && previous->second.peers != MPI_GROUP_NULL)
      PMPI_Group_free(&previous->second.peers);
    const auto id = ++next_request_id;
    pending[key] = Pending{id, receive, group, peer, tag};
    return id;
  } catch (...) {
    if (group != MPI_GROUP_NULL) PMPI_Group_free(&group);
    return -1;
  }
}

void complete_request(MPI_Fint key, MPI_Status* status) noexcept {
  try {
    std::lock_guard<std::mutex> lock(pending_mutex);
    auto found = pending.find(key);
    if (found == pending.end()) return;
    auto item = found->second;
    pending.erase(found);
    int cancelled = 0, count = 0;
    PMPI_Test_cancelled(status, &cancelled);
    int peer = item.receive ? status->MPI_SOURCE : item.peer;
    if (item.receive && !cancelled) PMPI_Get_count(status, MPI_BYTE, &count);
    if (peer >= 0 && item.peers != MPI_GROUP_NULL) {
      MPI_Group world;
      PMPI_Comm_group(MPI_COMM_WORLD, &world);
      int translated = MPI_UNDEFINED;
      PMPI_Group_translate_ranks(item.peers, 1, &peer, world, &translated);
      PMPI_Group_free(&world);
      peer = translated;
    }
    if (item.peers != MPI_GROUP_NULL) PMPI_Group_free(&item.peers);
    if (recorder) {
      const auto now = Clock::now();
      recorder->record(item.receive ? "MPI_Irecv_complete" : "MPI_Isend_complete", now, now,
                       cancelled ? 0 : std::max(count, 0), cancelled ? MPI_PROC_NULL : peer,
                       item.receive ? status->MPI_TAG : item.tag, MPI_SUCCESS, 0, item.id);
    }
  } catch (...) {}
}

MPI_Fint request_key(MPI_Request request) {
  return request == MPI_REQUEST_NULL ? -1 : PMPI_Request_c2f(request);
}

void initialize_recorder() noexcept {
  try {
  int rank = 0;
  int world_size = 1;
  if (PMPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS ||
      PMPI_Comm_size(MPI_COMM_WORLD, &world_size) != MPI_SUCCESS) {
    return;
  }
  recorder = std::make_unique<Recorder>(rank, world_size);
  } catch (...) { recorder.reset(); }
}

void record(const char* operation, Clock::time_point start, Clock::time_point end,
            long long bytes, int peer, int tag, int error_code,
            MPI_Comm communicator = MPI_COMM_WORLD, long long request_id = -1) noexcept {
  try {
  if (recorder) {
    const int normalized_peer = error_code == MPI_SUCCESS
                                    ? world_peer(communicator, peer)
                                    : peer;
    const long long communicator_id = error_code == MPI_SUCCESS
                                          ? static_cast<long long>(PMPI_Comm_c2f(communicator))
                                          : -1;
    recorder->record(operation, start, end, error_code == MPI_SUCCESS ? bytes : 0,
                     normalized_peer, tag, error_code, communicator_id, request_id);
  }
  } catch (...) { /* Instrumentation must not unwind into the application. */ }
}

}  // namespace
}  // namespace ranklens

extern "C" {

int ranklens_MPI_Init(int* argc, char*** argv) {
  const int result = PMPI_Init(argc, argv);
  if (result == MPI_SUCCESS) {
    ranklens::initialize_recorder();
  }
  return result;
}

int ranklens_MPI_Init_thread(int* argc, char*** argv, int required, int* provided) {
  const int result = PMPI_Init_thread(argc, argv, required, provided);
  if (result == MPI_SUCCESS) {
    ranklens::initialize_recorder();
  }
  return result;
}

int ranklens_MPI_Finalize() {
  for (auto& entry : ranklens::pending) {
    if (entry.second.peers != MPI_GROUP_NULL) PMPI_Group_free(&entry.second.peers);
  }
  ranklens::pending.clear();
  if (ranklens::recorder) {
    try { ranklens::recorder->finalize(); } catch (...) {}
    ranklens::recorder.reset();
  }
  return PMPI_Finalize();
}

int ranklens_MPI_Send(const void* buffer, int count, MPI_Datatype datatype, int destination, int tag,
                      MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Send(buffer, count, datatype, destination, tag, communicator);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Send", start, end, destination == MPI_PROC_NULL ? 0 : ranklens::payload_size(count, datatype), destination,
                   tag, result, communicator);
  return result;
}

int ranklens_MPI_Recv(void* buffer, int count, MPI_Datatype datatype, int source, int tag,
                      MPI_Comm communicator, MPI_Status* status) {
  MPI_Status local_status{};
  MPI_Status* observed_status = status == MPI_STATUS_IGNORE ? &local_status : status;
  const auto start = ranklens::Clock::now();
  const int result =
      PMPI_Recv(buffer, count, datatype, source, tag, communicator, observed_status);
  const auto end = ranklens::Clock::now();

  int actual_count = count;
  int actual_source = source;
  int actual_tag = tag;
  if (result == MPI_SUCCESS) {
    int received_count = 0;
    if (PMPI_Get_count(observed_status, datatype, &received_count) == MPI_SUCCESS &&
        received_count != MPI_UNDEFINED) {
      actual_count = received_count;
    }
    actual_source = observed_status->MPI_SOURCE;
    actual_tag = observed_status->MPI_TAG;
  }
  ranklens::record("MPI_Recv", start, end, ranklens::payload_size(actual_count, datatype),
                   actual_source, actual_tag, result, communicator);
  return result;
}

int ranklens_MPI_Allreduce(const void* send_buffer, void* receive_buffer, int count,
                           MPI_Datatype datatype, MPI_Op operation, MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result =
      PMPI_Allreduce(send_buffer, receive_buffer, count, datatype, operation, communicator);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Allreduce", start, end, ranklens::payload_size(count, datatype), -1, -1,
                   result, communicator);
  return result;
}

int ranklens_MPI_Bcast(void* buffer, int count, MPI_Datatype datatype, int root,
                       MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Bcast(buffer, count, datatype, root, communicator);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Bcast", start, end, ranklens::payload_size(count, datatype), root, -1,
                   result, communicator);
  return result;
}

int ranklens_MPI_Isend(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Isend(buffer, count, datatype, peer, tag, comm, request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS ? ranklens::remember(*request, false, comm, peer, tag) : -1;
  ranklens::record("MPI_Isend", start, end, peer == MPI_PROC_NULL ? 0 : ranklens::payload_size(count, datatype), peer, tag, result, comm, id);
  return result;
}

int ranklens_MPI_Irecv(void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Irecv(buffer, count, datatype, peer, tag, comm, request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS ? ranklens::remember(*request, true, comm, peer, tag) : -1;
  ranklens::record("MPI_Irecv", start, end, 0, peer, tag, result, comm, id);
  return result;
}
int ranklens_MPI_Wait(MPI_Request* request, MPI_Status* status) {
  const auto key = ranklens::request_key(*request);
  MPI_Status local{};
  auto observed = status == MPI_STATUS_IGNORE ? &local : status;
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Wait(request, observed);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Wait", start, end, 0, -1, -1, result);
  if (result == MPI_SUCCESS && key != -1) ranklens::complete_request(key, observed);
  return result;
}

int ranklens_MPI_Test(MPI_Request* request, int* flag, MPI_Status* status) {
  const auto key = ranklens::request_key(*request);
  MPI_Status local{};
  auto observed = status == MPI_STATUS_IGNORE ? &local : status;
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Test(request, flag, observed);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Test", start, end, 0, -1, -1, result);
  if (result == MPI_SUCCESS && *flag && key != -1) ranklens::complete_request(key, observed);
  return result;
}
int ranklens_MPI_Waitall(int count, MPI_Request requests[], MPI_Status statuses[]) {
  if (count < 0) return PMPI_Waitall(count, requests, statuses);
  std::vector<MPI_Fint> keys;
  std::vector<MPI_Status> local;
  try {
    keys.reserve(count);
    for (int i = 0; i < count; ++i) keys.push_back(ranklens::request_key(requests[i]));
    if (statuses == MPI_STATUSES_IGNORE) local.resize(count);
  } catch (...) { return PMPI_Waitall(count, requests, statuses); }
  auto observed = statuses == MPI_STATUSES_IGNORE ? local.data() : statuses;
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Waitall(count, requests, observed);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Waitall", start, end, 0, -1, -1, result);
  for (int i = 0; i < count; ++i) {
    if (keys[i] != -1 && (result == MPI_SUCCESS ||
        (result == MPI_ERR_IN_STATUS && observed[i].MPI_ERROR == MPI_SUCCESS)))
      ranklens::complete_request(keys[i], &observed[i]);
  }
  return result;
}

int ranklens_MPI_Barrier(MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Barrier(communicator);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Barrier", start, end, 0, -1, -1, result, communicator);
  return result;
}

#if defined(__APPLE__)

#define RANKLENS_INTERPOSE(replacement, replacee)                                      \
  __attribute__((used)) static const struct {                                         \
    const void* replacement_address;                                                  \
    const void* replacee_address;                                                     \
  } ranklens_interpose_##replacee __attribute__((section("__DATA,__interpose"))) = { \
      reinterpret_cast<const void*>(replacement), reinterpret_cast<const void*>(replacee)}

RANKLENS_INTERPOSE(ranklens_MPI_Init, MPI_Init);
RANKLENS_INTERPOSE(ranklens_MPI_Init_thread, MPI_Init_thread);
RANKLENS_INTERPOSE(ranklens_MPI_Finalize, MPI_Finalize);
RANKLENS_INTERPOSE(ranklens_MPI_Send, MPI_Send);
RANKLENS_INTERPOSE(ranklens_MPI_Recv, MPI_Recv);
RANKLENS_INTERPOSE(ranklens_MPI_Allreduce, MPI_Allreduce);
RANKLENS_INTERPOSE(ranklens_MPI_Bcast, MPI_Bcast);
RANKLENS_INTERPOSE(ranklens_MPI_Barrier, MPI_Barrier);
RANKLENS_INTERPOSE(ranklens_MPI_Isend, MPI_Isend);
RANKLENS_INTERPOSE(ranklens_MPI_Irecv, MPI_Irecv);
RANKLENS_INTERPOSE(ranklens_MPI_Wait, MPI_Wait);
RANKLENS_INTERPOSE(ranklens_MPI_Test, MPI_Test);
RANKLENS_INTERPOSE(ranklens_MPI_Waitall, MPI_Waitall);

#else

int MPI_Init(int* argc, char*** argv) { return ranklens_MPI_Init(argc, argv); }

int MPI_Init_thread(int* argc, char*** argv, int required, int* provided) {
  return ranklens_MPI_Init_thread(argc, argv, required, provided);
}

int MPI_Finalize() { return ranklens_MPI_Finalize(); }
int MPI_Isend(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) { return ranklens_MPI_Isend(buffer, count, datatype, peer, tag, comm, request); }
int MPI_Irecv(void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) { return ranklens_MPI_Irecv(buffer, count, datatype, peer, tag, comm, request); }
int MPI_Wait(MPI_Request* request, MPI_Status* status) { return ranklens_MPI_Wait(request, status); }
int MPI_Test(MPI_Request* request, int* flag, MPI_Status* status) { return ranklens_MPI_Test(request, flag, status); }
int MPI_Waitall(int count, MPI_Request requests[], MPI_Status statuses[]) { return ranklens_MPI_Waitall(count, requests, statuses); }

int MPI_Send(const void* buffer, int count, MPI_Datatype datatype, int destination, int tag,
             MPI_Comm communicator) {
  return ranklens_MPI_Send(buffer, count, datatype, destination, tag, communicator);
}

int MPI_Recv(void* buffer, int count, MPI_Datatype datatype, int source, int tag,
             MPI_Comm communicator, MPI_Status* status) {
  return ranklens_MPI_Recv(buffer, count, datatype, source, tag, communicator, status);
}

int MPI_Allreduce(const void* send_buffer, void* receive_buffer, int count, MPI_Datatype datatype,
                  MPI_Op operation, MPI_Comm communicator) {
  return ranklens_MPI_Allreduce(send_buffer, receive_buffer, count, datatype, operation,
                                communicator);
}

int MPI_Bcast(void* buffer, int count, MPI_Datatype datatype, int root, MPI_Comm communicator) {
  return ranklens_MPI_Bcast(buffer, count, datatype, root, communicator);
}

int MPI_Barrier(MPI_Comm communicator) { return ranklens_MPI_Barrier(communicator); }

#endif

}  // extern "C"
