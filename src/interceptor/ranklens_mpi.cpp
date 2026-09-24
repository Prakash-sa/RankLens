#include <mpi.h>

#include <array>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstring>
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
#include <thread>
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

std::size_t event_buffer_limit() {
  const std::string value = env_value("RANKLENS_EVENT_BUFFER_RECORDS");
  if (value.empty()) return 1024;
  try { return std::min<std::size_t>(std::stoull(value), 65536); }
  catch (...) { return 1024; }
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

constexpr std::array<const char*, 32> kOperationNames = {
    "MPI_Send", "MPI_Recv", "MPI_Isend", "MPI_Irecv", "MPI_Wait", "MPI_Test",
    "MPI_Waitall", "MPI_Waitany", "MPI_Waitsome", "MPI_Testall", "MPI_Testany",
    "MPI_Testsome", "MPI_Allreduce", "MPI_Bcast", "MPI_Barrier",
    "MPI_Cancel", "MPI_Request_free", "MPI_Send_init", "MPI_Recv_init",
    "MPI_Bsend_init", "MPI_Ssend_init", "MPI_Rsend_init", "MPI_Start",
    "MPI_Startall", "MPI_Psend_init", "MPI_Precv_init", "MPI_Pready",
    "MPI_Pready_range", "MPI_Pready_list", "MPI_Parrived",
    "MPI_Isend_complete", "MPI_Irecv_complete"};

std::size_t operation_index(const char* operation) {
  for (std::size_t index = 0; index < kOperationNames.size(); ++index) {
    if (std::strcmp(operation, kOperationNames[index]) == 0) return index;
  }
  return kOperationNames.size();
}

bool is_completion(std::size_t index) {
  return index == kOperationNames.size() - 2 || index == kOperationNames.size() - 1;
}

bool is_sent_payload(std::size_t index) {
  return index == 0 || index == 2;
}

bool is_received_payload(std::size_t index) {
  return index == 1 || index == kOperationNames.size() - 1;
}

struct EventRecord {
  const char* operation = "unknown";
  std::uint64_t sequence = 0;
  std::uint64_t timestamp_ns = 0;
  std::uint64_t duration_ns = 0;
  std::uint64_t payload_bytes = 0;
  int peer = -1;
  int tag = -1;
  int error_code = MPI_SUCCESS;
  long long communicator = -1;
  long long request_id = -1;
};

struct RecorderSnapshot {
  std::array<OperationStats, kOperationNames.size() + 1> operations{};
  std::uint64_t mpi_time_ns = 0;
  std::uint64_t api_calls = 0;
  std::uint64_t request_completions = 0;
  std::uint64_t failed_calls = 0;
  std::uint64_t bytes_sent = 0;
  std::uint64_t bytes_received = 0;
  std::uint64_t events_dropped = 0;
  std::uint64_t events_written = 0;
  std::uint64_t request_tracking_overflows = 0;
};

class Recorder {
 public:
  Recorder(int rank, int world_size)
      : rank_(rank),
        world_size_(world_size),
        host_(hostname()),
        pid_(process_id()),
        started_at_(Clock::now()),
        max_events_(event_limit()),
        event_buffer_capacity_(event_buffer_limit()),
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
      if (!event_stream_) trace_events_ = false;
    }
    active_events_.reserve(event_buffer_capacity_);
    staging_events_.reserve(event_buffer_capacity_);
    writer_thread_ = std::thread([this] { writer_loop(); });
  }

  ~Recorder() { stop_writer(); }

  void record(const char* operation, Clock::time_point start, Clock::time_point end,
              long long bytes, int peer, int tag, int error_code, long long communicator = 0,
              long long request_id = -1) noexcept {
    if (disabled_) return;

    const auto index = operation_index(operation);
    const std::uint64_t duration = elapsed_ns(start, end);
    const std::uint64_t timestamp = elapsed_ns(started_at_, start);
    const std::uint64_t safe_bytes = bytes > 0 ? static_cast<std::uint64_t>(bytes) : 0;
    bool wake_writer = false;
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      auto& stats = operations_[index];
      ++stats.calls;
      stats.duration_ns += duration;
      stats.payload_bytes += safe_bytes;
      mpi_time_ns_ += duration;
      if (is_completion(index)) {
        ++request_completions_;
      } else {
        ++api_calls_;
        if (error_code != MPI_SUCCESS) ++failed_calls_;
      }
      if (is_sent_payload(index)) bytes_sent_ += safe_bytes;
      if (is_received_payload(index)) bytes_received_ += safe_bytes;

      if (trace_events_) {
        const auto event_sequence = event_sequence_++;
        if (events_accepted_ >= max_events_ || active_events_.size() >= event_buffer_capacity_) {
          ++events_dropped_;
        } else {
          active_events_.push_back(EventRecord{operation, event_sequence, timestamp, duration, safe_bytes, peer,
                                                tag, error_code, communicator, request_id});
          ++events_accepted_;
          wake_writer = active_events_.size() >= std::max<std::size_t>(1, event_buffer_capacity_ / 2);
        }
      }
    }
    if (wake_writer) writer_wakeup_.notify_one();
  }

  void prepare_finalize() noexcept { stop_writer(); }

  void request_tracking_overflow() noexcept {
    std::lock_guard<std::mutex> lock(state_mutex_);
    ++request_tracking_overflows_;
  }

  void finish_finalize(int finalize_return_code) noexcept {
    if (disabled_) return;
    try {
      drain_events();
      write_snapshot(finalize_return_code == MPI_SUCCESS,
                     finalize_return_code == MPI_SUCCESS ? "finalized" : "finalize_failed",
                     finalize_return_code);
      if (event_stream_) {
        event_stream_.flush();
        event_stream_.close();
      }
    } catch (...) {
    }
  }

 private:
  void stop_writer() noexcept {
    if (!writer_thread_.joinable()) return;
    stop_requested_.store(true, std::memory_order_release);
    writer_wakeup_.notify_one();
    try { writer_thread_.join(); } catch (...) {}
  }

  void writer_loop() noexcept {
    try {
      auto next_snapshot = Clock::now();
      while (!stop_requested_.load(std::memory_order_acquire)) {
        std::unique_lock<std::mutex> wait_lock(writer_mutex_);
        writer_wakeup_.wait_for(wait_lock, std::chrono::milliseconds(100), [this] {
          return stop_requested_.load(std::memory_order_acquire);
        });
        wait_lock.unlock();
        drain_events();
        if (Clock::now() >= next_snapshot) {
          write_snapshot(false, "collecting", MPI_ERR_PENDING);
          next_snapshot = Clock::now() + std::chrono::seconds(1);
        }
      }
      drain_events();
      write_snapshot(false, "pre_finalize", MPI_ERR_PENDING);
    } catch (...) {
      writer_failed_.store(true, std::memory_order_release);
    }
  }

  void drain_events() {
    if (!trace_events_ || !event_stream_) return;
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      active_events_.swap(staging_events_);
    }
    for (const auto& event : staging_events_) {
      event_stream_ << "{\"schema_version\":2,\"sequence\":" << event.sequence
                    << ",\"timestamp_ns\":" << event.timestamp_ns
                    << ",\"rank\":" << rank_ << ",\"operation\":\""
                    << json_escape(event.operation) << "\",\"record_kind\":\""
                    << (is_completion(operation_index(event.operation)) ? "request_completion" : "api_call")
                    << "\",\"duration_ns\":" << event.duration_ns
                    << ",\"payload_bytes\":" << event.payload_bytes << ",\"peer\":" << event.peer
                    << ",\"tag\":" << event.tag << ",\"error_code\":" << event.error_code
                    << ",\"communicator\":" << event.communicator
                    << ",\"request_id\":" << event.request_id << "}\n";
    }
    {
      std::lock_guard<std::mutex> lock(state_mutex_);
      events_written_ += staging_events_.size();
    }
    staging_events_.clear();
  }

  RecorderSnapshot capture_snapshot() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return RecorderSnapshot{operations_, mpi_time_ns_, api_calls_, request_completions_,
                            failed_calls_, bytes_sent_, bytes_received_, events_dropped_,
                            events_written_, request_tracking_overflows_};
  }

  // The writer thread owns filesystem I/O. Atomic rename avoids publishing partial JSON;
  // it is not an fsync durability guarantee.
  void write_snapshot(bool complete, const char* capture_state, int finalize_return_code) {
    const auto state = capture_snapshot();
    const std::uint64_t runtime_ns = elapsed_ns(started_at_, Clock::now());
    if (event_stream_) event_stream_.flush();
    std::ofstream summary(rank_path("summary.json.tmp"), std::ios::out | std::ios::trunc);
    if (!summary) return;

    summary << "{\n"
            << "  \"schema_version\": 2,\n"
            << "  \"complete\": " << (complete ? "true" : "false") << ",\n"
            << "  \"capture_state\": \"" << capture_state << "\",\n"
            << "  \"finalize_return_code\": " << finalize_return_code << ",\n"
            << "  \"context\": {\"run_id\": \"" << json_escape(env_value("RANKLENS_RUN_ID"))
            << "\", \"capture_epoch\": \"" << json_escape(env_value("RANKLENS_CAPTURE_EPOCH"))
            << "\", \"attempt_id\": \"" << json_escape(env_value("RANKLENS_ATTEMPT_ID"))
            << "\", \"slurm_job_id\": \"" << json_escape(env_value("SLURM_JOB_ID"))
            << "\", \"slurm_step_id\": \"" << json_escape(env_value("SLURM_STEP_ID"))
            << "\", \"flux_job_id\": \"" << json_escape(env_value("FLUX_JOB_ID"))
            << "\", \"traceparent\": \"" << json_escape(env_value("TRACEPARENT"))
            << "\", \"events_dropped\": \"" << state.events_dropped
            << "\", \"events_written\": \"" << state.events_written
            << "\", \"event_buffer_records\": \"" << event_buffer_capacity_
            << "\", \"request_tracking_overflows\": \""
            << state.request_tracking_overflows
            << "\", \"writer_failed\": \""
            << (writer_failed_.load(std::memory_order_acquire) ? "true" : "false")
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
            << "  \"mpi_time_ns\": " << state.mpi_time_ns << ",\n"
            << "  \"mpi_calls\": " << state.api_calls << ",\n"
            << "  \"request_completions\": " << state.request_completions << ",\n"
            << "  \"failed_calls\": " << state.failed_calls << ",\n"
            << "  \"bytes_sent\": " << state.bytes_sent << ",\n"
            << "  \"bytes_received\": " << state.bytes_received << ",\n"
            << "  \"operations\": {\n";

    std::size_t remaining = 0;
    for (const auto& stats : state.operations) if (stats.calls > 0) ++remaining;
    for (std::size_t index = 0; index < state.operations.size(); ++index) {
      const auto& stats = state.operations[index];
      if (stats.calls == 0) continue;
      const char* name = index < kOperationNames.size() ? kOperationNames[index] : "Other";
      summary << "    \"" << name << "\": {\"calls\": " << stats.calls
              << ", \"duration_ns\": " << stats.duration_ns
              << ", \"payload_bytes\": " << stats.payload_bytes
              << ", \"record_kind\": \"" << (is_completion(index) ? "request_completion" : "api_call")
              << "\"}" << (--remaining == 0 ? "\n" : ",\n");
    }
    summary << "  }\n}\n";
    summary.close();
    if (summary) {
      std::error_code error;
      std::filesystem::rename(rank_path("summary.json.tmp"), rank_path("summary.json"), error);
    }
  }

  std::filesystem::path rank_path(const char* suffix) const {
    std::ostringstream filename;
    filename << "rank-" << std::setw(5) << std::setfill('0') << rank_ << '-' << suffix;
    return output_directory_ / filename.str();
  }

  int rank_ = 0;
  int world_size_ = 1;
  std::string host_;
  int pid_ = 0;
  Clock::time_point started_at_;
  std::uint64_t max_events_ = 0;
  std::size_t event_buffer_capacity_ = 0;
  bool trace_events_ = true;
  bool disabled_ = false;
  std::filesystem::path output_directory_;
  std::ofstream event_stream_;
  std::mutex state_mutex_;
  std::array<OperationStats, kOperationNames.size() + 1> operations_{};
  std::vector<EventRecord> active_events_;
  std::vector<EventRecord> staging_events_;
  std::uint64_t mpi_time_ns_ = 0;
  std::uint64_t api_calls_ = 0;
  std::uint64_t request_completions_ = 0;
  std::uint64_t failed_calls_ = 0;
  std::uint64_t bytes_sent_ = 0;
  std::uint64_t bytes_received_ = 0;
  std::uint64_t events_accepted_ = 0;
  std::uint64_t event_sequence_ = 0;
  std::uint64_t events_written_ = 0;
  std::uint64_t events_dropped_ = 0;
  std::uint64_t request_tracking_overflows_ = 0;
  std::mutex writer_mutex_;
  std::condition_variable writer_wakeup_;
  std::atomic<bool> stop_requested_{false};
  std::atomic<bool> writer_failed_{false};
  std::thread writer_thread_;
};

std::unique_ptr<Recorder> recorder;

struct Pending {
  long long id;
  bool receive;
  MPI_Group peers;
  int peer;
  int tag;
  bool persistent;
};
std::mutex pending_mutex;
using RequestKey = MPI_Request;
std::map<RequestKey, std::vector<Pending>> pending;
std::map<RequestKey, Pending> persistent;
long long next_request_id = 0;

std::size_t pending_size() noexcept {
  std::size_t total = 0;
  for (const auto& entry : pending) total += entry.second.size();
  return total;
}

long long remember(MPI_Request request, bool receive, MPI_Comm comm, int peer, int tag,
                   bool persistent_request = false) noexcept {
  if (request == MPI_REQUEST_NULL) return -1;
  MPI_Group group = MPI_GROUP_NULL;
  try {
    std::lock_guard<std::mutex> lock(pending_mutex);
    const auto current_size = persistent_request ? persistent.size() : pending_size();
    if (current_size >= 65536) {
      if (recorder) recorder->request_tracking_overflow();
      return -1;
    }
    int inter = 0;
    PMPI_Comm_test_inter(comm, &inter);
    if (inter) PMPI_Comm_remote_group(comm, &group);
    else PMPI_Comm_group(comm, &group);
    auto key = request;
    const auto id = ++next_request_id;
    if (persistent_request) {
      auto previous = persistent.find(key);
      if (previous != persistent.end() && previous->second.peers != MPI_GROUP_NULL)
        PMPI_Group_free(&previous->second.peers);
      persistent[key] = Pending{id, receive, group, peer, tag, true};
    } else {
      pending[key].push_back(Pending{id, receive, group, peer, tag, false});
    }
    return id;
  } catch (...) {
    if (group != MPI_GROUP_NULL) PMPI_Group_free(&group);
    return -1;
  }
}

void forget_request(RequestKey key) noexcept {
  if (key == MPI_REQUEST_NULL) return;
  try {
    std::lock_guard<std::mutex> lock(pending_mutex);
    auto found = pending.find(key);
    if (found != pending.end()) {
      for (auto& item : found->second) {
        if (!item.persistent && item.peers != MPI_GROUP_NULL) PMPI_Group_free(&item.peers);
      }
      pending.erase(found);
    }
    auto persistent_found = persistent.find(key);
    if (persistent_found == persistent.end()) return;
    if (persistent_found->second.peers != MPI_GROUP_NULL)
      PMPI_Group_free(&persistent_found->second.peers);
    persistent.erase(persistent_found);
  } catch (...) {}
}

long long request_id(RequestKey key) noexcept {
  if (key == MPI_REQUEST_NULL) return -1;
  try {
    std::lock_guard<std::mutex> lock(pending_mutex);
    auto active = pending.find(key);
    if (active != pending.end() && !active->second.empty()) return active->second.front().id;
    auto saved = persistent.find(key);
    if (saved != persistent.end()) return saved->second.id;
  } catch (...) {}
  return -1;
}

long long start_persistent(RequestKey key) noexcept {
  if (key == MPI_REQUEST_NULL) return -1;
  try {
    std::lock_guard<std::mutex> lock(pending_mutex);
    auto saved = persistent.find(key);
    if (saved == persistent.end()) return -1;
    pending[key].push_back(Pending{saved->second.id, saved->second.receive, saved->second.peers,
                                   saved->second.peer, saved->second.tag, true});
    return saved->second.id;
  } catch (...) {}
  return -1;
}

void complete_request(RequestKey key, MPI_Status* status) noexcept {
  try {
    std::lock_guard<std::mutex> lock(pending_mutex);
    auto found = pending.find(key);
    if (found == pending.end() || found->second.empty()) return;
    auto item = found->second.front();
    found->second.erase(found->second.begin());
    if (found->second.empty()) pending.erase(found);
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
    if (!item.persistent && item.peers != MPI_GROUP_NULL) PMPI_Group_free(&item.peers);
    if (recorder) {
      const auto now = Clock::now();
      recorder->record(item.receive ? "MPI_Irecv_complete" : "MPI_Isend_complete", now, now,
                       cancelled ? 0 : std::max(count, 0), cancelled ? MPI_PROC_NULL : peer,
                       item.receive ? status->MPI_TAG : item.tag, MPI_SUCCESS, 0, item.id);
    }
  } catch (...) {}
}

RequestKey request_key(MPI_Request request) {
  return request;
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
  if (ranklens::recorder) {
    ranklens::recorder->prepare_finalize();
  }
  for (auto& entry : ranklens::pending) {
    for (auto& item : entry.second) {
      if (!item.persistent && item.peers != MPI_GROUP_NULL) PMPI_Group_free(&item.peers);
    }
  }
  ranklens::pending.clear();
  for (auto& entry : ranklens::persistent) {
    if (entry.second.peers != MPI_GROUP_NULL) PMPI_Group_free(&entry.second.peers);
  }
  ranklens::persistent.clear();
  const int result = PMPI_Finalize();
  if (ranklens::recorder) {
    ranklens::recorder->finish_finalize(result);
    ranklens::recorder.reset();
  }
  return result;
}

int ranklens_MPI_Send(const void* buffer, int count, MPI_Datatype datatype, int destination, int tag,
                      MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Send(buffer, count, datatype, destination, tag, communicator);
  const auto end = ranklens::Clock::now();
  const auto bytes = result == MPI_SUCCESS && destination != MPI_PROC_NULL
                         ? ranklens::payload_size(count, datatype) : 0;
  ranklens::record("MPI_Send", start, end, bytes, destination,
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
  const auto bytes = result == MPI_SUCCESS ? ranklens::payload_size(actual_count, datatype) : 0;
  ranklens::record("MPI_Recv", start, end, bytes,
                   actual_source, actual_tag, result, communicator);
  return result;
}

int ranklens_MPI_Allreduce(const void* send_buffer, void* receive_buffer, int count,
                           MPI_Datatype datatype, MPI_Op operation, MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result =
      PMPI_Allreduce(send_buffer, receive_buffer, count, datatype, operation, communicator);
  const auto end = ranklens::Clock::now();
  const auto bytes = result == MPI_SUCCESS ? ranklens::payload_size(count, datatype) : 0;
  ranklens::record("MPI_Allreduce", start, end, bytes, -1, -1,
                   result, communicator);
  return result;
}

int ranklens_MPI_Bcast(void* buffer, int count, MPI_Datatype datatype, int root,
                       MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Bcast(buffer, count, datatype, root, communicator);
  const auto end = ranklens::Clock::now();
  const auto bytes = result == MPI_SUCCESS ? ranklens::payload_size(count, datatype) : 0;
  ranklens::record("MPI_Bcast", start, end, bytes, root, -1,
                   result, communicator);
  return result;
}

int ranklens_MPI_Isend(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Isend(buffer, count, datatype, peer, tag, comm, request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS ? ranklens::remember(*request, false, comm, peer, tag) : -1;
  const auto bytes = result == MPI_SUCCESS && peer != MPI_PROC_NULL
                         ? ranklens::payload_size(count, datatype) : 0;
  ranklens::record("MPI_Isend", start, end, bytes, peer, tag, result, comm, id);
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

int ranklens_MPI_Send_init(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag,
                           MPI_Comm comm, MPI_Request* request) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Send_init(buffer, count, datatype, peer, tag, comm, request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS
                      ? ranklens::remember(*request, false, comm, peer, tag, true)
                      : -1;
  ranklens::record("MPI_Send_init", start, end, 0, peer, tag, result, comm, id);
  return result;
}

int ranklens_MPI_Recv_init(void* buffer, int count, MPI_Datatype datatype, int peer, int tag,
                           MPI_Comm comm, MPI_Request* request) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Recv_init(buffer, count, datatype, peer, tag, comm, request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS
                      ? ranklens::remember(*request, true, comm, peer, tag, true)
                      : -1;
  ranklens::record("MPI_Recv_init", start, end, 0, peer, tag, result, comm, id);
  return result;
}

int ranklens_MPI_Bsend_init(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag,
                            MPI_Comm comm, MPI_Request* request) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Bsend_init(buffer, count, datatype, peer, tag, comm, request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS
                      ? ranklens::remember(*request, false, comm, peer, tag, true)
                      : -1;
  ranklens::record("MPI_Bsend_init", start, end, 0, peer, tag, result, comm, id);
  return result;
}

int ranklens_MPI_Ssend_init(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag,
                            MPI_Comm comm, MPI_Request* request) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Ssend_init(buffer, count, datatype, peer, tag, comm, request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS
                      ? ranklens::remember(*request, false, comm, peer, tag, true)
                      : -1;
  ranklens::record("MPI_Ssend_init", start, end, 0, peer, tag, result, comm, id);
  return result;
}

int ranklens_MPI_Rsend_init(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag,
                            MPI_Comm comm, MPI_Request* request) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Rsend_init(buffer, count, datatype, peer, tag, comm, request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS
                      ? ranklens::remember(*request, false, comm, peer, tag, true)
                      : -1;
  ranklens::record("MPI_Rsend_init", start, end, 0, peer, tag, result, comm, id);
  return result;
}

#if defined(RANKLENS_HAVE_MPI_PARTITIONED)
int ranklens_MPI_Psend_init(const void* buffer, int partitions, MPI_Count count,
                            MPI_Datatype datatype, int peer, int tag, MPI_Comm comm,
                            MPI_Info info, MPI_Request* request) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Psend_init(
      buffer, partitions, count, datatype, peer, tag, comm, info, request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS
                      ? ranklens::remember(*request, false, comm, peer, tag, true)
                      : -1;
  ranklens::record("MPI_Psend_init", start, end, 0, peer, tag, result, comm, id);
  return result;
}

int ranklens_MPI_Precv_init(void* buffer, int partitions, MPI_Count count,
                            MPI_Datatype datatype, int peer, int tag, MPI_Comm comm,
                            MPI_Info info, MPI_Request* request) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Precv_init(
      buffer, partitions, count, datatype, peer, tag, comm, info, request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS
                      ? ranklens::remember(*request, true, comm, peer, tag, true)
                      : -1;
  ranklens::record("MPI_Precv_init", start, end, 0, peer, tag, result, comm, id);
  return result;
}

int ranklens_MPI_Pready(int partition, MPI_Request request) {
  const auto id = ranklens::request_id(ranklens::request_key(request));
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Pready(partition, request);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Pready", start, end, 0, -1, partition, result,
                   MPI_COMM_WORLD, id);
  return result;
}

int ranklens_MPI_Pready_range(int partition_low, int partition_high,
                              MPI_Request request) {
  const auto id = ranklens::request_id(ranklens::request_key(request));
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Pready_range(partition_low, partition_high, request);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Pready_range", start, end, 0, partition_low,
                   partition_high, result, MPI_COMM_WORLD, id);
  return result;
}

int ranklens_MPI_Pready_list(int length, int partitions[], MPI_Request request) {
  const auto id = ranklens::request_id(ranklens::request_key(request));
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Pready_list(length, partitions, request);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Pready_list", start, end, 0, -1, length, result,
                   MPI_COMM_WORLD, id);
  return result;
}

int ranklens_MPI_Parrived(MPI_Request request, int partition, int* flag) {
  const auto id = ranklens::request_id(ranklens::request_key(request));
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Parrived(request, partition, flag);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Parrived", start, end, 0, -1, partition, result,
                   MPI_COMM_WORLD, id);
  return result;
}
#endif

int ranklens_MPI_Start(MPI_Request* request) {
  const auto key = ranklens::request_key(*request);
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Start(request);
  const auto end = ranklens::Clock::now();
  const auto id = result == MPI_SUCCESS ? ranklens::start_persistent(key) : ranklens::request_id(key);
  ranklens::record("MPI_Start", start, end, 0, -1, -1, result, MPI_COMM_WORLD, id);
  return result;
}

int ranklens_MPI_Startall(int count, MPI_Request requests[]) {
  if (count < 0) return PMPI_Startall(count, requests);
  std::vector<ranklens::RequestKey> keys;
  try {
    keys.reserve(count);
    for (int item = 0; item < count; ++item) keys.push_back(ranklens::request_key(requests[item]));
  } catch (...) {
    return PMPI_Startall(count, requests);
  }
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Startall(count, requests);
  const auto end = ranklens::Clock::now();
  if (result == MPI_SUCCESS) {
    for (const auto key : keys) ranklens::start_persistent(key);
  }
  ranklens::record("MPI_Startall", start, end, 0, -1, -1, result);
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
  if (result == MPI_SUCCESS && key != MPI_REQUEST_NULL) ranklens::complete_request(key, observed);
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
  if (result == MPI_SUCCESS && *flag && key != MPI_REQUEST_NULL) ranklens::complete_request(key, observed);
  return result;
}
int ranklens_MPI_Waitall(int count, MPI_Request requests[], MPI_Status statuses[]) {
  if (count < 0) return PMPI_Waitall(count, requests, statuses);
  std::vector<ranklens::RequestKey> keys;
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
    if (keys[i] != MPI_REQUEST_NULL && (result == MPI_SUCCESS ||
        (result == MPI_ERR_IN_STATUS && observed[i].MPI_ERROR == MPI_SUCCESS)))
      ranklens::complete_request(keys[i], &observed[i]);
  }
  return result;
}

int ranklens_MPI_Waitany(int count, MPI_Request requests[], int* index, MPI_Status* status) {
  if (count < 0) return PMPI_Waitany(count, requests, index, status);
  std::vector<ranklens::RequestKey> keys;
  try {
    keys.reserve(count);
    for (int item = 0; item < count; ++item) keys.push_back(ranklens::request_key(requests[item]));
  } catch (...) {
    return PMPI_Waitany(count, requests, index, status);
  }
  MPI_Status local{};
  auto observed = status == MPI_STATUS_IGNORE ? &local : status;
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Waitany(count, requests, index, observed);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Waitany", start, end, 0, -1, -1, result);
  if (result == MPI_SUCCESS && *index != MPI_UNDEFINED && 0 <= *index && *index < count)
    ranklens::complete_request(keys[*index], observed);
  return result;
}

int ranklens_MPI_Testall(int count, MPI_Request requests[], int* flag, MPI_Status statuses[]) {
  if (count < 0) return PMPI_Testall(count, requests, flag, statuses);
  std::vector<ranklens::RequestKey> keys;
  std::vector<MPI_Status> local;
  try {
    keys.reserve(count);
    for (int item = 0; item < count; ++item) keys.push_back(ranklens::request_key(requests[item]));
    if (statuses == MPI_STATUSES_IGNORE) local.resize(count);
  } catch (...) {
    return PMPI_Testall(count, requests, flag, statuses);
  }
  auto observed = statuses == MPI_STATUSES_IGNORE ? local.data() : statuses;
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Testall(count, requests, flag, observed);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Testall", start, end, 0, -1, -1, result);
  if ((result == MPI_SUCCESS || result == MPI_ERR_IN_STATUS) && *flag) {
    for (int item = 0; item < count; ++item) {
      if (keys[item] != MPI_REQUEST_NULL &&
          (result == MPI_SUCCESS || observed[item].MPI_ERROR == MPI_SUCCESS))
        ranklens::complete_request(keys[item], &observed[item]);
    }
  }
  return result;
}

int ranklens_MPI_Testany(int count, MPI_Request requests[], int* index, int* flag,
                         MPI_Status* status) {
  if (count < 0) return PMPI_Testany(count, requests, index, flag, status);
  std::vector<ranklens::RequestKey> keys;
  try {
    keys.reserve(count);
    for (int item = 0; item < count; ++item) keys.push_back(ranklens::request_key(requests[item]));
  } catch (...) {
    return PMPI_Testany(count, requests, index, flag, status);
  }
  MPI_Status local{};
  auto observed = status == MPI_STATUS_IGNORE ? &local : status;
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Testany(count, requests, index, flag, observed);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Testany", start, end, 0, -1, -1, result);
  if (result == MPI_SUCCESS && *flag && *index != MPI_UNDEFINED && 0 <= *index && *index < count)
    ranklens::complete_request(keys[*index], observed);
  return result;
}

int ranklens_MPI_Waitsome(int count, MPI_Request requests[], int* outcount, int indices[],
                          MPI_Status statuses[]) {
  if (count < 0) return PMPI_Waitsome(count, requests, outcount, indices, statuses);
  std::vector<ranklens::RequestKey> keys;
  std::vector<MPI_Status> local;
  try {
    keys.reserve(count);
    for (int item = 0; item < count; ++item) keys.push_back(ranklens::request_key(requests[item]));
    if (statuses == MPI_STATUSES_IGNORE) local.resize(count);
  } catch (...) {
    return PMPI_Waitsome(count, requests, outcount, indices, statuses);
  }
  auto observed = statuses == MPI_STATUSES_IGNORE ? local.data() : statuses;
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Waitsome(count, requests, outcount, indices, observed);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Waitsome", start, end, 0, -1, -1, result);
  if ((result == MPI_SUCCESS || result == MPI_ERR_IN_STATUS) &&
      *outcount != MPI_UNDEFINED && *outcount > 0) {
    for (int completed = 0; completed < *outcount; ++completed) {
      const int item = indices[completed];
      if (0 <= item && item < count && keys[item] != MPI_REQUEST_NULL &&
          (result == MPI_SUCCESS || observed[completed].MPI_ERROR == MPI_SUCCESS))
        ranklens::complete_request(keys[item], &observed[completed]);
    }
  }
  return result;
}

int ranklens_MPI_Testsome(int count, MPI_Request requests[], int* outcount, int indices[],
                          MPI_Status statuses[]) {
  if (count < 0) return PMPI_Testsome(count, requests, outcount, indices, statuses);
  std::vector<ranklens::RequestKey> keys;
  std::vector<MPI_Status> local;
  try {
    keys.reserve(count);
    for (int item = 0; item < count; ++item) keys.push_back(ranklens::request_key(requests[item]));
    if (statuses == MPI_STATUSES_IGNORE) local.resize(count);
  } catch (...) {
    return PMPI_Testsome(count, requests, outcount, indices, statuses);
  }
  auto observed = statuses == MPI_STATUSES_IGNORE ? local.data() : statuses;
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Testsome(count, requests, outcount, indices, observed);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Testsome", start, end, 0, -1, -1, result);
  if ((result == MPI_SUCCESS || result == MPI_ERR_IN_STATUS) &&
      *outcount != MPI_UNDEFINED && *outcount > 0) {
    for (int completed = 0; completed < *outcount; ++completed) {
      const int item = indices[completed];
      if (0 <= item && item < count && keys[item] != MPI_REQUEST_NULL &&
          (result == MPI_SUCCESS || observed[completed].MPI_ERROR == MPI_SUCCESS))
        ranklens::complete_request(keys[item], &observed[completed]);
    }
  }
  return result;
}

int ranklens_MPI_Cancel(MPI_Request* request) {
  const auto id = ranklens::request_id(ranklens::request_key(*request));
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Cancel(request);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Cancel", start, end, 0, -1, -1, result, MPI_COMM_WORLD, id);
  return result;
}

int ranklens_MPI_Request_free(MPI_Request* request) {
  const auto key = ranklens::request_key(*request);
  const auto id = ranklens::request_id(key);
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Request_free(request);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Request_free", start, end, 0, -1, -1, result, MPI_COMM_WORLD, id);
  if (result == MPI_SUCCESS) ranklens::forget_request(key);
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
RANKLENS_INTERPOSE(ranklens_MPI_Send_init, MPI_Send_init);
RANKLENS_INTERPOSE(ranklens_MPI_Recv_init, MPI_Recv_init);
RANKLENS_INTERPOSE(ranklens_MPI_Bsend_init, MPI_Bsend_init);
RANKLENS_INTERPOSE(ranklens_MPI_Ssend_init, MPI_Ssend_init);
RANKLENS_INTERPOSE(ranklens_MPI_Rsend_init, MPI_Rsend_init);
#if defined(RANKLENS_HAVE_MPI_PARTITIONED)
RANKLENS_INTERPOSE(ranklens_MPI_Psend_init, MPI_Psend_init);
RANKLENS_INTERPOSE(ranklens_MPI_Precv_init, MPI_Precv_init);
RANKLENS_INTERPOSE(ranklens_MPI_Pready, MPI_Pready);
RANKLENS_INTERPOSE(ranklens_MPI_Pready_range, MPI_Pready_range);
RANKLENS_INTERPOSE(ranklens_MPI_Pready_list, MPI_Pready_list);
RANKLENS_INTERPOSE(ranklens_MPI_Parrived, MPI_Parrived);
#endif
RANKLENS_INTERPOSE(ranklens_MPI_Start, MPI_Start);
RANKLENS_INTERPOSE(ranklens_MPI_Startall, MPI_Startall);
RANKLENS_INTERPOSE(ranklens_MPI_Wait, MPI_Wait);
RANKLENS_INTERPOSE(ranklens_MPI_Test, MPI_Test);
RANKLENS_INTERPOSE(ranklens_MPI_Waitall, MPI_Waitall);
RANKLENS_INTERPOSE(ranklens_MPI_Waitany, MPI_Waitany);
RANKLENS_INTERPOSE(ranklens_MPI_Waitsome, MPI_Waitsome);
RANKLENS_INTERPOSE(ranklens_MPI_Testall, MPI_Testall);
RANKLENS_INTERPOSE(ranklens_MPI_Testany, MPI_Testany);
RANKLENS_INTERPOSE(ranklens_MPI_Testsome, MPI_Testsome);
RANKLENS_INTERPOSE(ranklens_MPI_Cancel, MPI_Cancel);
RANKLENS_INTERPOSE(ranklens_MPI_Request_free, MPI_Request_free);

#else

int MPI_Init(int* argc, char*** argv) { return ranklens_MPI_Init(argc, argv); }

int MPI_Init_thread(int* argc, char*** argv, int required, int* provided) {
  return ranklens_MPI_Init_thread(argc, argv, required, provided);
}

int MPI_Finalize() { return ranklens_MPI_Finalize(); }
int MPI_Isend(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) { return ranklens_MPI_Isend(buffer, count, datatype, peer, tag, comm, request); }
int MPI_Irecv(void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) { return ranklens_MPI_Irecv(buffer, count, datatype, peer, tag, comm, request); }
int MPI_Send_init(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) { return ranklens_MPI_Send_init(buffer, count, datatype, peer, tag, comm, request); }
int MPI_Recv_init(void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) { return ranklens_MPI_Recv_init(buffer, count, datatype, peer, tag, comm, request); }
int MPI_Bsend_init(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) { return ranklens_MPI_Bsend_init(buffer, count, datatype, peer, tag, comm, request); }
int MPI_Ssend_init(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) { return ranklens_MPI_Ssend_init(buffer, count, datatype, peer, tag, comm, request); }
int MPI_Rsend_init(const void* buffer, int count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Request* request) { return ranklens_MPI_Rsend_init(buffer, count, datatype, peer, tag, comm, request); }
#if defined(RANKLENS_HAVE_MPI_PARTITIONED)
int MPI_Psend_init(const void* buffer, int partitions, MPI_Count count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Info info, MPI_Request* request) { return ranklens_MPI_Psend_init(buffer, partitions, count, datatype, peer, tag, comm, info, request); }
int MPI_Precv_init(void* buffer, int partitions, MPI_Count count, MPI_Datatype datatype, int peer, int tag, MPI_Comm comm, MPI_Info info, MPI_Request* request) { return ranklens_MPI_Precv_init(buffer, partitions, count, datatype, peer, tag, comm, info, request); }
int MPI_Pready(int partition, MPI_Request request) { return ranklens_MPI_Pready(partition, request); }
int MPI_Pready_range(int partition_low, int partition_high, MPI_Request request) { return ranklens_MPI_Pready_range(partition_low, partition_high, request); }
int MPI_Pready_list(int length, int partitions[], MPI_Request request) { return ranklens_MPI_Pready_list(length, partitions, request); }
int MPI_Parrived(MPI_Request request, int partition, int* flag) { return ranklens_MPI_Parrived(request, partition, flag); }
#endif
int MPI_Start(MPI_Request* request) { return ranklens_MPI_Start(request); }
int MPI_Startall(int count, MPI_Request requests[]) { return ranklens_MPI_Startall(count, requests); }
int MPI_Wait(MPI_Request* request, MPI_Status* status) { return ranklens_MPI_Wait(request, status); }
int MPI_Test(MPI_Request* request, int* flag, MPI_Status* status) { return ranklens_MPI_Test(request, flag, status); }
int MPI_Waitall(int count, MPI_Request requests[], MPI_Status statuses[]) { return ranklens_MPI_Waitall(count, requests, statuses); }
int MPI_Waitany(int count, MPI_Request requests[], int* index, MPI_Status* status) { return ranklens_MPI_Waitany(count, requests, index, status); }
int MPI_Waitsome(int count, MPI_Request requests[], int* outcount, int indices[], MPI_Status statuses[]) { return ranklens_MPI_Waitsome(count, requests, outcount, indices, statuses); }
int MPI_Testall(int count, MPI_Request requests[], int* flag, MPI_Status statuses[]) { return ranklens_MPI_Testall(count, requests, flag, statuses); }
int MPI_Testany(int count, MPI_Request requests[], int* index, int* flag, MPI_Status* status) { return ranklens_MPI_Testany(count, requests, index, flag, status); }
int MPI_Testsome(int count, MPI_Request requests[], int* outcount, int indices[], MPI_Status statuses[]) { return ranklens_MPI_Testsome(count, requests, outcount, indices, statuses); }
int MPI_Cancel(MPI_Request* request) { return ranklens_MPI_Cancel(request); }
int MPI_Request_free(MPI_Request* request) { return ranklens_MPI_Request_free(request); }

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
