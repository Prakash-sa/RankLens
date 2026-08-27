#include <mpi.h>

#include <array>
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

#if defined(_WIN32)
#include <process.h>
#else
#include <unistd.h>
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
        disabled_ = true;
      }
    }
  }

  void record(const char* operation, Clock::time_point start, Clock::time_point end,
              long long bytes, int peer, int tag, int error_code) {
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
    if (op == "MPI_Send") {
      bytes_sent_ += safe_bytes;
    } else if (op == "MPI_Recv") {
      bytes_received_ += safe_bytes;
    }

    if (trace_events_ && event_stream_) {
      event_stream_ << "{\"schema_version\":1,\"timestamp_ns\":" << timestamp
                    << ",\"rank\":" << rank_ << ",\"operation\":\""
                    << json_escape(operation) << "\",\"duration_ns\":" << duration
                    << ",\"payload_bytes\":" << safe_bytes << ",\"peer\":" << peer
                    << ",\"tag\":" << tag << ",\"error_code\":" << error_code << "}\n";
    }
  }

  void finalize() {
    if (disabled_) {
      return;
    }

    const std::uint64_t runtime_ns = elapsed_ns(started_at_, Clock::now());
    std::lock_guard<std::mutex> lock(mutex_);
    if (event_stream_) {
      event_stream_.flush();
      event_stream_.close();
    }

    std::ofstream summary(rank_path("summary.json"), std::ios::out | std::ios::trunc);
    if (!summary) {
      return;
    }

    summary << "{\n"
            << "  \"schema_version\": 1,\n"
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

void initialize_recorder() {
  int rank = 0;
  int world_size = 1;
  if (PMPI_Comm_rank(MPI_COMM_WORLD, &rank) != MPI_SUCCESS ||
      PMPI_Comm_size(MPI_COMM_WORLD, &world_size) != MPI_SUCCESS) {
    return;
  }
  recorder = std::make_unique<Recorder>(rank, world_size);
}

void record(const char* operation, Clock::time_point start, Clock::time_point end,
            long long bytes, int peer, int tag, int error_code) {
  if (recorder) {
    recorder->record(operation, start, end, bytes, peer, tag, error_code);
  }
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
    ranklens::recorder->finalize();
    ranklens::recorder.reset();
  }
  return PMPI_Finalize();
}

int ranklens_MPI_Send(const void* buffer, int count, MPI_Datatype datatype, int destination, int tag,
                      MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Send(buffer, count, datatype, destination, tag, communicator);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Send", start, end, ranklens::payload_size(count, datatype), destination,
                   tag, result);
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
                   actual_source, actual_tag, result);
  return result;
}

int ranklens_MPI_Allreduce(const void* send_buffer, void* receive_buffer, int count,
                           MPI_Datatype datatype, MPI_Op operation, MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result =
      PMPI_Allreduce(send_buffer, receive_buffer, count, datatype, operation, communicator);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Allreduce", start, end, ranklens::payload_size(count, datatype), -1, -1,
                   result);
  return result;
}

int ranklens_MPI_Bcast(void* buffer, int count, MPI_Datatype datatype, int root,
                       MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Bcast(buffer, count, datatype, root, communicator);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Bcast", start, end, ranklens::payload_size(count, datatype), root, -1,
                   result);
  return result;
}

int ranklens_MPI_Barrier(MPI_Comm communicator) {
  const auto start = ranklens::Clock::now();
  const int result = PMPI_Barrier(communicator);
  const auto end = ranklens::Clock::now();
  ranklens::record("MPI_Barrier", start, end, 0, -1, -1, result);
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

#else

int MPI_Init(int* argc, char*** argv) { return ranklens_MPI_Init(argc, argv); }

int MPI_Init_thread(int* argc, char*** argv, int required, int* provided) {
  return ranklens_MPI_Init_thread(argc, argv, required, provided);
}

int MPI_Finalize() { return ranklens_MPI_Finalize(); }

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
