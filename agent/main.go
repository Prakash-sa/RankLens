package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"
)

const (
	maxRecordBytes  = 1 << 20
	maxSummaryBytes = 1 << 20
	maxSegmentBytes = 8 << 20
	defaultSpoolMax = 1 << 30
)

type config struct {
	CaptureDir          string
	SpoolDir            string
	APIURL              string
	TokenFile           string
	ClusterID           string
	AttemptID           string
	ProducerID          string
	DeletionGeneration  uint64
	MaxSpoolBytes       int64
	MinSpoolFreeBytes   int64
	MaxAgentMemoryBytes int64
	Interval            time.Duration
	Once                bool
}

type fileCursor struct {
	Offset int64  `json:"offset"`
	Digest string `json:"digest,omitempty"`
}

type agentState struct {
	TransportEpoch string                `json:"transport_epoch"`
	NextSequence   map[string]uint64     `json:"next_sequence_by_stream"`
	Files          map[string]fileCursor `json:"files"`
}

type segmentUpload struct {
	EnvelopeMajor      int                `json:"envelope_major"`
	ClusterID          string             `json:"cluster_id"`
	AttemptID          string             `json:"attempt_id"`
	ProducerID         string             `json:"producer_id"`
	TransportEpoch     string             `json:"transport_epoch"`
	StreamID           string             `json:"stream_id"`
	FirstSequence      uint64             `json:"first_sequence"`
	LastSequence       uint64             `json:"last_sequence"`
	RecordCount        int                `json:"record_count"`
	DeletionGeneration uint64             `json:"deletion_generation"`
	Compression        string             `json:"compression"`
	ContentType        string             `json:"content_type"`
	ExpandedSizeBytes  int                `json:"expanded_size_bytes"`
	PayloadSHA256      string             `json:"payload_sha256"`
	PayloadBase64      string             `json:"payload_base64"`
	NodeBudget         nodeBudgetSnapshot `json:"node_budget"`
}

type nodeBudgetSnapshot struct {
	AgentMemoryBytes    int64 `json:"agent_memory_bytes"`
	MaxAgentMemoryBytes int64 `json:"max_agent_memory_bytes"`
	SpoolFreeBytes      int64 `json:"spool_free_bytes"`
	MinSpoolFreeBytes   int64 `json:"min_spool_free_bytes"`
	MaxSpoolBytes       int64 `json:"max_spool_bytes"`
}

type durableReceipt struct {
	ReceiptID string `json:"receipt_id"`
	Status    string `json:"status"`
}

type agent struct {
	config config
	state  agentState
	client *http.Client
}

func randomID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return hex.EncodeToString(value), nil
}

func writeAtomic(path string, content []byte, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), ".ranklens-")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(mode); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(content); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, path); err != nil {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func loadOrCreateState(spoolDir string) (agentState, error) {
	path := filepath.Join(spoolDir, "state.json")
	content, err := os.ReadFile(path)
	if err == nil {
		var state agentState
		if err := json.Unmarshal(content, &state); err != nil {
			return agentState{}, fmt.Errorf("decode state: %w", err)
		}
		if state.TransportEpoch == "" || state.Files == nil || state.NextSequence == nil {
			return agentState{}, errors.New("state is missing required identity")
		}
		return state, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return agentState{}, err
	}
	epoch, err := randomID()
	if err != nil {
		return agentState{}, err
	}
	state := agentState{
		TransportEpoch: epoch,
		NextSequence:   map[string]uint64{},
		Files:          map[string]fileCursor{},
	}
	if err := saveState(spoolDir, state); err != nil {
		return agentState{}, err
	}
	return state, nil
}

func saveState(spoolDir string, state agentState) error {
	content, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	content = append(content, '\n')
	return writeAtomic(filepath.Join(spoolDir, "state.json"), content, 0o640)
}

func newAgent(configuration config) (*agent, error) {
	if configuration.ClusterID == "" || configuration.AttemptID == "" || configuration.ProducerID == "" {
		return nil, errors.New("cluster, attempt, and producer identities are required")
	}
	if configuration.MaxSpoolBytes <= 0 {
		return nil, errors.New("max spool bytes must be positive")
	}
	if configuration.MinSpoolFreeBytes < 0 {
		return nil, errors.New("minimum spool free bytes cannot be negative")
	}
	if configuration.MaxAgentMemoryBytes < 0 {
		return nil, errors.New("max agent memory bytes cannot be negative")
	}
	state, err := loadOrCreateState(configuration.SpoolDir)
	if err != nil {
		return nil, err
	}
	return &agent{
		config: configuration,
		state:  state,
		client: &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func agentMemoryBytes() int64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	if stats.Sys > uint64(^uint(0)>>1) {
		return int64(^uint(0) >> 1)
	}
	return int64(stats.Sys)
}

func filesystemAvailableBytes(path string) (int64, error) {
	var stats syscall.Statfs_t
	if err := syscall.Statfs(path, &stats); err != nil {
		return 0, err
	}
	free := uint64(stats.Bavail) * uint64(stats.Bsize)
	if free > uint64(^uint(0)>>1) {
		return int64(^uint(0) >> 1), nil
	}
	return int64(free), nil
}

func (a *agent) measureNodeBudgets() (nodeBudgetSnapshot, error) {
	snapshot := nodeBudgetSnapshot{
		AgentMemoryBytes:    agentMemoryBytes(),
		MaxAgentMemoryBytes: a.config.MaxAgentMemoryBytes,
		MinSpoolFreeBytes:   a.config.MinSpoolFreeBytes,
		MaxSpoolBytes:       a.config.MaxSpoolBytes,
	}
	free, err := filesystemAvailableBytes(a.config.SpoolDir)
	if err != nil {
		return snapshot, fmt.Errorf("measure spool filesystem free space: %w", err)
	}
	snapshot.SpoolFreeBytes = free
	return snapshot, nil
}

func (a *agent) enforceNodeBudgets() (nodeBudgetSnapshot, error) {
	snapshot, err := a.measureNodeBudgets()
	if err != nil {
		return snapshot, err
	}
	if a.config.MaxAgentMemoryBytes > 0 {
		if snapshot.AgentMemoryBytes > a.config.MaxAgentMemoryBytes {
			return snapshot, fmt.Errorf("agent memory budget exceeded: used=%d limit=%d", snapshot.AgentMemoryBytes, a.config.MaxAgentMemoryBytes)
		}
	}
	if a.config.MinSpoolFreeBytes > 0 {
		if snapshot.SpoolFreeBytes < a.config.MinSpoolFreeBytes {
			return snapshot, fmt.Errorf("spool filesystem free budget violated: free=%d required=%d", snapshot.SpoolFreeBytes, a.config.MinSpoolFreeBytes)
		}
	}
	return snapshot, nil
}

func (a *agent) spoolPayload(streamID string, payload []byte, records int) error {
	if records <= 0 || len(payload) == 0 {
		return nil
	}
	if len(payload) > maxSegmentBytes {
		return errors.New("sealed segment exceeds 8 MiB admission budget")
	}
	nodeBudget, err := a.enforceNodeBudgets()
	if err != nil {
		return err
	}
	digest := sha256.Sum256(payload)
	first := a.state.NextSequence[streamID]
	last := first + uint64(records) - 1
	upload := segmentUpload{
		EnvelopeMajor:      2,
		ClusterID:          a.config.ClusterID,
		AttemptID:          a.config.AttemptID,
		ProducerID:         a.config.ProducerID,
		TransportEpoch:     a.state.TransportEpoch,
		StreamID:           streamID,
		FirstSequence:      first,
		LastSequence:       last,
		RecordCount:        records,
		DeletionGeneration: a.config.DeletionGeneration,
		Compression:        "identity",
		ContentType:        "application/x-ndjson",
		ExpandedSizeBytes:  len(payload),
		PayloadSHA256:      hex.EncodeToString(digest[:]),
		PayloadBase64:      base64.StdEncoding.EncodeToString(payload),
		NodeBudget:         nodeBudget,
	}
	encoded, err := json.Marshal(upload)
	if err != nil {
		return err
	}
	streamDigest := sha256.Sum256([]byte(streamID))
	name := fmt.Sprintf("%s-%020d-%020d.segment.json", hex.EncodeToString(streamDigest[:4]), first, last)
	path := filepath.Join(a.config.SpoolDir, "pending", name)
	if existing, err := os.ReadFile(path); err == nil {
		if !bytes.Equal(existing, encoded) {
			return errors.New("existing spool range has different content")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		used, err := directoryBytes(filepath.Join(a.config.SpoolDir, "pending"), a.config.MaxSpoolBytes)
		if err != nil {
			return err
		}
		if int64(len(encoded)) > a.config.MaxSpoolBytes-used {
			return fmt.Errorf("spool quota exceeded: used=%d limit=%d", used, a.config.MaxSpoolBytes)
		}
		if err := writeAtomic(path, encoded, 0o640); err != nil {
			return err
		}
	} else {
		return err
	}
	a.state.NextSequence[streamID] = last + 1
	return nil
}

func directoryBytes(path string, limit int64) (int64, error) {
	entries, err := os.ReadDir(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var total int64
	for _, entry := range entries {
		if !entry.Type().IsRegular() {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return 0, err
		}
		if info.Size() > limit-total {
			return limit, nil
		}
		total += info.Size()
	}
	return total, nil
}

func readBoundedFile(path string, limit int64) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	content, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(content)) > limit {
		return nil, fmt.Errorf("%s exceeds %d-byte limit", filepath.Base(path), limit)
	}
	return content, nil
}

func compactSummary(path string, content []byte) ([]byte, error) {
	var summary map[string]any
	if err := json.Unmarshal(content, &summary); err != nil {
		return nil, fmt.Errorf("invalid summary %s: %w", filepath.Base(path), err)
	}
	record := map[string]any{
		"record_kind": "rank_summary_file",
		"source_file": filepath.Base(path),
		"payload":     summary,
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return nil, err
	}
	return append(encoded, '\n'), nil
}

func completeNDJSON(content []byte) ([]byte, int, error) {
	lastNewline := bytes.LastIndexByte(content, '\n')
	if lastNewline < 0 {
		return nil, 0, nil
	}
	complete := content[:lastNewline+1]
	count := 0
	for _, line := range bytes.Split(complete, []byte{'\n'}) {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}
		if len(line) > maxRecordBytes {
			return nil, 0, errors.New("event record exceeds 1 MiB")
		}
		var value map[string]any
		if err := json.Unmarshal(line, &value); err != nil {
			return nil, 0, fmt.Errorf("invalid event JSON: %w", err)
		}
		count++
	}
	return complete, count, nil
}

func (a *agent) collect() error {
	entries, err := os.ReadDir(a.config.CaptureDir)
	if err != nil {
		return err
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		path := filepath.Join(a.config.CaptureDir, name)
		cursor := a.state.Files[name]
		switch {
		case strings.HasSuffix(name, "-summary.json"):
			content, err := readBoundedFile(path, maxSummaryBytes)
			if err != nil {
				return err
			}
			digest := sha256.Sum256(content)
			rendered := hex.EncodeToString(digest[:])
			if cursor.Digest == rendered {
				continue
			}
			payload, err := compactSummary(path, content)
			if err != nil {
				return err
			}
			if err := a.spoolPayload("rank-summary", payload, 1); err != nil {
				return err
			}
			cursor.Digest = rendered
			a.state.Files[name] = cursor
			if err := saveState(a.config.SpoolDir, a.state); err != nil {
				return err
			}
		case strings.HasSuffix(name, "-events.jsonl"):
			file, err := os.Open(path)
			if err != nil {
				return err
			}
			info, statErr := file.Stat()
			if statErr != nil {
				file.Close()
				return statErr
			}
			if info.Size() < cursor.Offset {
				file.Close()
				return fmt.Errorf("capture file %s was truncated; refusing identity reuse", name)
			}
			if _, err := file.Seek(cursor.Offset, io.SeekStart); err != nil {
				file.Close()
				return err
			}
			content, readErr := io.ReadAll(io.LimitReader(file, 8*1024*1024))
			file.Close()
			if readErr != nil {
				return readErr
			}
			payload, records, err := completeNDJSON(content)
			if err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
			if records == 0 {
				continue
			}
			if err := a.spoolPayload("mpi-events", payload, records); err != nil {
				return err
			}
			cursor.Offset += int64(len(payload))
			a.state.Files[name] = cursor
			if err := saveState(a.config.SpoolDir, a.state); err != nil {
				return err
			}
		}
	}
	return nil
}

func readToken(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("token path must be a regular file, not a link")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", errors.New("token file must not be accessible by group or other users")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	token := strings.TrimSpace(string(content))
	if len(token) < 24 {
		return "", errors.New("machine token is too short")
	}
	return token, nil
}

func validateEndpoint(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Host == "" {
		return errors.New("invalid API URL")
	}
	if parsed.Scheme == "https" {
		return nil
	}
	host := parsed.Hostname()
	if parsed.Scheme == "http" && (host == "127.0.0.1" || host == "localhost" || host == "::1") {
		return nil
	}
	return errors.New("non-loopback API endpoints require HTTPS")
}

func (a *agent) drain() error {
	if a.config.APIURL == "" {
		return nil
	}
	if err := validateEndpoint(a.config.APIURL); err != nil {
		return err
	}
	token, err := readToken(a.config.TokenFile)
	if err != nil {
		return err
	}
	entries, err := filepath.Glob(filepath.Join(a.config.SpoolDir, "pending", "*.segment.json"))
	if err != nil {
		return err
	}
	sort.Strings(entries)
	for _, path := range entries {
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		request, err := http.NewRequest(http.MethodPost, strings.TrimRight(a.config.APIURL, "/")+"/v1/segments", bytes.NewReader(content))
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Request-ID", a.state.TransportEpoch+":"+filepath.Base(path))
		response, err := a.client.Do(request)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(io.LimitReader(response.Body, 64*1024))
		response.Body.Close()
		if readErr != nil {
			return readErr
		}
		if response.StatusCode != http.StatusCreated && response.StatusCode != http.StatusOK {
			return fmt.Errorf("ingest returned %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
		}
		var receipt durableReceipt
		if err := json.Unmarshal(body, &receipt); err != nil || receipt.Status != "DURABLE" || receipt.ReceiptID == "" {
			return errors.New("ingest response did not contain a durable receipt")
		}
		receiptPath := filepath.Join(a.config.SpoolDir, "receipts", receipt.ReceiptID+".json")
		if err := writeAtomic(receiptPath, append(body, '\n'), 0o640); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	configuration := config{}
	flag.StringVar(&configuration.CaptureDir, "capture-dir", "", "RankLens capture directory")
	flag.StringVar(&configuration.SpoolDir, "spool-dir", "", "private durable spool directory")
	flag.StringVar(&configuration.APIURL, "api-url", "", "enterprise API base URL; empty keeps data local")
	flag.StringVar(&configuration.TokenFile, "token-file", "", "0600 machine token file")
	flag.StringVar(&configuration.ClusterID, "cluster-id", "", "registered cluster identity")
	flag.StringVar(&configuration.AttemptID, "attempt-id", "", "reconciled scheduler-attempt identity")
	flag.StringVar(&configuration.ProducerID, "producer-id", "", "registered node-agent identity")
	flag.Uint64Var(&configuration.DeletionGeneration, "deletion-generation", 0, "current attempt deletion generation")
	flag.Int64Var(&configuration.MaxSpoolBytes, "max-spool-bytes", defaultSpoolMax, "maximum pending spool bytes")
	flag.Int64Var(&configuration.MinSpoolFreeBytes, "min-spool-free-bytes", 0, "minimum filesystem free bytes required before sealing a segment")
	flag.Int64Var(&configuration.MaxAgentMemoryBytes, "max-agent-memory-bytes", 0, "maximum Go runtime memory bytes allowed before sealing a segment; 0 disables the check")
	flag.DurationVar(&configuration.Interval, "interval", time.Second, "collection and replay interval")
	flag.BoolVar(&configuration.Once, "once", false, "collect and replay once, then exit")
	flag.Parse()
	if configuration.CaptureDir == "" || configuration.SpoolDir == "" || configuration.Interval < 100*time.Millisecond {
		fmt.Fprintln(os.Stderr, "capture-dir, spool-dir, and interval >=100ms are required")
		os.Exit(2)
	}
	instance, err := newAgent(configuration)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for {
		if err := instance.collect(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		if err := instance.drain(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			if configuration.Once {
				os.Exit(1)
			}
		}
		if configuration.Once {
			return
		}
		time.Sleep(configuration.Interval)
	}
}
