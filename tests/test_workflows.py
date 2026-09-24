import json
import tempfile
import unittest
import zipfile
from contextlib import ExitStack
from pathlib import Path
from unittest.mock import patch

from ranklens.analyzer import analyze, TelemetryError
from ranklens.demo import write_demo
from ranklens.runner import run, RunnerError, discover_library
from ranklens.workflows import bundle, benchmark, compare, catalog, export_csv


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name)
        self.capture = self.path / "capture"
        write_demo(self.capture)

    def test_malformed_events_are_warnings(self):
        with (self.capture / "rank-00000-events.jsonl").open("a") as stream:
            stream.write('[]\nnull\n{broken\n{"operation":"MPI_Send","rank":-1,"peer":2,"payload_bytes":-8}\n')
        result = analyze(self.capture)
        self.assertEqual(len(result.warnings), 4)
        self.assertTrue(result.synthetic)
        self.assertEqual(sum(row["calls"] for row in result.timeline), 32)

    def test_rank_and_schema_validation(self):
        path = self.capture / "rank-00000-summary.json"
        original = json.loads(path.read_text())
        for key, value in [("world_size", 0), ("rank", 4), ("schema_version", True), ("synthetic", "false")]:
            with self.subTest(key=key):
                path.write_text(json.dumps({**original, key: value}))
                with self.assertRaises(TelemetryError):
                    analyze(self.capture)
        path.write_text(json.dumps(original))
        for threshold in (float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                analyze(self.capture, threshold)

    def test_partial_capture(self):
        (self.capture / "rank-00003-summary.json").unlink()
        self.assertFalse(analyze(self.capture).complete)

    def test_bundle_and_csv(self):
        result = analyze(self.capture)
        bundle(result, self.path / "report.zip")
        with zipfile.ZipFile(self.path / "report.zip") as archive:
            self.assertEqual(json.loads(archive.read("analysis.json"))["synthetic"], True)
            self.assertIn(b"SYNTHETIC DATA", archive.read("report.html"))
        export_csv(result, self.path / "ranks.csv")
        self.assertEqual(len((self.path / "ranks.csv").read_text().splitlines()), 5)

    def test_comparison_requires_provenance(self):
        result = analyze(self.capture)
        self.assertFalse(compare(result, result)["comparable"])
        result.synthetic = False
        result.metadata = {"workload": "input-sha256"}
        self.assertEqual(compare(result, result)["speedup"], 1)
        result.complete = False
        self.assertFalse(compare(result, result)["comparable"])

    def test_catalog_idempotent(self):
        database = self.path / "catalog.db"
        first = catalog(database, self.capture)
        second = catalog(database, self.capture)
        self.assertEqual(len(second), 1)
        self.assertEqual(first, second)

    def test_runner_rejects_reuse_and_explicit_missing_library(self):
        with self.assertRaises(RunnerError):
            run(["solver"], Path("fake.so"), self.capture)
        with self.assertRaises(RunnerError):
            discover_library(self.path / "missing.so")

    def test_failed_launch_leaves_metadata(self):
        with patch("ranklens.runner.subprocess.run", side_effect=FileNotFoundError("missing")):
            self.assertEqual(run(["nonexistent"], Path("library.so"), self.path / "run"), 127)
        data = json.loads((self.path / "run/run.json").read_text())
        self.assertEqual(data["state"], "failed")
        self.assertEqual(len(data["run_id"]), 32)

    def test_benchmark_records_summary_mode_budget_evidence(self):
        class Completed:
            returncode = 0
            stdout = b"same-science-output\n"
            stderr = b""

        clock = 0
        timing_values = []
        for trial in range(5):
            durations = [100, 115] if trial % 2 == 0 else [115, 100]
            for duration in durations:
                timing_values.extend((clock, clock + duration))
                clock += duration + 10
        timings = iter(timing_values)
        seen_env = []

        def run_trial(command, **kwargs):
            seen_env.append(kwargs.get("env"))
            return Completed()

        with ExitStack() as stack:
            stack.enter_context(patch("ranklens.workflows.subprocess.run", side_effect=run_trial))
            stack.enter_context(
                patch("ranklens.workflows.time.perf_counter_ns", side_effect=lambda: next(timings))
            )
            stack.enter_context(
                patch("ranklens.workflows.analyze", return_value=type("Capture", (), {"complete": True})())
            )
            result = benchmark(
                ["solver", "input.json"],
                Path("libranklens_mpi.so"),
                self.path / "benchmark",
                repeats=5,
                max_median_overhead_percent=25,
                max_p95_overhead_percent=40,
            )

        self.assertEqual(result["mode"], "summary")
        self.assertEqual(len(result["paired_overhead_percent"]), 5)
        self.assertAlmostEqual(result["paired_median_overhead_percent"], 15.0)
        self.assertAlmostEqual(result["paired_p95_overhead_percent"], 15.0)
        self.assertEqual(result["paired_added_time_ns"], [15, 15, 15, 15, 15])
        self.assertEqual(result["paired_median_added_time_ns"], 15)
        self.assertEqual(result["paired_p95_added_time_ns"], 15.0)
        for interval in (
            result["paired_median_overhead_ci95_percent"],
            result["paired_p95_overhead_ci95_percent"],
        ):
            self.assertAlmostEqual(interval[0], 15.0)
            self.assertAlmostEqual(interval[1], 15.0)
        self.assertTrue(result["budget"]["conclusive"])
        self.assertTrue(result["budget"]["passed"])
        self.assertTrue(result["stdout_equal"])
        instrumented_envs = [env for env in seen_env if env is not None]
        self.assertEqual(
            [env["RANKLENS_TRACE_EVENTS"] for env in instrumented_envs],
            ["0", "0", "0", "0", "0"],
        )
        saved = json.loads((self.path / "benchmark" / "benchmark.json").read_text())
        self.assertAlmostEqual(saved["paired_p95_overhead_percent"], 15.0)

    def test_benchmark_writes_report_before_budget_failure(self):
        class Completed:
            returncode = 0
            stdout = b"same\n"
            stderr = b""

        timings = iter([100, 200, 300, 500, 600, 800, 900, 1000])
        with ExitStack() as stack:
            stack.enter_context(patch("ranklens.workflows.subprocess.run", return_value=Completed()))
            stack.enter_context(
                patch("ranklens.workflows.time.perf_counter_ns", side_effect=lambda: next(timings))
            )
            stack.enter_context(
                patch("ranklens.workflows.analyze", return_value=type("Capture", (), {"complete": True})())
            )
            with self.assertRaisesRegex(ValueError, "budget failed"):
                benchmark(
                    ["solver"],
                    Path("libranklens_mpi.so"),
                    self.path / "failed-budget",
                    repeats=2,
                    mode="detail",
                    max_median_overhead_percent=10,
                )
        saved = json.loads((self.path / "failed-budget" / "benchmark.json").read_text())
        self.assertFalse(saved["budget"]["passed"])
        self.assertFalse(saved["budget"]["conclusive"])
        self.assertEqual(saved["environment"]["ranklens_trace_events"], "1")

    def test_benchmark_does_not_pass_a_budget_with_too_few_pairs(self):
        class Completed:
            returncode = 0
            stdout = b"same\n"
            stderr = b""

        timings = iter([0, 100, 110, 210, 220, 320, 330, 430])
        destination = self.path / "inconclusive-budget"
        with ExitStack() as stack:
            stack.enter_context(
                patch("ranklens.workflows.subprocess.run", return_value=Completed())
            )
            stack.enter_context(
                patch(
                    "ranklens.workflows.time.perf_counter_ns",
                    side_effect=lambda: next(timings),
                )
            )
            stack.enter_context(
                patch(
                    "ranklens.workflows.analyze",
                    return_value=type("Capture", (), {"complete": True})(),
                )
            )
            with self.assertRaisesRegex(ValueError, "inconclusive"):
                benchmark(
                    ["solver"],
                    Path("libranklens_mpi.so"),
                    destination,
                    repeats=2,
                    max_median_overhead_percent=100,
                )

        saved = json.loads((destination / "benchmark.json").read_text())
        self.assertFalse(saved["budget"]["conclusive"])
        self.assertFalse(saved["budget"]["passed"])

    def test_benchmark_can_gate_absolute_added_time_for_short_runs(self):
        class Completed:
            returncode = 0
            stdout = b"same\n"
            stderr = b""

        clock = 0
        timing_values = []
        for trial in range(5):
            durations = [1_000_000, 3_000_000] if trial % 2 == 0 else [3_000_000, 1_000_000]
            for duration in durations:
                timing_values.extend((clock, clock + duration))
                clock += duration + 100
        timings = iter(timing_values)
        destination = self.path / "absolute-budget"
        with ExitStack() as stack:
            stack.enter_context(
                patch("ranklens.workflows.subprocess.run", return_value=Completed())
            )
            stack.enter_context(
                patch(
                    "ranklens.workflows.time.perf_counter_ns",
                    side_effect=lambda: next(timings),
                )
            )
            stack.enter_context(
                patch(
                    "ranklens.workflows.analyze",
                    return_value=type("Capture", (), {"complete": True})(),
                )
            )
            with self.assertRaisesRegex(ValueError, "budget failed"):
                benchmark(
                    ["solver"],
                    Path("libranklens_mpi.so"),
                    destination,
                    repeats=5,
                    max_median_added_time_ms=1.0,
                )

        saved = json.loads((destination / "benchmark.json").read_text())
        self.assertTrue(saved["budget"]["conclusive"])
        self.assertFalse(saved["budget"]["passed"])
        self.assertEqual(saved["paired_median_added_time_ns"], 2_000_000)
