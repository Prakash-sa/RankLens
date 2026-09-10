import json
import tempfile
import unittest
import zipfile
from pathlib import Path
from unittest.mock import patch

from ranklens.analyzer import analyze, TelemetryError
from ranklens.demo import write_demo
from ranklens.runner import run, RunnerError, discover_library
from ranklens.workflows import bundle, compare, catalog, export_csv


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
