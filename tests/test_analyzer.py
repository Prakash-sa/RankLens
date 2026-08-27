from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from ranklens.analyzer import TelemetryError, analyze
from ranklens.demo import write_demo


class AnalyzerTests(unittest.TestCase):
    def test_demo_detects_straggler_collective_and_hotspot(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_demo(directory)

            result = analyze(directory)

            self.assertEqual(result.world_size, 4)
            self.assertEqual(result.ranks_observed, 4)
            self.assertEqual(result.straggler_ranks, [3])
            self.assertGreater(result.imbalance_ratio, 1.3)
            self.assertEqual(result.operations["MPI_Allreduce"].calls, 200)
            self.assertEqual(result.communication_edges[0].source, 0)
            self.assertGreater(result.communication_edges[0].traffic_share, 0.7)
            categories = {finding.category for finding in result.findings}
            self.assertIn("imbalance", categories)
            self.assertIn("collectives", categories)
            self.assertIn("communication", categories)

    def test_rejects_missing_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(TelemetryError, "no rank"):
                analyze(Path(temporary))

    def test_rejects_invalid_threshold(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_demo(directory)
            with self.assertRaisesRegex(ValueError, "greater than 1.0"):
                analyze(directory, straggler_threshold=1.0)


if __name__ == "__main__":
    unittest.main()

