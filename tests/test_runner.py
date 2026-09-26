from __future__ import annotations

import json
import platform
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ranklens.runner import (
    RunnerError,
    _forward_macos_openmpi_environment,
    discover_library,
    instrumented_environment,
    run,
)


class RunnerTests(unittest.TestCase):
    def test_explicit_library_is_discovered(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            library = Path(temporary) / "libranklens_mpi.so"
            library.touch()
            self.assertEqual(discover_library(library), library.resolve())

    def test_linux_environment_uses_ld_preload(self) -> None:
        with mock.patch.object(platform, "system", return_value="Linux"):
            environment = instrumented_environment(
                Path("/tmp/libranklens_mpi.so"), Path("results"), {"LD_PRELOAD": "/tmp/other.so"}
            )
        self.assertEqual(
            environment["LD_PRELOAD"], "/tmp/libranklens_mpi.so:/tmp/other.so"
        )
        self.assertTrue(environment["RANKLENS_OUTPUT_DIR"].endswith("results"))

    def test_unsupported_platform_is_rejected(self) -> None:
        with mock.patch.object(platform, "system", return_value="Windows"):
            with self.assertRaises(RunnerError):
                instrumented_environment(Path("ranklens.dll"), Path("results"), {})

    def test_openmpi_on_macos_gets_explicit_exports(self) -> None:
        completed = mock.Mock(stdout="mpirun (Open MPI) 5.0.9", stderr="")
        with mock.patch.object(platform, "system", return_value="Darwin"), mock.patch(
            "ranklens.runner.subprocess.run", return_value=completed
        ):
            command = _forward_macos_openmpi_environment(
                ["mpirun", "-n", "2", "solver"],
                {
                    "DYLD_INSERT_LIBRARIES": "/tmp/ranklens.dylib",
                    "DYLD_FORCE_FLAT_NAMESPACE": "1",
                    "RANKLENS_OUTPUT_DIR": "/tmp/results",
                },
            )
        self.assertEqual(command[0], "mpirun")
        self.assertEqual(command.count("-x"), 3)
        self.assertEqual(command[-3:], ["-n", "2", "solver"])

    def test_successful_application_records_unavailable_telemetry_separately(self) -> None:
        with tempfile.TemporaryDirectory() as temporary, mock.patch(
            "ranklens.runner.subprocess.run",
            return_value=mock.Mock(returncode=0),
        ):
            output = Path(temporary) / "capture"
            code = run(["solver"], Path("library.so"), output)

            metadata = json.loads((output / "run.json").read_text())
            self.assertEqual(code, 0)
            self.assertEqual(metadata["state"], "completed")
            self.assertEqual(
                metadata["telemetry"],
                {
                    "status": "unavailable",
                    "rank_summaries": 0,
                    "event_streams": 0,
                    "reason": "no_rank_summaries",
                },
            )

    def test_runner_records_observed_summary_and_event_counts(self) -> None:
        def completed(_command, **kwargs):
            output = Path(kwargs["env"]["RANKLENS_OUTPUT_DIR"])
            (output / "rank-00000-summary.json").write_text("{}")
            (output / "rank-00000-events.jsonl").write_text("")
            return mock.Mock(returncode=0)

        with tempfile.TemporaryDirectory() as temporary, mock.patch(
            "ranklens.runner.subprocess.run", side_effect=completed
        ):
            output = Path(temporary) / "capture"
            run(["solver"], Path("library.so"), output)

            metadata = json.loads((output / "run.json").read_text())
            self.assertEqual(metadata["telemetry"]["status"], "observed")
            self.assertEqual(metadata["telemetry"]["rank_summaries"], 1)
            self.assertEqual(metadata["telemetry"]["event_streams"], 1)
            self.assertIsNone(metadata["telemetry"]["reason"])


if __name__ == "__main__":
    unittest.main()
