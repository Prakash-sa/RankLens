from __future__ import annotations

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


if __name__ == "__main__":
    unittest.main()
