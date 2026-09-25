from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from ranklens.analyzer import analyze
from ranklens.demo import write_demo
from ranklens.report import render_html, render_text, write_json


class ReportTests(unittest.TestCase):
    def test_text_and_html_include_diagnosis(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_demo(directory)
            result = analyze(directory)

            text = render_text(result)
            html = render_html(result)

            self.assertIn("RANKLENS PERFORMANCE REPORT", text)
            self.assertIn("Runtime stragglers detected", text)
            self.assertIn("Evidence coverage", text)
            self.assertIn("<!doctype html>", html)
            self.assertIn("Rank runtime distribution", html)
            self.assertIn("Request lifecycle", html)
            self.assertIn("not promised speedups", html)

    def test_json_report_is_machine_readable(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            write_demo(directory)
            result = analyze(directory)
            destination = directory / "analysis.json"

            write_json(result, destination)
            payload = json.loads(destination.read_text(encoding="utf-8"))

            self.assertEqual(payload["world_size"], 4)
            self.assertEqual(payload["straggler_ranks"], [3])
            self.assertIn("event_detail", payload["coverage"])


if __name__ == "__main__":
    unittest.main()
