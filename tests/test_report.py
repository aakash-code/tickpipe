"""The report is written for a clean run and for a run with differences, and the
example configuration only names source kinds that exist."""

from __future__ import annotations

import json
import os
import re
import tomllib
from pathlib import Path

import polars as pl

from helpers import WINDOW, run_batch
from tickpipe import compare, feeds, report, runtime, settings, sources, stores

ROOT = Path(__file__).resolve().parents[1]


def chart_data(page: str) -> dict:
    return json.loads(re.search(r"const DATA = (\{.*?\});\n", page).group(1))


def test_report_for_identical_runs(ticks, tmp_path):
    expected = run_batch(*ticks)
    parity, joined = compare.compare(expected, expected)
    page = report.write(tmp_path / "report.html", parity, joined, window_ns=WINDOW).read_text(encoding="utf-8")
    assert "identical, tick for tick" in page
    assert "__" not in re.sub(r"<script>.*</script>", "", page, flags=re.S)  # every placeholder was filled
    data = chart_data(page)
    assert len(data["t"]) <= 900
    for chart in data["charts"]:
        assert chart["historical"] == chart["live"]
    # No dashes, arrows, dingbats or emoji: plain text only.
    banned = [(0x2013, 0x2014), (0x2190, 0x21FF), (0x2700, 0x27BF), (0x1F300, 0x1FAFF)]
    assert not any(low <= ord(ch) <= high for ch in set(page) for low, high in banned)


def test_report_shows_a_difference_that_falls_between_plotted_points(ticks, tmp_path):
    expected = run_batch(*ticks)
    middle = expected.height // 2
    tampered = expected.with_columns(
        pl.when(pl.int_range(pl.len()) == middle).then(pl.col("level_flow") + 5).otherwise(pl.col("level_flow"))
    )
    parity, joined = compare.compare(expected, tampered)
    page = report.write(tmp_path / "report.html", parity, joined, window_ns=WINDOW).read_text(encoding="utf-8")
    assert "Live and historical differ" in page and "First ticks that differ" in page
    level = chart_data(page)["charts"][0]
    assert sum(a != b for a, b in zip(level["historical"], level["live"], strict=True)) == 1


def test_example_configuration_is_valid():
    with (ROOT / "config.examples.toml").open("rb") as handle:
        examples = tomllib.load(handle)["sources"]
    kinds = {spec["kind"] for spec in examples.values()}
    assert kinds == set(sources.READERS)
    for name, spec in examples.items():
        assert spec["kind"] == "parquet" or "query" in spec, name


def test_default_configuration_loads():
    cfg = settings.load(ROOT / "config.toml")
    assert len(cfg.contracts) == 10 and cfg.window_ns == 300 * 10**9
    assert cfg.sources["trades"]["kind"] == "parquet"
    assert settings.duration_ns("250ms") == 250_000_000


def test_example_configuration_covers_every_store_and_feed():
    with (ROOT / "config.examples.toml").open("rb") as handle:
        examples = tomllib.load(handle)
    assert {spec["kind"] for spec in examples["stores"].values()} == set(stores.kinds())
    assert set(feeds.BUILT_IN) <= {spec["kind"] for spec in examples["feeds"].values()}
    assert examples["record"]["feed"] in examples["feeds"] and examples["record"]["store"] in examples["stores"]


def test_importing_tickpipe_loads_nothing_heavy():
    """The commands set up their process before NumPy and Polars load, so the package itself must stay light."""
    import subprocess
    import sys

    code = "import sys, tickpipe; print('numpy' in sys.modules, 'polars' in sys.modules, tickpipe.Pipeline.__name__)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True).stdout.split()
    assert out == ["False", "False", "Pipeline"]


def test_commands_start_with_only_the_threads_they_use(monkeypatch):
    monkeypatch.delenv("OPENBLAS_NUM_THREADS", raising=False)
    monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)
    runtime.lean("historical")
    assert os.environ["OPENBLAS_NUM_THREADS"] == "1" and "POLARS_MAX_THREADS" not in os.environ
    runtime.lean("record")
    assert os.environ["POLARS_MAX_THREADS"] == "2"
    monkeypatch.setenv("POLARS_MAX_THREADS", "8")  # a value the user set wins
    runtime.lean("live")
    assert os.environ["POLARS_MAX_THREADS"] == "8"


def test_readme_documents_every_command():
    from tickpipe.__main__ import COMMANDS, build_parser

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert all(ord(character) < 128 for character in readme)  # plain text only
    subparsers = next(action for action in build_parser()._actions if action.dest == "command")
    assert set(subparsers.choices) == set(COMMANDS)  # every command that parses has a handler
    for command in COMMANDS:
        assert f"tickpipe {command}" in readme, command
