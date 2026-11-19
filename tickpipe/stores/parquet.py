"""Ticks as Parquet files, in a local folder or on an object store.

Layout:

    <path>/<stream>/date=<YYYY-MM-DD>/part-<first tick time>.parquet

One file per flush, named by the time of its first tick, so the files of a day
sort into recording order by name. The date is the UTC date of the ticks.

A file is written under a temporary name and renamed into place, so a reader
never sees half a file and a crash loses at most the batch being written.

Many small files are slow to read back, so a local store merges a day's files
into larger ones (about half a million ticks each, `merge_rows`) when the recording
moves on to the next day and when it closes. The merge works a group of files at
a time, so its memory does not grow with the size of the day. It writes the new
files beside the old folder, checks the row count, and only then swaps the
folders; what an interrupted merge leaves behind is cleaned up the next time the
store is opened. Object stores are not merged: use a longer flush interval there.
"""

from __future__ import annotations

import logging
import os
import shutil
from datetime import date, timedelta
from pathlib import Path

import polars as pl

from tickpipe import ticks
from tickpipe.stores import Store

log = logging.getLogger("tickpipe.stores.parquet")
_DAY_NS = 86_400_000_000_000
_EPOCH = date(1970, 1, 1)


def _day_text(day_number: int) -> str:
    return (_EPOCH + timedelta(days=int(day_number))).isoformat()


class ParquetStore(Store):
    kind = "parquet"

    def __init__(self, spec: dict, root: str | Path = ".") -> None:
        super().__init__(spec, root)
        path = str(self.spec["path"])
        self.remote = "://" in path
        self.base = path.rstrip("/") if self.remote else str((self.root / path).resolve())
        self.options = {key: str(value) for key, value in self.spec.get("storage_options", {}).items()} or None
        self.merge = bool(self.spec.get("compact", True)) and not self.remote
        self.merge_rows = int(self.spec.get("merge_rows", 500_000))
        self._day: dict[str, int] = {}  # the day each stream is currently writing
        self._written: set[tuple[str, int]] = set()
        if not self.remote:
            self._recover()

    # ---- writing ------------------------------------------------------------------

    def write(self, stream: str, frame: pl.DataFrame) -> None:
        days = frame["ts"] // _DAY_NS
        if days[0] == days[-1]:
            pieces = [(int(days[0]), frame)]
        else:
            split = frame.with_columns(days.alias("_day")).partition_by("_day", maintain_order=True, as_dict=True)
            pieces = [(int(key[0]), part.drop("_day")) for key, part in split.items()]
        for day, piece in pieces:
            previous = self._day.get(stream)
            if self.merge and previous is not None and day > previous:
                self.compact(stream, _day_text(previous))
            self._day[stream] = day
            self._written.add((stream, day))
            self._write_file(stream, day, piece)

    def _write_file(self, stream: str, day: int, piece: pl.DataFrame) -> None:
        name = f"part-{int(piece['ts'][0]):020d}.parquet"
        out = piece.with_columns(pl.col("ts").cast(pl.Datetime("ns")), pl.col("exch_ts").cast(pl.Datetime("ns")))
        if self.remote:
            out.write_parquet(f"{self.base}/{stream}/date={_day_text(day)}/{name}", storage_options=self.options)
            return
        folder = Path(self.base) / stream / f"date={_day_text(day)}"
        folder.mkdir(parents=True, exist_ok=True)
        temporary = folder / f"{name}.tmp"
        out.write_parquet(temporary)
        os.replace(temporary, folder / name)

    # ---- merging a day's small files ---------------------------------------------------

    def compact(self, stream: str, day: str) -> bool:
        """Merge the files of one stream and day into files of about `merge_rows` rows.
        Returns True if the day now has fewer files."""
        parent = Path(self.base) / stream
        folder, staging, retired = parent / f"date={day}", parent / f"_compact_{day}", parent / f"_old_{day}"
        parts = sorted(folder.glob("part-*.parquet"))
        if len(parts) < 2:
            return False
        try:
            shutil.rmtree(staging, ignore_errors=True)
            staging.mkdir(parents=True)
            merged = self._merge_into(staging, parts)
            if merged >= len(parts):  # every file was already full size: nothing to gain
                shutil.rmtree(staging, ignore_errors=True)
                return False
            os.rename(folder, retired)
            os.rename(staging, folder)
            shutil.rmtree(retired, ignore_errors=True)
            return True
        except OSError:
            # A reader holding a file open, or a full disk. The parts are intact and readable.
            log.warning("could not merge %s files of %s; they stay as they are", stream, day, exc_info=True)
            shutil.rmtree(staging, ignore_errors=True)
            if retired.exists() and not folder.exists():
                os.rename(retired, folder)
            return False

    def _merge_into(self, staging: Path, parts: list[Path]) -> int:
        """Write the parts into `staging` as fewer, larger files and return how many.

        Parts are read and written a group at a time, so the memory this needs is
        set by `merge_rows` and not by how many ticks the day holds. Each merged
        file takes the name of its first part, which keeps the files in recording
        order by name.
        """
        files = rows_in = rows_out = 0
        group: list[pl.DataFrame] = []
        group_rows, first = 0, None

        def write_group() -> None:
            nonlocal files, rows_out, group, group_rows, first
            target = staging / first.name
            if len(group) == 1:
                shutil.copyfile(first, target)  # already one file: no need to decode it
            else:
                pl.concat(group).write_parquet(target)
            rows_out += pl.scan_parquet(str(target)).select(pl.len()).collect().item()
            files += 1
            group, group_rows, first = [], 0, None

        for part in parts:
            frame = pl.read_parquet(part)
            rows_in += frame.height
            if first is None:
                first = part
            group.append(frame)
            group_rows += frame.height
            if group_rows >= self.merge_rows:
                write_group()
        if group:
            write_group()
        if rows_in != rows_out:
            raise OSError(f"merged files hold {rows_out} rows, the parts held {rows_in}")
        return files

    def _recover(self) -> None:
        """Undo what an interrupted merge left behind."""
        base = Path(self.base)
        if not base.exists():
            return
        for parent in (path for path in base.iterdir() if path.is_dir()):
            for retired in parent.glob("_old_*"):
                folder = parent / f"date={retired.name[len('_old_') :]}"
                if folder.exists():
                    shutil.rmtree(retired, ignore_errors=True)  # the swap had finished
                else:
                    os.rename(retired, folder)  # the swap had not: put the parts back
            for staging in parent.glob("_compact_*"):
                shutil.rmtree(staging, ignore_errors=True)

    def close(self) -> None:
        if self.merge:
            for stream, day in sorted(self._written):
                self.compact(stream, _day_text(day))
        self._written.clear()

    # ---- reading back -----------------------------------------------------------------

    def source_spec(self, stream: str, start_ns: int | None = None, end_ns: int | None = None) -> dict:
        if stream not in ticks.STREAMS:
            raise ValueError(f"unknown stream {stream!r}")
        if self.remote:
            path: str | list[str] = f"{self.base}/{stream}/date=*/part-*.parquet"
        else:
            first = None if start_ns is None else _day_text(start_ns // _DAY_NS)
            last = None if end_ns is None else _day_text((end_ns - 1) // _DAY_NS)
            folders = sorted((Path(self.base) / stream).glob("date=*"))
            path = [
                str(part)
                for folder in folders
                if (first is None or folder.name[5:] >= first) and (last is None or folder.name[5:] <= last)
                for part in sorted(folder.glob("part-*.parquet"))
            ]
            if not path:
                raise FileNotFoundError(f"no recorded {stream} under {self.base} for the requested time")
        spec: dict = {"kind": "parquet", "path": path}
        if self.options:
            spec["storage_options"] = self.options
        if start_ns is not None:
            spec["start"] = int(start_ns)
        if end_ns is not None:
            spec["end"] = int(end_ns)
        return spec
