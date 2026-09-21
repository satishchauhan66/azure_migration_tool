# Author: Satish Chauhan
"""Parse AzCopy output and format upload status for the GUI."""

from __future__ import annotations

import re
import time
from dataclasses import dataclass, field
from typing import Optional

_AZCOPY_JOB_PROGRESS_RE = re.compile(
    r"(?P<pct>[\d.]+)\s*%.*?"
    r"(?P<done>\d+)\s+Done,\s*"
    r"(?P<failed>\d+)\s+Failed,\s*"
    r"(?P<pending>\d+)\s+Pending",
    re.IGNORECASE,
)
_THROUGHPUT_RE = re.compile(r"Throughput\s*\(Mb/s\):\s*([\d.]+)", re.IGNORECASE)


def _format_bytes(num: float) -> str:
    if num >= 1024**4:
        return f"{num / 1024**4:.2f} TB"
    if num >= 1024**3:
        return f"{num / 1024**3:.2f} GB"
    if num >= 1024**2:
        return f"{num / 1024**2:.1f} MB"
    if num >= 1024:
        return f"{num / 1024:.0f} KB"
    return f"{num:.0f} B"


def _format_duration(seconds: float) -> str:
    if seconds < 0 or seconds > 86400 * 7:
        return "—"
    s = int(seconds)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{sec:02d}"
    return f"{m}:{sec:02d}"


@dataclass
class AzCopyJobProgress:
    percent: float
    done: int
    failed: int
    pending: int


def parse_azcopy_progress_line(line: str) -> Optional[AzCopyJobProgress]:
    m = _AZCOPY_JOB_PROGRESS_RE.search(line or "")
    if not m:
        return None
    return AzCopyJobProgress(
        percent=float(m.group("pct")),
        done=int(m.group("done")),
        failed=int(m.group("failed")),
        pending=int(m.group("pending")),
    )


@dataclass
class BlobUploadStatusTracker:
    """Live upload stats for status bar (files, bytes, speed, ETA)."""

    total_files: int
    total_bytes: int
    phase: str = "Starting"
    completed_files: int = 0
    failed_files: int = 0
    azcopy_percent: float = 0.0
    throughput_mbps: float = 0.0
    started_at: float = field(default_factory=time.time)

    def set_phase(self, phase: str) -> None:
        self.phase = phase

    def note_file_finished(self, success: bool) -> None:
        if success:
            self.completed_files += 1
        else:
            self.failed_files += 1

    def note_log_line(self, line: str) -> None:
        prog = parse_azcopy_progress_line(line)
        if prog:
            self.azcopy_percent = prog.percent
            if self.total_files > 1 and prog.done + prog.failed + prog.pending > 0:
                self.completed_files = min(self.total_files, prog.done)
                self.failed_files = prog.failed
        tm = _THROUGHPUT_RE.search(line or "")
        if tm:
            try:
                self.throughput_mbps = float(tm.group(1))
            except ValueError:
                pass

    @property
    def bytes_done_estimate(self) -> float:
        if self.total_bytes <= 0:
            return 0.0
        if self.azcopy_percent > 0:
            return self.total_bytes * (self.azcopy_percent / 100.0)
        if self.total_files > 0:
            return self.total_bytes * (self.completed_files / self.total_files)
        return 0.0

    def format_status_line(self) -> str:
        elapsed = time.time() - self.started_at
        done_files = min(self.total_files, self.completed_files)
        file_part = f"{done_files}/{self.total_files} files"
        if self.failed_files:
            file_part += f" ({self.failed_files} failed)"

        pct = self.azcopy_percent
        if pct <= 0 and self.total_files > 0:
            pct = 100.0 * done_files / self.total_files

        size_part = ""
        if self.total_bytes > 0:
            size_part = (
                f" · {_format_bytes(self.bytes_done_estimate)} / "
                f"{_format_bytes(self.total_bytes)}"
            )

        speed_part = ""
        eta_part = ""
        mbps = self.throughput_mbps
        if mbps <= 0 and elapsed > 5 and self.bytes_done_estimate > 0:
            mbps = (self.bytes_done_estimate / (1024 * 1024)) / elapsed
        if mbps > 0:
            speed_part = f" · {mbps:.0f} MB/s"
            remaining = max(0.0, self.total_bytes - self.bytes_done_estimate)
            if remaining > 0:
                eta_sec = remaining / (mbps * 1024 * 1024)
                eta_part = f" · ETA ~{_format_duration(eta_sec)}"

        return (
            f"{self.phase}: {file_part} ({pct:.0f}%){size_part}{speed_part}{eta_part} "
            f"· elapsed {_format_duration(elapsed)}"
        )
