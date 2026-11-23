"""
The CSV file (java/docs/DEIDENTIFICATION.md sections 10.1 and 17.3), byte for byte as
the Java job's CsvRecordSink writes it:

- UTF-8, no byte order mark; a header row of output names, never formula-guarded;
- a field is quoted only when it holds the delimiter, a quote or a line break, and a
  quote inside is doubled (RFC 4180); null and empty are written as nothing;
- formula guard (on by default): a value starting with ``= + - @``, tab or carriage
  return gets a leading ``'``.

The Spark job builds each row's line on the workers (``line``) and writes them with
Spark's text writer, so Spark's CSV writer (backslash escapes) is never used. The
driver then streams the header and that text into ``<file>.csv.part``, computing the
SHA-256, and renames it to ``<file>.csv`` (``assemble``). An existing file is never
overwritten.
"""

from __future__ import annotations

import hashlib
import os
import shutil
from pathlib import Path

_GUARD = frozenset("=+-@\t\r")


def escape(value: str | None, delimiter: str, guard: bool) -> str:
    if not value:
        return ""
    if guard and value[0] in _GUARD:
        value = "'" + value
    if delimiter in value or '"' in value or "\n" in value or "\r" in value:
        return '"' + value.replace('"', '""') + '"'
    return value


def line(values: list, delimiter: str, guard: bool) -> str:
    """One row without its line ending."""
    return delimiter.join(escape(v, delimiter, guard) for v in values)


def check_target(target: Path) -> None:
    if target.exists():
        raise RuntimeError("output file " + target.name + " already exists; not overwriting it")


def assemble(target: Path, header: list[str], part_files: list[Path], *, delimiter: str, crlf: bool) -> str:
    """
    Writes ``<target>.part`` (header, then the parts' bytes in order), renames it to
    ``target`` and returns the file's SHA-256 (hex). The part is deleted on failure.
    """
    check_target(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_name(target.name + ".part")
    digest = hashlib.sha256()
    ending = "\r\n" if crlf else "\n"
    try:
        with open(part, "wb") as out:
            head = (line(header, delimiter, False) + ending).encode("utf-8")
            out.write(head)
            digest.update(head)
            for p in part_files:
                with open(p, "rb") as src:
                    while True:
                        chunk = src.read(1 << 20)
                        if not chunk:
                            break
                        out.write(chunk)
                        digest.update(chunk)
        check_target(target)
        _rename(part, target)
    except BaseException:
        try:
            part.unlink()
        except FileNotFoundError:
            pass
        raise
    return digest.hexdigest()


def _rename(src: Path, dst: Path) -> None:
    try:
        os.replace(src, dst)
    except OSError:
        # a file system that can't rename in place (some FUSE mounts): copy, then delete
        shutil.copyfile(src, dst)
        src.unlink()


def write_text(target: Path, text: str) -> None:
    """``<target>.part``, then renamed (the manifest)."""
    part = target.with_name(target.name + ".part")
    try:
        part.write_text(text, encoding="utf-8")
        _rename(part, target)
    except BaseException:
        try:
            part.unlink()
        except FileNotFoundError:
            pass
        raise
