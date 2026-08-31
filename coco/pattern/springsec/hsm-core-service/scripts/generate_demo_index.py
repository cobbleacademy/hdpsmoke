#!/usr/bin/env python3
"""Injects docs/architecture-diagram.svg and docs/sequence-diagram.svg into the
live demo's index.html template at build time, so those two files stay the
single, hand-edited source of truth -- the live demo's diagrams are always a
fresh copy of them, never a manually-pasted-and-forgotten one.

Why generate an inline copy at all, instead of just having index.html
<object>-reference docs/*.svg directly: a separate Python-based project reads
index.html's raw source (not a rendered DOM) to drive DEMO-LIVE, so the SVG
markup has to still be textually present in the file it reads -- changing
that delivery shape would break that consumer. See AUTHORIZATION.md-adjacent
discussion in dev-status-seed.json for the full reasoning.

Run by hsm-core-service's pom.xml (exec-maven-plugin, process-resources
phase) -- bound to the SAME phase as the default resource copy so this
step's output isn't overwritten by it (explicitly-declared executions run
after Maven's own implicit default-lifecycle bindings for that phase).

Usage: generate_demo_index.py <output-path>

Set HSM_DOCS_DIR to override where docs/architecture-diagram.svg and
docs/sequence-diagram.svg are read from, if this script's default assumption
about checkout layout (repo_root/docs, with repo_root three levels above this
script: java/hsm-core-service/scripts) doesn't hold for a given build
environment -- e.g. a CI pipeline that checks out only the java/ subtree.
"""
import os
import re
import sys
from pathlib import Path

# All file I/O is explicitly UTF-8, not the platform default -- this repo's
# docs use non-ASCII characters (em dashes, arrows) freely, and Python 3's
# default text encoding follows the OS locale, which is ASCII/POSIX (not
# UTF-8) on plenty of minimal Linux build images/containers. Relying on the
# platform default silently works on a UTF-8-locale dev machine and then
# fails with a confusing UnicodeDecodeError/UnicodeEncodeError on a
# differently-configured build host -- pin it instead.
ENCODING = "utf-8"

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parents[2]  # java/hsm-core-service/scripts -> repo root
DOCS_DIR = Path(os.environ["HSM_DOCS_DIR"]) if os.environ.get("HSM_DOCS_DIR") else REPO_ROOT / "docs"
TEMPLATE_PATH = SCRIPT_DIR.parents[0] / "src" / "main" / "resources" / "static" / "index.html"


def read_text_or_exit(path: Path, what: str) -> str:
    if not path.is_file():
        raise SystemExit(
            f"{what} not found at {path} -- this script resolved DOCS_DIR as {DOCS_DIR} "
            f"(REPO_ROOT={REPO_ROOT}, computed from this script's own location "
            f"{SCRIPT_DIR} three levels up). If this build's checkout layout puts "
            f"docs/ somewhere else, set HSM_DOCS_DIR to the correct absolute path "
            f"rather than editing this script.")
    return path.read_text(encoding=ENCODING)

ARCH_MARKER = "@@ARCHITECTURE_DIAGRAM_SVG@@"
SEQ_HEADER_MARKER = "@@SEQUENCE_DIAGRAM_HEADER@@"
SEQ_BODY_MARKER = "@@SEQUENCE_DIAGRAM_BODY@@"


def extract_sequence_header(seq_svg: str) -> str:
    match = re.search(
        r"<!-- LIVE_DEMO_HEADER_START.*?-->(.*?)<!-- LIVE_DEMO_HEADER_END -->",
        seq_svg, re.S)
    if not match:
        raise SystemExit(
            "LIVE_DEMO_HEADER_START/END markers not found in docs/sequence-diagram.svg -- "
            "did the header section get restructured? Update this script's extraction to match.")
    return match.group(1).strip()


def extract_sequence_body(seq_svg: str) -> str:
    # Search only after LIVE_DEMO_HEADER_END, not from the start of the file --
    # this exact open_tag string also appears earlier, inside the
    # LIVE_DEMO_HEADER_START/END explanatory comment itself (which describes
    # this very tag), so searching from position 0 previously matched that
    # comment instead of the real tag and pulled the header content into the
    # body a second time. Anchoring past the header marker makes that class of
    # collision impossible regardless of what the comment text says.
    header_end_marker = "<!-- LIVE_DEMO_HEADER_END -->"
    header_end = seq_svg.find(header_end_marker)
    if header_end == -1:
        raise SystemExit("LIVE_DEMO_HEADER_END marker not found in docs/sequence-diagram.svg")
    search_from = header_end + len(header_end_marker)

    open_tag = '<g transform="translate(0,80)">'
    start = seq_svg.find(open_tag, search_from)
    if start == -1:
        raise SystemExit(
            f'{open_tag!r} not found after LIVE_DEMO_HEADER_END in docs/sequence-diagram.svg -- '
            "did the body's wrapping group get restructured? Update this script's extraction to match.")
    start += len(open_tag)
    end = seq_svg.rfind("</g>")
    if end == -1 or end < start:
        raise SystemExit("no closing </g> found after the body's opening group in docs/sequence-diagram.svg")
    return seq_svg[start:end].strip()


def main() -> None:
    if len(sys.argv) != 2:
        raise SystemExit(f"usage: {sys.argv[0]} <output-path>")
    output_path = Path(sys.argv[1])

    arch_svg = read_text_or_exit(DOCS_DIR / "architecture-diagram.svg", "docs/architecture-diagram.svg").strip()
    seq_svg = read_text_or_exit(DOCS_DIR / "sequence-diagram.svg", "docs/sequence-diagram.svg")

    seq_header = extract_sequence_header(seq_svg)
    seq_body = extract_sequence_body(seq_svg)

    template = read_text_or_exit(TEMPLATE_PATH, "index.html template")
    for marker in (ARCH_MARKER, SEQ_HEADER_MARKER, SEQ_BODY_MARKER):
        if template.count(marker) != 1:
            raise SystemExit(
                f"expected exactly one {marker} in {TEMPLATE_PATH}, found {template.count(marker)} -- "
                "was the template edited without updating this script?")

    generated = (template
                 .replace(ARCH_MARKER, arch_svg)
                 .replace(SEQ_HEADER_MARKER, seq_header)
                 .replace(SEQ_BODY_MARKER, seq_body))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(generated, encoding=ENCODING)
    print(f"generate_demo_index.py: wrote {output_path} "
          f"({len(generated)} bytes) from docs/architecture-diagram.svg + docs/sequence-diagram.svg")


if __name__ == "__main__":
    main()
