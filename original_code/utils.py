import re
from typing import Iterator, List, Optional, Tuple

Record = Tuple[str, str, int, int]

IMAGE_EXTENSIONS = (".jpg", ".png", ".gif")
TERM_CLEAN_RE = re.compile(r"[^a-z0-9]+")


def parse_line(line: str) -> Optional[Record]:
    parts = line.split()
    if len(parts) < 4:
        return None
    try:
        project = parts[0]
        title = parts[1]
        hits = int(parts[2])
        size = int(parts[3])
    except ValueError:
        return None
    return project, title, hits, size


def normalize_terms(title: str) -> Iterator[str]:
    for raw in title.split("_"):
        cleaned = TERM_CLEAN_RE.sub("", raw.lower())
        if cleaned:
            yield cleaned


def iter_records_from_file(input_path: str) -> Iterator[Record]:
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            record = parse_line(line)
            if record is not None:
                yield record


def write_report(path: str, results: dict) -> None:
    def format_result(value: object) -> List[str]:
        return ["```", f"{value}", "```"]

    lines: List[str] = []
    lines.append("# Assignment 2 Results")
    lines.append("")
    for key in [
        "Q1_page_size",
        "Q2_images",
        "Q3_terms",
        "Q4_projects",
        "Q5_top_title",
    ]:
        block = results[key]
        lines.append(f"## {block['title']}")
        lines.append("")
        lines.append("### Map-Reduce")
        lines.append(f"- Time (s): {block['mr_time']:.6f}")
        lines.append("- Result:")
        lines.extend(format_result(block["mr_result"]))
        lines.append("")
        lines.append("### Spark Loops")
        lines.append(f"- Time (s): {block['loop_time']:.6f}")
        lines.append("- Result:")
        lines.extend(format_result(block["loop_result"]))
        lines.append("")

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))