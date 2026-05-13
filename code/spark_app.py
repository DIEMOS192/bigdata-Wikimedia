# NOTE: The command to run this script correctly can be found in the README.md file
import argparse
import re
import time
from typing import Dict, Iterator, List, Optional, Tuple

from pyspark import SparkConf, SparkContext

Record = Tuple[str, str, int, int]


from spark_app_mapreduce import (
    IMAGE_EXTENSIONS,
    mr_image_counts,
    mr_page_size_stats,
    mr_top_projects,
    mr_top_title_per_project,
    mr_top_terms,
    normalize_terms,
)


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


def iter_records_from_file(input_path: str):
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            record = parse_line(line)
            if record is not None:
                yield record


# --- Pure Python loop-based implementations ---

def loop_page_size_stats(input_path: str) -> Tuple[int, int, float]:
    min_val = max_val = None
    total_sum = total_count = 0

    for *_, size in iter_records_from_file(input_path):
        if min_val is None or size < min_val:
            min_val = size
        if max_val is None or size > max_val:
            max_val = size
        total_sum += size
        total_count += 1

    avg_val = (total_sum / total_count) if total_count > 0 else 0.0
    return int(min_val or 0), int(max_val or 0), float(avg_val)


def loop_image_counts(input_path: str) -> Tuple[int, int]:
    total_images = non_en_images = 0

    for project, title, *_ in iter_records_from_file(input_path):
        if title.lower().endswith(IMAGE_EXTENSIONS):
            total_images += 1
            if project != "en":
                non_en_images += 1

    return int(total_images), int(non_en_images)


def loop_top_terms(input_path: str) -> List[Tuple[str, int]]:
    term_counts: Dict[str, int] = {}
    
    for _, title, *_ in iter_records_from_file(input_path):
        for term in normalize_terms(title):
            term_counts[term] = term_counts.get(term, 0) + 1

    # Sort descending by count and return top 10
    sorted_terms = sorted(term_counts.items(), key=lambda item: -item[1])
    return sorted_terms[:10]


def loop_top_projects(input_path: str) -> List[Tuple[str, int]]:
    project_counts: Dict[str, int] = {}

    for project, _, hits, _ in iter_records_from_file(input_path):
        project_counts[project] = project_counts.get(project, 0) + hits

    # Sort descending by hits and return top 5
    sorted_projects = sorted(project_counts.items(), key=lambda item: -item[1])
    return sorted_projects[:5]


def loop_top_title_per_project(input_path: str):
    best: Dict[str, Tuple[str, int]] = {}

    for project, title, hits, _ in iter_records_from_file(input_path):
        current = best.get(project)
        if current is None or hits > current[1]:
            best[project] = (title, hits)

    result_list = [(project, title, hits) for project, (title, hits) in best.items()]
    return sorted(result_list, key=lambda x: x[0])


def time_call(fn, *args):
    start = time.perf_counter()
    result = fn(*args)
    elapsed = time.perf_counter() - start
    return result, elapsed


def write_report(path: str, results: Dict[str, Dict[str, object]]) -> None:
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Wikimedia pageviews analysis with PySpark")
    parser.add_argument(
        "--input",
        default="pagecounts-20160101-000000_parsed.out",
        help="Path to the pageviews data file",
    )
    parser.add_argument(
        "--master",
        default="local[*]",
        help="Spark master URL (default: local[*])",
    )
    parser.add_argument(
        "--report",
        default="results_report.md",
        help="Output report file path",
    )
    args = parser.parse_args()

    conf = SparkConf().setAppName("wikimedia-pageviews-assignment2").setMaster(args.master)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    rdd = sc.textFile(args.input).map(parse_line).filter(lambda x: x is not None).cache()

    results: Dict[str, Dict[str, object]] = {}

    (mr_stats, mr_time) = time_call(mr_page_size_stats, rdd)
    (loop_stats, loop_time) = time_call(loop_page_size_stats, args.input)
    results["Q1_page_size"] = {
        "title": "Q1: Min, max, and average page size",
        "mr_result": mr_stats,
        "mr_time": mr_time,
        "loop_result": loop_stats,
        "loop_time": loop_time,
    }

    (mr_img, mr_time) = time_call(mr_image_counts, rdd)
    (loop_img, loop_time) = time_call(loop_image_counts, args.input)
    results["Q2_images"] = {
        "title": "Q2: Image page titles and non-English count",
        "mr_result": mr_img,
        "mr_time": mr_time,
        "loop_result": loop_img,
        "loop_time": loop_time,
    }

    (mr_terms, mr_time) = time_call(mr_top_terms, rdd)
    (loop_terms, loop_time) = time_call(loop_top_terms, args.input)
    results["Q3_terms"] = {
        "title": "Q3: Top 10 terms in page titles",
        "mr_result": mr_terms,
        "mr_time": mr_time,
        "loop_result": loop_terms,
        "loop_time": loop_time,
    }

    (mr_projects, mr_time) = time_call(mr_top_projects, rdd)
    (loop_projects, loop_time) = time_call(loop_top_projects, args.input)
    results["Q4_projects"] = {
        "title": "Q4: Top 5 projects by total page hits",
        "mr_result": mr_projects,
        "mr_time": mr_time,
        "loop_result": loop_projects,
        "loop_time": loop_time,
    }

    (mr_top_title, mr_time) = time_call(mr_top_title_per_project, rdd)
    mr_top_title_sorted = sorted(mr_top_title, key=lambda x: x[0])

    start = time.perf_counter()
    loop_top_title_results = loop_top_title_per_project(args.input)
    loop_time = time.perf_counter() - start
    
    # Format exactly like Spark's textFile would output
    loop_lines = [f"{item}" for item in loop_top_title_results]

    loop_top_title_text = "\n".join(loop_lines)
    results["Q5_top_title"] = {
        "title": "Q5: Top page title per project by hits",
        "mr_result": mr_top_title_sorted,
        "mr_time": mr_time,
        "loop_result": loop_top_title_text,
        "loop_time": loop_time,
    }

    if args.report:
        write_report(args.report, results)

    print("Results:")
    for key, block in results.items():
        print(block["title"])
        print("  Map-Reduce:", block["mr_result"], f"(time {block['mr_time']:.6f}s)")
        print("  Spark Loops:", block["loop_result"], f"(time {block['loop_time']:.6f}s)")

    sc.stop()


if __name__ == "__main__":
    main()
