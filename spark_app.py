import argparse
import os
import re
import shutil
import time
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

from pyspark import SparkConf, SparkContext

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


# --- Map-reduce style implementations ---

def mr_page_size_stats(rdd) -> Tuple[int, int, float]:
    sizes = rdd.map(lambda x: x[3])
    min_v, max_v, sum_v, count_v = sizes.aggregate(
        (None, None, 0, 0),
        lambda acc, v: (
            v if acc[0] is None or v < acc[0] else acc[0],
            v if acc[1] is None or v > acc[1] else acc[1],
            acc[2] + v,
            acc[3] + 1,
        ),
        lambda a, b: (
            b[0] if a[0] is None or (b[0] is not None and b[0] < a[0]) else a[0],
            b[1] if a[1] is None or (b[1] is not None and b[1] > a[1]) else a[1],
            a[2] + b[2],
            a[3] + b[3],
        ),
    )
    avg_v = (sum_v / count_v) if count_v else 0.0
    return int(min_v), int(max_v), float(avg_v)


def mr_image_counts(rdd) -> Tuple[int, int]:
    def is_image(title: str) -> bool:
        low = title.lower()
        return low.endswith(IMAGE_EXTENSIONS)

    images = rdd.filter(lambda x: is_image(x[1]))
    total_images = images.count()
    non_en_images = images.filter(lambda x: x[0] != "en").count()
    return int(total_images), int(non_en_images)


def mr_top_terms(rdd) -> List[Tuple[str, int]]:
    return (
        rdd.flatMap(lambda x: normalize_terms(x[1]))
        .map(lambda t: (t, 1))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(10, key=lambda x: -x[1])
    )


def mr_top_projects(rdd) -> List[Tuple[str, int]]:
    return (
        rdd.map(lambda x: (x[0], x[2]))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(5, key=lambda x: -x[1])
    )


def mr_top_title_per_project(rdd) -> List[Tuple[str, str, int]]:
    return (
        rdd.map(lambda x: (x[0], (x[1], x[2])))
        .reduceByKey(lambda a, b: a if a[1] >= b[1] else b)
        .map(lambda x: (x[0], x[1][0], x[1][1]))
        .collect()
    )


# --- Loop-based implementations (partition-local loops) ---

def loop_page_size_stats(rdd) -> Tuple[int, int, float]:
    def part_stats(it: Iterable[Record]) -> Iterator[Tuple[int, int, int, int]]:
        min_v = None
        max_v = None
        sum_v = 0
        count_v = 0
        for _, _, _, size in it:
            if min_v is None or size < min_v:
                min_v = size
            if max_v is None or size > max_v:
                max_v = size
            sum_v += size
            count_v += 1
        if count_v == 0:
            return iter(())
        return iter([(min_v, max_v, sum_v, count_v)])

    partials = rdd.mapPartitions(part_stats)

    def combine(a, b):
        return (
            b[0] if a[0] is None or b[0] < a[0] else a[0],
            b[1] if a[1] is None or b[1] > a[1] else a[1],
            a[2] + b[2],
            a[3] + b[3],
        )

    min_v, max_v, sum_v, count_v = partials.reduce(combine)
    avg_v = (sum_v / count_v) if count_v else 0.0
    return int(min_v), int(max_v), float(avg_v)


def loop_image_counts(rdd) -> Tuple[int, int]:
    def part_counts(it: Iterable[Record]) -> Iterator[Tuple[int, int]]:
        total = 0
        non_en = 0
        for project, title, _, _ in it:
            low = title.lower()
            if low.endswith(IMAGE_EXTENSIONS):
                total += 1
                if project != "en":
                    non_en += 1
        if total == 0 and non_en == 0:
            return iter(())
        return iter([(total, non_en)])

    partials = rdd.mapPartitions(part_counts)
    total, non_en = partials.reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    return int(total), int(non_en)


def loop_top_terms(rdd) -> List[Tuple[str, int]]:
    def part_counts(it: Iterable[Record]) -> Iterator[Tuple[str, int]]:
        counts: Dict[str, int] = {}
        for _, title, _, _ in it:
            for term in normalize_terms(title):
                counts[term] = counts.get(term, 0) + 1
        return iter(counts.items())

    return (
        rdd.mapPartitions(part_counts)
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(10, key=lambda x: -x[1])
    )


def loop_top_projects(rdd) -> List[Tuple[str, int]]:
    def part_counts(it: Iterable[Record]) -> Iterator[Tuple[str, int]]:
        counts: Dict[str, int] = {}
        for project, _, hits, _ in it:
            counts[project] = counts.get(project, 0) + hits
        return iter(counts.items())

    return (
        rdd.mapPartitions(part_counts)
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(5, key=lambda x: -x[1])
    )


def loop_top_title_per_project_rdd(rdd):
    def part_best(it: Iterable[Record]) -> Iterator[Tuple[str, Tuple[str, int]]]:
        best: Dict[str, Tuple[str, int]] = {}
        for project, title, hits, _ in it:
            current = best.get(project)
            if current is None or hits > current[1]:
                best[project] = (title, hits)
        return iter(best.items())

    return (
        rdd.mapPartitions(part_best)
        .reduceByKey(lambda a, b: a if a[1] >= b[1] else b)
        .map(lambda x: (x[0], x[1][0], x[1][1]))
    )


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
        default="pagecounts-20160101-000000_parsed.out/pagecounts-20160101-000000_parsed.out",
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
    (loop_stats, loop_time) = time_call(loop_page_size_stats, rdd)
    results["Q1_page_size"] = {
        "title": "Q1: Min, max, and average page size",
        "mr_result": mr_stats,
        "mr_time": mr_time,
        "loop_result": loop_stats,
        "loop_time": loop_time,
    }

    (mr_img, mr_time) = time_call(mr_image_counts, rdd)
    (loop_img, loop_time) = time_call(loop_image_counts, rdd)
    results["Q2_images"] = {
        "title": "Q2: Image page titles and non-English count",
        "mr_result": mr_img,
        "mr_time": mr_time,
        "loop_result": loop_img,
        "loop_time": loop_time,
    }

    (mr_terms, mr_time) = time_call(mr_top_terms, rdd)
    (loop_terms, loop_time) = time_call(loop_top_terms, rdd)
    results["Q3_terms"] = {
        "title": "Q3: Top 10 terms in page titles",
        "mr_result": mr_terms,
        "mr_time": mr_time,
        "loop_result": loop_terms,
        "loop_time": loop_time,
    }

    (mr_projects, mr_time) = time_call(mr_top_projects, rdd)
    (loop_projects, loop_time) = time_call(loop_top_projects, rdd)
    results["Q4_projects"] = {
        "title": "Q4: Top 5 projects by total page hits",
        "mr_result": mr_projects,
        "mr_time": mr_time,
        "loop_result": loop_projects,
        "loop_time": loop_time,
    }

    (mr_top_title, mr_time) = time_call(mr_top_title_per_project, rdd)
    mr_top_title_sorted = sorted(mr_top_title, key=lambda x: x[0])

    loop_top_title_rdd = loop_top_title_per_project_rdd(rdd)
    loop_out_dir = "results_q5_loop"
    if os.path.exists(loop_out_dir):
        shutil.rmtree(loop_out_dir)
    start = time.perf_counter()
    loop_top_title_rdd.saveAsTextFile(loop_out_dir)
    loop_time = time.perf_counter() - start
    loop_lines: List[str] = []
    for name in sorted(os.listdir(loop_out_dir)):
        if name.startswith("part-"):
            with open(os.path.join(loop_out_dir, name), "r", encoding="utf-8") as f:
                loop_lines.extend([line.rstrip("\n") for line in f])
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
