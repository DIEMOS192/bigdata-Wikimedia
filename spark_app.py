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
    sizes = rdd.map(lambda record: record[3])
    
    def seq_op(acc, val):
        min_v, max_v, sum_v, count = acc
        new_min = val if min_v is None else min(min_v, val)
        new_max = val if max_v is None else max(max_v, val)
        return (new_min, new_max, sum_v + val, count + 1)
        
    def comb_op(acc1, acc2):
        min1, max1, sum1, count1 = acc1
        min2, max2, sum2, count2 = acc2
        
        if min1 is None: new_min = min2
        elif min2 is None: new_min = min1
        else: new_min = min(min1, min2)
            
        if max1 is None: new_max = max2
        elif max2 is None: new_max = max1
        else: new_max = max(max1, max2)
            
        return (new_min, new_max, sum1 + sum2, count1 + count2)

    min_val, max_val, total_sum, total_count = sizes.aggregate(
        (None, None, 0, 0), seq_op, comb_op
    )
    
    avg_val = (total_sum / total_count) if total_count > 0 else 0.0
    return int(min_val or 0), int(max_val or 0), float(avg_val)


def mr_image_counts(rdd) -> Tuple[int, int]:
    images = rdd.filter(lambda record: record[1].lower().endswith(IMAGE_EXTENSIONS))
    images.cache()
    
    total_images = images.count()
    non_en_images = images.filter(lambda record: record[0] != "en").count()
    
    images.unpersist()
    return int(total_images), int(non_en_images)


def mr_top_terms(rdd) -> List[Tuple[str, int]]:
    return (
        rdd.flatMap(lambda record: normalize_terms(record[1]))
        .map(lambda term: (term, 1))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(10, key=lambda item: -item[1])
    )


def mr_top_projects(rdd) -> List[Tuple[str, int]]:
    return (
        rdd.map(lambda record: (record[0], record[2]))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(5, key=lambda item: -item[1])
    )


def mr_top_title_per_project(rdd) -> List[Tuple[str, str, int]]:
    return (
        rdd.map(lambda record: (record[0], (record[1], record[2])))
        .reduceByKey(lambda a, b: a if a[1] >= b[1] else b)
        .map(lambda item: (item[0], item[1][0], item[1][1]))
        .collect()
    )


# --- Loop-based implementations (partition-local loops) ---

def loop_page_size_stats(rdd) -> Tuple[int, int, float]:
    min_val = max_val = None
    total_sum = total_count = 0

    for *_, size in rdd.toLocalIterator():
        if min_val is None or size < min_val:
            min_val = size
        if max_val is None or size > max_val:
            max_val = size
        total_sum += size
        total_count += 1

    avg_val = (total_sum / total_count) if total_count > 0 else 0.0
    return int(min_val or 0), int(max_val or 0), float(avg_val)


def loop_image_counts(rdd) -> Tuple[int, int]:
    total_images = non_en_images = 0

    for project, title, *_ in rdd.toLocalIterator():
        if title.lower().endswith(IMAGE_EXTENSIONS):
            total_images += 1
            if project != "en":
                non_en_images += 1

    return int(total_images), int(non_en_images)


def loop_top_terms(rdd) -> List[Tuple[str, int]]:
    term_counts: Dict[str, int] = {}
    
    for _, title, *_ in rdd.toLocalIterator():
        for term in normalize_terms(title):
            term_counts[term] = term_counts.get(term, 0) + 1

    # Sort descending by count and return top 10
    sorted_terms = sorted(term_counts.items(), key=lambda item: -item[1])
    return sorted_terms[:10]


def loop_top_projects(rdd) -> List[Tuple[str, int]]:
    project_counts: Dict[str, int] = {}

    for project, _, hits, _ in rdd.toLocalIterator():
        project_counts[project] = project_counts.get(project, 0) + hits

    # Sort descending by hits and return top 5
    sorted_projects = sorted(project_counts.items(), key=lambda item: -item[1])
    return sorted_projects[:5]


def loop_top_title_per_project_rdd(rdd):
    best: Dict[str, Tuple[str, int]] = {}

    for project, title, hits, _ in rdd.toLocalIterator():
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

    start = time.perf_counter()
    loop_top_title_results = loop_top_title_per_project_rdd(rdd)
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
