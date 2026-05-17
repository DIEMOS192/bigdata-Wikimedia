import argparse
import time
from typing import Dict

from pyspark import SparkConf, SparkContext

# Import processing modules
import mr_queries as mr
import loop_queries as loop
from utils import parse_line, write_report


def time_call(fn, *args):
    start = time.perf_counter()
    result = fn(*args)
    elapsed = time.perf_counter() - start
    return result, elapsed

ABSOLUTE_PATH_TO_DATA = "/mnt/Data/Uni/BigData/Assign2/data/pagecounts-20160101-000000_parsed.out"

def main() -> None:
    parser = argparse.ArgumentParser(description="Wikimedia pageviews analysis with PySpark")
    parser.add_argument(
        "--input",
        default=ABSOLUTE_PATH_TO_DATA,
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

    # Q1
    (mr_stats, mr_time) = time_call(mr.mr_page_size_stats, rdd)
    (loop_stats, loop_time) = time_call(loop.loop_page_size_stats, rdd, sc)
    results["Q1_page_size"] = {
        "title": "Q1: Min, max, and average page size",
        "mr_result": mr_stats,
        "mr_time": mr_time,
        "loop_result": loop_stats,
        "loop_time": loop_time,
    }

    # Q2
    (mr_img, mr_time) = time_call(mr.mr_image_counts, rdd)
    (loop_img, loop_time) = time_call(loop.loop_image_counts, rdd, sc)
    results["Q2_images"] = {
        "title": "Q2: Image page titles and non-English count",
        "mr_result": mr_img,
        "mr_time": mr_time,
        "loop_result": loop_img,
        "loop_time": loop_time,
    }

    # Q3
    (mr_terms, mr_time) = time_call(mr.mr_top_terms, rdd)
    (loop_terms, loop_time) = time_call(loop.loop_top_terms, rdd, sc)
    results["Q3_terms"] = {
        "title": "Q3: Top 10 terms in page titles",
        "mr_result": mr_terms,
        "mr_time": mr_time,
        "loop_result": loop_terms,
        "loop_time": loop_time,
    }

    # Q4
    (mr_projects, mr_time) = time_call(mr.mr_top_projects, rdd)
    (loop_projects, loop_time) = time_call(loop.loop_top_projects, rdd, sc)
    results["Q4_projects"] = {
        "title": "Q4: Top 5 projects by total page hits",
        "mr_result": mr_projects,
        "mr_time": mr_time,
        "loop_result": loop_projects,
        "loop_time": loop_time,
    }

    # Q5
    (mr_top_title, mr_time) = time_call(mr.mr_top_title_per_project, rdd)
    mr_top_title_sorted = sorted(mr_top_title, key=lambda x: x[0])

    start = time.perf_counter()
    loop_top_title_results = loop.loop_top_title_per_project(rdd, sc)
    loop_time = time.perf_counter() - start
    
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