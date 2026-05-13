import time
import os
from pyspark import SparkConf, SparkContext

def parse_line(line):
    parts = line.split()
    if len(parts) < 4: return None
    try:
        return parts[0], parts[1], int(parts[2]), int(parts[3])
    except ValueError: return None

def main():
    conf = SparkConf().setAppName("Q4_Projects").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    input_path = "data/pagecounts-20160101-000000_parsed.out"
    rdd = sc.textFile(input_path).map(parse_line).filter(lambda x: x is not None).cache()

    # --- Map-Reduce Approach ---
    start_mr = time.perf_counter()
    mr_res = (
        rdd.map(lambda x: (x[0], x[2]))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(5, key=lambda x: -x[1])
    )
    time_mr = time.perf_counter() - start_mr

    print(f"MR Result: {mr_res}")
    print(f"MR Execution Time: {time_mr:.4f}s")

    # --- Spark Loop (mapPartitions) Approach ---
    start_loop = time.perf_counter()
    def part_counts(it):
        counts = {}
        for record in it:
            proj = record[0]
            hits = record[2]
            counts[proj] = counts.get(proj, 0) + hits
        return iter(counts.items())

    loop_res = (
        rdd.mapPartitions(part_counts)
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(5, key=lambda x: -x[1])
    )
    time_loop = time.perf_counter() - start_loop

    print(f"Loop Result: {loop_res}")
    print(f"Loop Execution Time: {time_loop:.4f}s")

    # Save output
    if not os.path.exists("output"): os.makedirs("output")
    with open(os.path.join("output", "q4_results.txt"), "w") as f:
        f.write(f"MR: {mr_res}\n")
        f.write(f"Loop: {loop_res}\n")

    sc.stop()

if __name__ == "__main__":
    main()
