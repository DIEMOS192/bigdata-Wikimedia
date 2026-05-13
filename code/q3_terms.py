import time
import os
import re
from pyspark import SparkConf, SparkContext

TERM_CLEAN_RE = re.compile(r"[^a-z0-9]+")

def parse_line(line):
    parts = line.split()
    if len(parts) < 4: return None
    try:
        return parts[0], parts[1], int(parts[2]), int(parts[3])
    except ValueError: return None

def normalize_terms(title):
    for raw in title.split("_"):
        cleaned = TERM_CLEAN_RE.sub("", raw.lower())
        if cleaned: yield cleaned

def main():
    conf = SparkConf().setAppName("Q3_Terms").setMaster("local[*]")
    conf.set("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    input_path = "data/pagecounts-20160101-000000_parsed.out"
    rdd = sc.textFile(input_path).map(parse_line).filter(lambda x: x is not None).cache()

    # --- Map-Reduce Approach ---
    start_mr = time.perf_counter()
    mr_res = (
        rdd.flatMap(lambda x: normalize_terms(x[1]))
        .map(lambda term: (term, 1))
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(10, key=lambda x: -x[1])
    )
    time_mr = time.perf_counter() - start_mr

    print(f"MR Result: {mr_res}")
    print(f"MR Execution Time: {time_mr:.4f}s")

    # --- Spark Loop (mapPartitions) Approach ---
    start_loop = time.perf_counter()
    def part_counts(it):
        counts = {}
        for record in it:
            for term in normalize_terms(record[1]):
                counts[term] = counts.get(term, 0) + 1
        return iter(counts.items())

    loop_res = (
        rdd.mapPartitions(part_counts)
        .reduceByKey(lambda a, b: a + b)
        .takeOrdered(10, key=lambda x: -x[1])
    )
    time_loop = time.perf_counter() - start_loop

    print(f"Loop Result: {loop_res}")
    print(f"Loop Execution Time: {time_loop:.4f}s")

    # Save output
    if not os.path.exists("output"): os.makedirs("output")
    with open(os.path.join("output", "q3_results.txt"), "w") as f:
        f.write(f"MR: {mr_res}\n")
        f.write(f"Loop: {loop_res}\n")

    sc.stop()

if __name__ == "__main__":
    main()
