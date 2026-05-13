import time
import os
from pyspark import SparkConf, SparkContext

def parse_line(line):
    parts = line.split()
    if len(parts) < 4:
        return None
    try:
        return parts[0], parts[1], int(parts[2]), int(parts[3])
    except ValueError:
        return None

def main():
    conf = SparkConf().setAppName("Q1_PageSize").setMaster("local[*]")
    conf.set("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    input_path = "../data/pagecounts-20160101-000000_parsed.out"
    rdd = sc.textFile(input_path).map(parse_line).filter(lambda x: x is not None).cache()

    # --- Map-Reduce Approach ---
    start_mr = time.perf_counter()
    sizes = rdd.map(lambda x: x[3])
    mr_res = sizes.aggregate(
        (None, None, 0, 0),
        lambda acc, v: (
            v if acc[0] is None or v < acc[0] else acc[0],
            v if acc[1] is None or v > acc[1] else acc[1],
            acc[2] + v,
            acc[3] + 1
        ),
        lambda a, b: (
            b[0] if a[0] is None or (b[0] is not None and b[0] < a[0]) else a[0],
            b[1] if a[1] is None or (b[1] is not None and b[1] > a[1]) else a[1],
            a[2] + b[2],
            a[3] + b[3]
        )
    )
    min_v, max_v, sum_v, count_v = mr_res
    avg_v = sum_v / count_v if count_v else 0.0
    time_mr = time.perf_counter() - start_mr

    print(f"MR Result: Min={min_v}, Max={max_v}, Avg={avg_v:.2f}")
    print(f"MR Execution Time: {time_mr:.4f}s")

    # --- Spark Loop (mapPartitions) Approach ---
    start_loop = time.perf_counter()
    def part_stats(it):
        p_min, p_max, p_sum, p_count = None, None, 0, 0
        for record in it:
            val = record[3]
            if p_min is None or val < p_min: p_min = val
            if p_max is None or val > p_max: p_max = val
            p_sum += val
            p_count += 1
        if p_count > 0:
            yield (p_min, p_max, p_sum, p_count)

    loop_res = rdd.mapPartitions(part_stats).reduce(
        lambda a, b: (
            min(a[0], b[0]),
            max(a[1], b[1]),
            a[2] + b[2],
            a[3] + b[3]
        )
    )
    l_min, l_max, l_sum, l_count = loop_res
    l_avg = l_sum / l_count if l_count else 0.0
    time_loop = time.perf_counter() - start_loop

    print(f"Loop Result: Min={l_min}, Max={l_max}, Avg={l_avg:.2f}")
    print(f"Loop Execution Time: {time_loop:.4f}s")

    # Save output
    output_dir = "output/q1_stats"
    if not os.path.exists("output"): os.makedirs("output")
    with open(os.path.join("output", "q1_results.txt"), "w") as f:
        f.write(f"MR: {mr_res}, Avg: {avg_v}\n")
        f.write(f"Loop: {loop_res}, Avg: {l_avg}\n")

    sc.stop()

if __name__ == "__main__":
    main()
