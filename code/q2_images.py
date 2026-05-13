import time
import os
from pyspark import SparkConf, SparkContext

IMAGE_EXTENSIONS = (".jpg", ".png", ".gif")

def parse_line(line):
    parts = line.split()
    if len(parts) < 4: return None
    try:
        return parts[0], parts[1], int(parts[2]), int(parts[3])
    except ValueError: return None

def main():
    conf = SparkConf().setAppName("Q2_Images").setMaster("local[*]")
    conf.set("spark.driver.extraJavaOptions", "--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    input_path = "../data/pagecounts-20160101-000000_parsed.out"
    rdd = sc.textFile(input_path).map(parse_line).filter(lambda x: x is not None).cache()

    # --- Map-Reduce Approach ---
    start_mr = time.perf_counter()
    images = rdd.filter(lambda x: x[1].lower().endswith(IMAGE_EXTENSIONS))
    total_images = images.count()
    non_en_images = images.filter(lambda x: x[0] != "en").count()
    time_mr = time.perf_counter() - start_mr

    print(f"MR Result: Total={total_images}, Non-EN={non_en_images}")
    print(f"MR Execution Time: {time_mr:.4f}s")

    # --- Spark Loop (mapPartitions) Approach ---
    start_loop = time.perf_counter()
    def part_counts(it):
        total, non_en = 0, 0
        for record in it:
            if record[1].lower().endswith(IMAGE_EXTENSIONS):
                total += 1
                if record[0] != "en":
                    non_en += 1
        yield (total, non_en)

    loop_res = rdd.mapPartitions(part_counts).reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))
    time_loop = time.perf_counter() - start_loop

    print(f"Loop Result: Total={loop_res[0]}, Non-EN={loop_res[1]}")
    print(f"Loop Execution Time: {time_loop:.4f}s")

    # Save output
    if not os.path.exists("output"): os.makedirs("output")
    with open(os.path.join("output", "q2_results.txt"), "w") as f:
        f.write(f"MR: Total={total_images}, Non-EN={non_en_images}\n")
        f.write(f"Loop: Total={loop_res[0]}, Non-EN={loop_res[1]}\n")

    sc.stop()

if __name__ == "__main__":
    main()
