import time
import os
import shutil
from pyspark import SparkConf, SparkContext

def parse_line(line):
    parts = line.split()
    if len(parts) < 4: return None
    try:
        return parts[0], parts[1], int(parts[2]), int(parts[3])
    except ValueError: return None

def main():
    conf = SparkConf().setAppName("Q5_TopTitle").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")

    input_path = "data/pagecounts-20160101-000000_parsed.out"
    rdd = sc.textFile(input_path).map(parse_line).filter(lambda x: x is not None).cache()

    # --- Map-Reduce Approach ---
    start_mr = time.perf_counter()
    mr_res_rdd = (
        rdd.map(lambda x: (x[0], (x[1], x[2])))
        .reduceByKey(lambda a, b: a if a[1] >= b[1] else b)
        .map(lambda x: (x[0], x[1][0], x[1][1]))
    )
    
    mr_output = "output/q5_mr"
    if os.path.exists(mr_output): shutil.rmtree(mr_output)
    mr_res_rdd.saveAsTextFile(mr_output)
    time_mr = time.perf_counter() - start_mr

    print(f"MR Execution Time (including save): {time_mr:.4f}s")

    # --- Spark Loop (mapPartitions) Approach ---
    start_loop = time.perf_counter()
    def part_best(it):
        best = {}
        for record in it:
            proj, title, hits = record[0], record[1], record[2]
            if proj not in best or hits > best[proj][1]:
                best[proj] = (title, hits)
        return iter(best.items())

    loop_res_rdd = (
        rdd.mapPartitions(part_best)
        .reduceByKey(lambda a, b: a if a[1] >= b[1] else b)
        .map(lambda x: (x[0], x[1][0], x[1][1]))
    )

    loop_output = "output/q5_loops"
    if os.path.exists(loop_output): shutil.rmtree(loop_output)
    loop_res_rdd.saveAsTextFile(loop_output)
    time_loop = time.perf_counter() - start_loop

    print(f"Loop Execution Time (including save): {time_loop:.4f}s")

    # Final summary in root output
    if not os.path.exists("output"): os.makedirs("output")
    with open(os.path.join("output", "q5_results.txt"), "w") as f:
        f.write(f"MR Output saved to: {mr_output}\n")
        f.write(f"Loop Output saved to: {loop_output}\n")

    sc.stop()

if __name__ == "__main__":
    main()
