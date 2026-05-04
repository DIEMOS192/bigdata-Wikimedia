# Wikimedia Pageviews - Spark Assignment 2

Spark-based analysis of Wikimedia pageview logs (Jan 1, 2016, 00:00-01:00). Each query is implemented twice: once using a map-reduce style approach and once using Spark loops, with runtime comparison and documented results.

## Data
- Source: Wikimedia pageview statistics (0-1am on Jan 1, 2016)
- File: pagecounts-20160101-000000_parsed.out (unzipped from the provided .zip)

## Tasks
1. Compute min, max, and average page size.
2. Count page titles ending with image extensions (.jpg, .png, .gif), and count how many are not in the English project (project code != "en").
3. Find the top 10 most frequent terms in page titles (case-insensitive, split by "_", with normalization).
4. Identify the top 5 projects by total page hits.
5. For each project, find the single page title with the highest page hits.

## Notes
- Implement each task twice (map-reduce vs Spark loops) and compare performance.
- Record timings and results in a separate report document.

## Run (PySpark)
```bash
python spark_app.py --input pagecounts-20160101-000000_parsed.out/pagecounts-20160101-000000_parsed.out
```

The script writes a report file named `results_report.md` by default.

## Expected Output Shape (Quick Verification)
- Q1: One tuple `(min_size, max_size, avg_size)`.
- Q2: One tuple `(image_title_count, non_english_image_count)`.
- Q3: 10 `(term, count)` pairs.
- Q4: 5 `(project_code, total_hits)` pairs.
- Q5: One `(project_code, page_title, hits)` per project.

## Approach Used (Per Problem)
Each query is implemented twice:
- **Map-reduce style:** uses Spark transformations like `map`, `reduceByKey`, `aggregate`, `takeOrdered`.
- **Spark loops:** does local loops inside `mapPartitions`, then merges partial results with `reduceByKey` or `reduce`.

Details by query:
1. **Min, max, average page size**
	- Map-reduce: aggregate sizes with a `(min, max, sum, count)` tuple.
	- Loops: compute `(min, max, sum, count)` per partition, then reduce them.
2. **Image titles + non-English count**
	- Map-reduce: filter titles ending with `.jpg`, `.png`, `.gif`, then count total and non-English.
	- Loops: count both values inside each partition loop, then sum them.
3. **Top 10 terms in titles**
	- Map-reduce: split titles by `_`, normalize to lowercase alphanumerics, count with `reduceByKey`, then take top 10.
	- Loops: build a local dictionary per partition, then merge and take top 10.
4. **Top 5 projects by total hits**
	- Map-reduce: `map(project, hits) -> reduceByKey(sum) -> takeOrdered(5)`.
	- Loops: sum hits per project in each partition, then merge and take top 5.
5. **Top page title per project**
	- Map-reduce: `reduceByKey` picks the `(title, hits)` with highest hits per project.
	- Loops: track best title per project inside each partition, then merge with `reduceByKey`.
