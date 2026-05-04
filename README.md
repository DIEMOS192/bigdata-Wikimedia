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
