select * from information_schema.tables;

select * from wikimedia_pageviews limit 5;

CREATE INDEX idx_project_code ON wikimedia_pageviews(project_code);

-- Q1: Min, max, average
SELECT MIN(page_size), MAX(page_size), AVG(page_size) FROM wikimedia_pageviews;

-- Q2: Images and non-English count
SELECT 
    COUNT(*) AS image_title_count,
    SUM(CASE WHEN project_code != 'en' THEN 1 ELSE 0 END) AS non_english_image_count
FROM wikimedia_pageviews
WHERE page_title ~* '\.(jpg|png|gif)$';


-- Q3: Top 10 terms
WITH words AS (
    SELECT regexp_replace(
             regexp_split_to_table(lower(page_title), '_'), 
             '[^a-z0-9]', '', 'g'
           ) AS term
    FROM wikimedia_pageviews
)
SELECT term, COUNT(*) AS count
FROM words
WHERE term != '' 
GROUP BY term
ORDER BY count DESC
LIMIT 10;

-- Q4: Top 5 projects
SELECT project_code, SUM(page_hits) AS total_hits
FROM wikimedia_pageviews
GROUP BY project_code
ORDER BY total_hits DESC
LIMIT 5;

-- Q5: Top page title per project
SELECT DISTINCT ON (project_code) 
    project_code, page_title, page_hits
FROM wikimedia_pageviews
ORDER BY project_code, page_hits DESC;