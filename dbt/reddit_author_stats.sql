-- This SQL script calculates statistics for Reddit authors, including the count of posts, 
-- the maximum post score, and the average number of comments. The results are grouped by author
-- and sorted in descending order based on the post count.
SELECT author, 
       COUNT(id) AS post_count,
       MAX(score) AS max_score,
       AVG(num_comments) AS avg_comments
FROM dev.public.reddit
GROUP BY author
ORDER BY post_count DESC;
