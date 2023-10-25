-- This SQL script retrieves data from the 'reddit' table and calculates a 'score_category' for each post based on its score.
-- Posts with a score of 1000 or higher are categorized as 'High Score', posts with a score of 100 or higher are categorized as 'Moderate Score',
-- and all other posts are categorized as 'Low Score'.

SELECT id, 
       title, 
       num_comments, 
       score,
       author,
       created_utc,
       url,
       upvote_ratio,
       CASE
           WHEN score >= 1000 THEN 'High Score'
           WHEN score >= 100 THEN 'Moderate Score'
           ELSE 'Low Score'
       END AS score_category
FROM dev.public.reddit;
