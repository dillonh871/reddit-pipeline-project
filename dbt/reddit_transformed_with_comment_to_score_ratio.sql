-- This SQL script retrieves data from the 'reddit' table, including post ID, title, number of comments, 
-- post score, author, creation timestamp (created_utc), post URL, upvote ratio, and calculates the comment-to-score ratio.
-- The comment-to-score ratio is calculated as the number of comments divided by the post score.

SELECT id, 
       title, 
       num_comments, 
       score,
       author,
       created_utc,
       url,
       upvote_ratio,
       num_comments::float / score AS comment_to_score_ratio
FROM dev.public.reddit;
