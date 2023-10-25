-- This SQL script retrieves data from the 'reddit' table, including post ID, title, number of comments, 
-- post score, author, creation timestamp (created_utc), post URL, upvote ratio, and calculates the age of each post in hours.
-- The age is calculated as the time difference between the current timestamp (NOW()) and the post's creation timestamp (created_utc),
-- expressed in hours.

SELECT id, 
       title, 
       num_comments, 
       score,
       author,
       created_utc,
       url,
       upvote_ratio,
       (EXTRACT(EPOCH FROM NOW()) - EXTRACT(EPOCH FROM created_utc)) / 3600 AS post_age_hours
FROM dev.public.reddit;
