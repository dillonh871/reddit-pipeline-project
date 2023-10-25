-- This SQL script extracts data from the 'reddit' table and transforms the 'created_utc' column into separate 'utc_date' and 'utc_time' columns.

SELECT id, 
       title, 
       num_comments, 
       score,
       author,
       created_utc,
       url,
       upvote_ratio,
       created_utc::date as utc_date,
       created_utc::time as utc_time
FROM dev.public.reddit