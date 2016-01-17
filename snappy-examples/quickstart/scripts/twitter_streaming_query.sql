------------------------------------------------------------
---- Sql query in live twitter stream -----
---- Get top 10 popular hashtags ------
------------------------------------------------------------

SELECT hashtag, count(*) as tagcount 
FROM HASHTAGTABLE 
GROUP BY hashtag 
ORDER BY tagcount DESC limit 10;

--- Get the top 10 popular retweet -----
SELECT retweetId as RetweetId, retweetCnt as RetweetsCount, retweetTxt as Text 
FROM RETWEETTABLE 
ORDER BY RetweetsCount DESC limit 10;

