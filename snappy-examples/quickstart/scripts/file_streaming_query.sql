--------------------------------------------------------------------------------------
---- Sql query on file stream table.
----To get the result for file stream using sql, 
----run quickstart/scripts/simulateTwitterStream from another shell 
--------------------------------------------------------------------------------------

--- Get top 10 popular hashtags ----
SELECT hashtag, count(*) as tagcount 
FROM HASHTAG_FILESTREAMTABLE 
GROUP BY hashtag 
ORDER BY tagcount DESC limit 10;

--- Get the top 10 popular retweet -----
SELECT retweetId as RetweetId, retweetCnt as RetweetsCount, retweetTxt as Text 
FROM RETWEET_FILESTREAMTABLE 
ORDER BY RetweetsCount DESC limit 10;

SELECT hashtag, count(hashtag) as TopKCount
FROM FILESTREAM_TOPKTABLE 
GROUP BY hashtag ORDER BY TopKCount limit 10;
