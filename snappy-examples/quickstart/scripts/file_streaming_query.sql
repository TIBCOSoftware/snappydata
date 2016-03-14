--------------------------------------------------------------------------------------
---- Sql query on file stream table.
----To get the result for file stream using sql, 
----run quickstart/scripts/simulateTwitterStream from another shell 
--------------------------------------------------------------------------------------

--- Get top 10 popular hashtags ----
SELECT hashtag, COUNT(*) AS tagcount
FROM hashtag_filestreamtable
GROUP BY hashtag 
ORDER BY tagcount DESC LIMIT 10;

--- Get the top 10 popular retweet -----
SELECT retweetId AS RetweetId, retweetCnt AS RetweetsCount, retweetTxt AS Text
FROM retweet_filestreamtable
ORDER BY RetweetsCount DESC LIMIT 10;

SELECT hashtag, COUNT(hashtag) AS TopKCount
FROM filestream_topktable
GROUP BY hashtag ORDER BY TopKCount LIMIT 10;
