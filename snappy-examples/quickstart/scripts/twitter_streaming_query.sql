------------------------------------------------------------
---- Sql query in live twitter stream -----
---- Get top 10 popular hashtags ------
------------------------------------------------------------

SELECT hashtag, COUNT(*) AS tagcount
FROM hashtagtable
GROUP BY hashtag
ORDER BY tagcount DESC LIMIT 10;

--- Get the top 10 popular retweet -----
SELECT retweetId AS RetweetId, retweetCnt AS RetweetsCount, retweetTxt AS Text
FROM retweettable
ORDER BY RetweetsCount DESC LIMIT 10;

SELECT hashtag, COUNT(hashtag) AS TopKCount
FROM TOPKTABLE
GROUP BY hashtag ORDER BY TopKCount LIMIT 10;
