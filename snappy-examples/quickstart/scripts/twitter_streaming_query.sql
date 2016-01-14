------------------------------------------------------------
---- Sql query in live twitter stream -----
---- Get top 10 popular hashtasg ------
------------------------------------------------------------

SELECT hashtag, count(*) as tagcount FROM HASHTAGTABLE group by hashtag order by tagcount desc limit 10;

--- Get the top 10 popular retweet -----

SELECT retweetCnt as RetweetsCount, retweetTxt as Text FROM RETWEETTABLE order by RetweetsCount desc limit 10;

