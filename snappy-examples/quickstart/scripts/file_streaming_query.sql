
--------------------------------------------------------------------------------------
---- Sql query on file stream.-----
----To get the result for file stream using sql, -----------------
----run quickstart/scripts/simulateTwitterStream from another shell ----
--------------------------------------------------------------------------------------

--- Get top 10 popular hashtasg ----
SELECT hashtag, count(*) as tagcount FROM HASHTAG_FILESTREAMTABLE group by hashtag order by tagcount desc limit 10;

--- Get the top 10 popular retweet -----

SELECT retweetCnt as RetweetsCount, retweetTxt as Text FROM RETWEET_FILESTREAMTABLE order by RetweetsCount desc limit 10;
