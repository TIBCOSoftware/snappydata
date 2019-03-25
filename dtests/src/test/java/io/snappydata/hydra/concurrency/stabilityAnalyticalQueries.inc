io.snappydata.hydra.cluster.SnappyPrms-analyticalQueryList =
  " select avg(arrdelay), avg(depdelay) from airline group by flightnum"
  " select avg(arrdelay), avg(depdelay), uniquecarrier from airline group by uniquecarrier"
  " select avg(arrdelay), avg(depdelay), uniquecarrier, year_ from airline group by uniquecarrier, year_"
  " select avg(arrdelay) from airline"
  " select avg(arrdelay) from airline group by uniquecarrier"
  " select avg(arrdelay), max(arrdelay), avg(depdelay), max(depdelay), origin from airline group by origin order by avg(arrdelay) desc"
  " select avg(arrdelay), uniquecarrier from airline group by uniquecarrier"
  " select avg(arrdelay), uniquecarrier from airline where uniquecarrier = 'UA' group by uniquecarrier"
  " select avg(arrdelay), uniquecarrier, year_, month_ from airline group by uniquecarrier, year_, month_  order by avg(arrdelay) desc"
  " select avg(depdelay) avgdelay, avg(arrdelay), uniquecarrier from airline where uniquecarrier in ('UA', 'DL') group by uniquecarrier order by avgdelay desc"
  " select avg(depdelay) avgdelay, uniquecarrier from airline group by uniquecarrier order by avgdelay desc"
  " select avg(depdelay) avgdelay, uniquecarrier from airline where uniquecarrier in ('UA', 'DL') group by uniquecarrier order by avgdelay desc"
  " select avg(depdelay) avgdelay, uniquecarrier from airline where uniquecarrier = 'UA' group by uniquecarrier order by avgdelay desc"
  " select avg(depdelay) delay, dest, month_ from airline where month_ in (10, 11, 12, 1) group by month_, dest order by delay"
  " select avg(depdelay) from airline where dayofweek in (1, 2)"
  " select avg(depdelay), origin from airline group by origin order by avg(depdelay) limit 50"
  " select avg(depdelay), uniquecarrier from airline group by uniquecarrier"
  " select avg(depdelay), uniquecarrier from airline where dayofweek in (1, 2) group by uniquecarrier"
  " select avg(arrdelay), count(*) from airline where uniquecarrier = 'UA'"
  " select avg(arrdelay) arrdelay, dest, month_ from airline where month_ in (10, 11, 12, 1) group by month_, dest order by arrdelay"
  " select avg(arrdelay) arrdelay, month_ from airline group by month_ order by arrdelay"
  " select count(*), avg(arrdelay), avg(depdelay), dayofweek from airline group by dayofweek"
  " select count(*), avg(arrdelay), avg(depdelay), dayofweek, month_ from airline group by month_, dayofweek order by month_, dayofweek "
  " select count(*) from (select avg(arrdelay), avg(depdelay) from airline group by flightnum)t1"
  " select count(*) from airline"
  " select count(*), uniquecarrier from airline group by uniquecarrier"
  " select count(*), uniquecarrier, origin from airline group by uniquecarrier, origin order by uniquecarrier, origin"
  " select count(*), uniquecarrier, year_ from airline group by uniquecarrier, year_"
  " select count(*), uniquecarrier, year_, month_ from airline group by year_, month_, uniquecarrier order by year_, month_, uniquecarrier desc"
  " select * from airline limit 1"
  " select max(arrdelay), max(depdelay), uniquecarrier from airline group by uniquecarrier"
  " select max(arrdelay), uniquecarrier from airline group by uniquecarrier"
  " select sum(arrdelay + depdelay), count(*) from airline group by uniquecarrier"
  " select sum(arrdelay + depdelay), count(*) from airline group by uniquecarrier limit 10"
  " select sum(arrdelay + depdelay), count(*) from airline where uniquecarrier = 'AA' group by uniquecarrier"
  " select avg(arrdelay), avg(depdelay) from airline group by flightnum"
  " select avg(arrdelay), avg(depdelay), uniquecarrier from airline group by uniquecarrier"
  " select avg(arrdelay), avg(depdelay), uniquecarrier, year_ from airline group by uniquecarrier, year_"
  /*" select avg(arrdelay), avg(depdelay) from STAGING_AIRLINE group by flightnum"
  " select avg(arrdelay) from STAGING_AIRLINE"*/
  /*" select avg(arrdelay) from STAGING_AIRLINE group by uniquecarrier"*/
  " select avg(arrdelay), max(arrdelay), avg(depdelay), max(depdelay), origin from STAGING_AIRLINE group by origin order by avg(arrdelay) desc"
  " select avg(arrdelay), uniquecarrier from STAGING_AIRLINE group by uniquecarrier"
  /*" select avg(arrdelay), uniquecarrier from STAGING_AIRLINE where uniquecarrier = 'UA' group by uniquecarrier"
  " select avg(arrdelay), uniquecarrier, year_, month_ from STAGING_AIRLINE group by uniquecarrier, year_, month_  order by avg(arrdelay) desc"*/
 // " select avg(depdelay) avgdelay, avg(arrdelay), uniquecarrier from STAGING_AIRLINE where uniquecarrier in ('UA', 'DL') group by uniquecarrier order by avgdelay desc"
  " select avg(depdelay) avgdelay, uniquecarrier from STAGING_AIRLINE group by uniquecarrier order by avgdelay desc"
 /* " select avg(depdelay) avgdelay, uniquecarrier from STAGING_AIRLINE where uniquecarrier in ('UA', 'DL') group by uniquecarrier order by avgdelay desc"
  " select avg(depdelay) avgdelay, uniquecarrier from STAGING_AIRLINE where uniquecarrier = 'UA' group by uniquecarrier order by avgdelay desc"
  " select avg(depdelay) delay, dest, month_ from STAGING_AIRLINE where month_ in (10, 11, 12, 1) group by month_, dest order by delay"*/
  " select avg(depdelay) from STAGING_AIRLINE where dayofweek in (1, 2)"
  /*" select avg(depdelay), origin from STAGING_AIRLINE group by origin order by avg(depdelay) limit 50"
  " select avg(depdelay), uniquecarrier from STAGING_AIRLINE group by uniquecarrier"
  " select avg(depdelay), uniquecarrier from STAGING_AIRLINE where dayofweek in (1, 2) group by uniquecarrier"*/
  /*" select avg(arrdelay), count(*) from STAGING_AIRLINE where uniquecarrier = 'UA'"*/
  " select avg(arrdelay) arrdelay, dest, month_ from STAGING_AIRLINE where month_ in (10, 11, 12, 1) group by month_, dest order by arrdelay"
 /* " select avg(arrdelay) arrdelay, month_ from STAGING_AIRLINE group by month_ order by arrdelay"*/
  /*" select count(*), avg(arrdelay), avg(depdelay), dayofweek from STAGING_AIRLINE group by dayofweek"
  " select count(*), avg(arrdelay), avg(depdelay), dayofweek, month_ from STAGING_AIRLINE group by month_, dayofweek order by month_, dayofweek "*/
  " select count(*) from (select avg(arrdelay), avg(depdelay) from STAGING_AIRLINE group by flightnum)t1"
/*  " select count(*) from STAGING_AIRLINE"*/
/*  " select count(*), uniquecarrier from STAGING_AIRLINE group by uniquecarrier"
  " select count(*), uniquecarrier, origin from STAGING_AIRLINE group by uniquecarrier, origin order by uniquecarrier, origin"
  " select count(*), uniquecarrier, year_ from STAGING_AIRLINE group by uniquecarrier, year_"*/
  " select count(*), uniquecarrier, year_, month_ from STAGING_AIRLINE group by year_, month_, uniquecarrier order by year_, month_, uniquecarrier desc"
  /*" select * from STAGING_AIRLINE limit 1"*/
  " select max(arrdelay), max(depdelay), uniquecarrier from STAGING_AIRLINE group by uniquecarrier"
  /*" select max(arrdelay), uniquecarrier from STAGING_AIRLINE group by uniquecarrier"
  " select sum(arrdelay + depdelay), count(*) from STAGING_AIRLINE group by uniquecarrier"*/
/*  " select sum(arrdelay + depdelay), count(*) from STAGING_AIRLINE group by uniquecarrier limit 10"
  " select sum(arrdelay + depdelay), count(*) from STAGING_AIRLINE where uniquecarrier = 'AA' group by uniquecarrier"
  " select sum(arrdelay + depdelay), count(*) from STAGING_AIRLINE where uniquecarrier = 'DL' group by uniquecarrier"
  " select sum(arrdelay + depdelay), count(*) from STAGING_AIRLINE where uniquecarrier = 'UA' group by uniquecarrier"*/
  " select sum(arrdelay + depdelay), count(*) from STAGING_AIRLINE where uniquecarrier = 'WN' group by uniquecarrier"
  " select avg(arrdelay), avg(depdelay) from STAGING_AIRLINE group by flightnum"
  /*" select avg(arrdelay), avg(depdelay), uniquecarrier from STAGING_AIRLINE group by uniquecarrier"
  " select avg(arrdelay), avg(depdelay), uniquecarrier, year_ from STAGING_AIRLINE group by uniquecarrier, year_"*/
  " SELECT CUSTOMER_ID,TOTAL_HELPFUL_VOTES FROM (SELECT CUSTOMER_ID,SUM(HELPFUL_VOTES) AS TOTAL_HELPFUL_VOTES FROM REVIEWS GROUP BY CUSTOMER_ID) ORDER BY TOTAL_HELPFUL_VOTES DESC LIMIT 10"
  " SELECT count(PRODUCT_ID) count, average_star_rating from (SELECT PRODUCT_ID,ceil(AVG(STAR_RATING)) AS average_star_rating FROM REVIEWS GROUP BY PRODUCT_ID) GROUP BY average_star_rating ORDER BY average_star_rating DESC"
  " SELECT * FROM (SELECT CUSTOMER_ID,YEAR,TOTAL_HELPFUL_VOTES,DENSE_RANK() OVER (PARTITION BY YEAR ORDER BY TOTAL_HELPFUL_VOTES DESC ) AS RANK FROM (SELECT CUSTOMER_ID,YEAR,SUM(HELPFUL_VOTES) AS TOTAL_HELPFUL_VOTES FROM REVIEWS GROUP BY CUSTOMER_ID, YEAR)) WHERE RANK <= 10"
  " SELECT MARKETPLACE, average_star_rating, COUNT(PRODUCT_ID) COUNT FROM (SELECT PRODUCT_ID, MARKETPLACE, CEIL(AVG(STAR_RATING)) AS average_star_rating FROM REVIEWS GROUP BY PRODUCT_ID, MARKETPLACE) GROUP BY MARKETPLACE, average_star_rating ORDER BY MARKETPLACE, average_star_rating DESC"
  " SELECT v1.product_id,v1.review_count AS total_reviews ,v2.review_count AS review_gt_3_star FROM review_count_GT_10 v1 JOIN REVIEW_count_rating_GT_3 v2 ON v1.PRODUCT_ID = v2.PRODUCT_ID WHERE v2.review_count / v1.review_count > 0.8"
  " SELECT V1.REVIEW_NUMBER,V1.STAR_RATING,V1.COUNT * 100 / V2.COUNT AS PROPORTION FROM COUNT_PER_RATING_REVIEW_NUMBER V1 JOIN COUNT_PER_REVIEW_NUMBER V2 ON V1.REVIEW_NUMBER = V2.REVIEW_NUMBER ORDER BY REVIEW_NUMBER, STAR_RATING DESC"
  " SELECT * FROM votes_by_category_and_marketplace v1 JOIN total_customers_per_marketplace_and_category v2 ON v1.marketplace = v2.marketplace AND v1.product_category = v2.product_category WHERE v1.RANK < v2.count * 0.1"
  " SELECT MARKETPLACE,YEAR,STAR_RATING,AVG(STAR_RATING) OVER(PARTITION BY MARKETPLACE ORDER BY YEAR ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_average_STAR_RATING FROM (SELECT MARKETPLACE,YEAR,AVG(STAR_RATING) AS STAR_RATING FROM REVIEWS GROUP BY MARKETPLACE,YEAR)"
  " SELECT * FROM product_ratings pr WHERE avg_rating > (SELECT avg FROM (SELECT avg(star_rating) avg, marketplace FROM REVIEWS GROUP BY marketplace) WHERE marketplace = pr.marketplace)"
  " SELECT * FROM (SELECT CUSTOMER_ID, MARKETPLACE,	avg_review_length, RANK() OVER (PARTITION BY MARKETPLACE ORDER BY avg_review_length desc) AS rank FROM (SELECT	CUSTOMER_ID, MARKETPLACE, AVG(LENGTH(REVIEW_BODY)) AS avg_review_length FROM	REVIEWS	GROUP BY MARKETPLACE, CUSTOMER_ID)) WHERE RANK <= 10"
  " SELECT	product_category, MONTH(REVIEW_DATE), COUNT(*) AS review_COUNT FROM reviews GROUP BY MONTH(REVIEW_DATE), product_category ORDER BY product_category, review_COUNT desc"
  " SELECT	PRODUCT_ID, AVG(STAR_RATING) AS avg_rating FROM	(SELECT	PRODUCT_ID, STAR_RATING	FROM REVIEWS WHERE UPPER(VERIFIED_PURCHASE) = 'Y') GROUP BY PRODUCT_ID"
  " SELECT	* FROM (SELECT word, COUNT, RANK() OVER (ORDER BY COUNT DESC) AS RANK FROM (SELECT word, COUNT(*) AS COUNT FROM	(SELECT explode( split( REVIEW_BODY,'\s+' )) AS word FROM REVIEWS ) GROUP BY word)) WHERE RANK <= 20"
  " SELECT * FROM (SELECT product_id, ratio, total_reviews, RANK() OVER (ORDER BY total_reviews) AS RANK FROM (SELECT v1.product_id, v2.review_count / v1.review_count AS ratio, v1.review_count AS total_reviews FROM products_with_votes_lt_50 v1 JOIN rating_gt_3_count v2 ON v1.product_id = v2.product_id) WHERE ratio > 0.8 ) WHERE RANK <= 10"
  " SELECT * FROM (SELECT PRODUCT_ID, avg_star_rating, avg_review_length, RANK() OVER (ORDER BY avg_star_rating DESC) AS RANK FROM (SELECT product_id, AVG(STAR_RATING) AS avg_star_rating, AVG(SIZE( split(REVIEW_BODY, '\s+'))) AS avg_review_length FROM REVIEWS GROUP BY PRODUCT_ID) WHERE avg_star_rating > 3 AND avg_review_length < 50) WHERE RANK <= 10"
  " SELECT CUSTOMER_ID FROM (SELECT CUSTOMER_ID, COUNT(*) COUNT FROM (SELECT * FROM REVIEWS WHERE UPPER(VERIFIED_PURCHASE) <> 'Y' ) GROUP BY CUSTOMER_ID) WHERE COUNT = 0"
  " SELECT MONTH(REVIEW_DATE) AS review_MONTH, YEAR(REVIEW_DATE) AS review_YEAR , SUM(HELPFUL_VOTES)/ COUNT(*) AS HELPFUL_VOTES_to_submitted_reviews_ratio FROM REVIEWS GROUP BY MONTH(REVIEW_DATE), YEAR(REVIEW_DATE)"
  " SELECT * FROM (SELECT PRODUCT_PARENT, AVG(STAR_RATING) AS avg_rating, COUNT(DISTINCT(PRODUCT_ID)) AS no_of_products FROM REVIEWS GROUP BY PRODUCT_PARENT) WHERE no_of_products > 5 AND avg_rating < 2"
  " SELECT count(distinct product_id) from Reviews"
  " SELECT product_title from reviews limit 10"
  " SELECT distinct year from reviews order by year"
  " SELECT count(*), year from reviews group by year order by count(*) desc"
  " SELECT count(*), spark_partition_id() from reviews group by spark_partition_id() order by count(*) desc"
  " SELECT distinct customer_id from reviews"
  " SELECT count(distinct customer_id) from reviews"
  " SELECT customer_id, count(*)  from reviews where review_body is not null group by customer_id"
  " SELECT customer_id, count(*)  from reviews where review_body is not null group by customer_id order by count(*) desc limit 20"
  " SELECT product_title from reviews where product_title like '%iPhone%' limit 20"
  /*" SELECT CUSTOMER_ID,TOTAL_HELPFUL_VOTES FROM (SELECT CUSTOMER_ID,SUM(HELPFUL_VOTES) AS TOTAL_HELPFUL_VOTES FROM STAGING_REVIEWS GROUP BY CUSTOMER_ID) ORDER BY TOTAL_HELPFUL_VOTES DESC LIMIT 10"*/
  " SELECT count(PRODUCT_ID) count, average_star_rating from (SELECT PRODUCT_ID,ceil(AVG(STAR_RATING)) AS average_star_rating FROM STAGING_REVIEWS GROUP BY PRODUCT_ID) GROUP BY average_star_rating ORDER BY average_star_rating DESC"
  " SELECT * FROM (SELECT CUSTOMER_ID,YEAR,TOTAL_HELPFUL_VOTES,DENSE_RANK() OVER (PARTITION BY YEAR ORDER BY TOTAL_HELPFUL_VOTES DESC ) AS RANK FROM (SELECT CUSTOMER_ID,YEAR,SUM(HELPFUL_VOTES) AS TOTAL_HELPFUL_VOTES FROM STAGING_REVIEWS GROUP BY CUSTOMER_ID, YEAR)) WHERE RANK <= 10"
  /*" SELECT MARKETPLACE, average_star_rating, COUNT(PRODUCT_ID) COUNT FROM (SELECT PRODUCT_ID, MARKETPLACE, CEIL(AVG(STAR_RATING)) AS average_star_rating FROM STAGING_REVIEWS GROUP BY PRODUCT_ID, MARKETPLACE) GROUP BY MARKETPLACE, average_star_rating ORDER BY MARKETPLACE, average_star_rating DESC"
  " SELECT STAR_RATING, PS.PRODUCT_CATEGORY, COUNT_PER_RATING AS COUNT,COUNT_PER_RATING * 100 / COUNT AS SHARE_IN_PERCENTAGE FROM (SELECT COUNT(*) AS COUNT_PER_RATING,STAR_RATING, product_category FROM STAGING_REVIEWS GROUP BY product_category,STAR_RATING) AS PS JOIN (SELECT COUNT(*) COUNT,product_category FROM STAGING_REVIEWS GROUP BY product_category) P ON p.product_category = ps.product_category ORDER BY PS.PRODUCT_CATEGORY,STAR_RATING DESC"*/
  " SELECT v1.product_id,v1.review_count AS total_STAGING_REVIEWS ,v2.review_count AS review_gt_3_star FROM review_count_GT_10 v1 JOIN REVIEW_count_rating_GT_3 v2 ON v1.PRODUCT_ID = v2.PRODUCT_ID WHERE v2.review_count / v1.review_count > 0.8"
 /* " SELECT V1.REVIEW_NUMBER,V1.STAR_RATING,V1.COUNT * 100 / V2.COUNT AS PROPORTION FROM COUNT_PER_RATING_REVIEW_NUMBER V1 JOIN COUNT_PER_REVIEW_NUMBER V2 ON V1.REVIEW_NUMBER = V2.REVIEW_NUMBER ORDER BY REVIEW_NUMBER, STAR_RATING DESC"*/
  " SELECT * FROM votes_by_category_and_marketplace v1 JOIN total_customers_per_marketplace_and_category v2 ON v1.marketplace = v2.marketplace AND v1.product_category = v2.product_category WHERE v1.RANK < v2.count * 0.1"
  /*" SELECT MARKETPLACE,YEAR,STAR_RATING,AVG(STAR_RATING) OVER(PARTITION BY MARKETPLACE ORDER BY YEAR ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS moving_average_STAR_RATING FROM (SELECT MARKETPLACE,YEAR,AVG(STAR_RATING) AS STAR_RATING FROM STAGING_REVIEWS GROUP BY MARKETPLACE,YEAR)"*/
  /*" SELECT * FROM product_ratings pr WHERE avg_rating > (SELECT avg FROM (SELECT avg(star_rating) avg, marketplace FROM STAGING_REVIEWS GROUP BY marketplace) WHERE marketplace = pr.marketplace)"*/
  " SELECT * FROM (SELECT CUSTOMER_ID, MARKETPLACE,	avg_review_length, RANK() OVER (PARTITION BY MARKETPLACE ORDER BY avg_review_length desc) AS rank FROM (SELECT	CUSTOMER_ID, MARKETPLACE, AVG(LENGTH(REVIEW_BODY)) AS avg_review_length FROM	STAGING_REVIEWS	GROUP BY MARKETPLACE, CUSTOMER_ID)) WHERE RANK <= 10"
  " SELECT	product_category, MONTH(REVIEW_DATE), COUNT(*) AS review_COUNT FROM STAGING_REVIEWS GROUP BY MONTH(REVIEW_DATE), product_category ORDER BY product_category, review_COUNT desc"
  /*" SELECT	PRODUCT_ID, AVG(STAR_RATING) AS avg_rating FROM	(SELECT	PRODUCT_ID, STAR_RATING	FROM STAGING_REVIEWS WHERE UPPER(VERIFIED_PURCHASE) = 'Y') GROUP BY PRODUCT_ID"
  " SELECT	* FROM (SELECT word, COUNT, RANK() OVER (ORDER BY COUNT DESC) AS RANK FROM (SELECT word, COUNT(*) AS COUNT FROM	(SELECT explode( split( REVIEW_BODY,'\s+' )) AS word FROM STAGING_REVIEWS ) GROUP BY word)) WHERE RANK <= 20"
  " SELECT * FROM (SELECT product_id, ratio, total_STAGING_REVIEWS, RANK() OVER (ORDER BY total_STAGING_REVIEWS) AS RANK FROM (SELECT v1.product_id, v2.review_count / v1.review_count AS ratio, v1.review_count AS total_STAGING_REVIEWS FROM products_with_votes_lt_50 v1 JOIN rating_gt_3_count v2 ON v1.product_id = v2.product_id) WHERE ratio > 0.8 ) WHERE RANK <= 10"*/
  " SELECT * FROM (SELECT PRODUCT_ID, avg_star_rating, avg_review_length, RANK() OVER (ORDER BY avg_star_rating DESC) AS RANK FROM (SELECT product_id, AVG(STAR_RATING) AS avg_star_rating, AVG(SIZE( split(REVIEW_BODY, '\s+'))) AS avg_review_length FROM STAGING_REVIEWS GROUP BY PRODUCT_ID) WHERE avg_star_rating > 3 AND avg_review_length < 50) WHERE RANK <= 10"
  /*" SELECT CUSTOMER_ID FROM (SELECT CUSTOMER_ID, COUNT(*) COUNT FROM (SELECT * FROM STAGING_REVIEWS WHERE UPPER(VERIFIED_PURCHASE) <> 'Y' ) GROUP BY CUSTOMER_ID) WHERE COUNT = 0"*/
  /*" SELECT MONTH(REVIEW_DATE) AS review_MONTH, YEAR(REVIEW_DATE) AS review_YEAR , SUM(HELPFUL_VOTES)/ COUNT(*) AS HELPFUL_VOTES_to_submitted_STAGING_REVIEWS_ratio FROM STAGING_REVIEWS GROUP BY MONTH(REVIEW_DATE), YEAR(REVIEW_DATE)"
  " SELECT * FROM (SELECT PRODUCT_PARENT, AVG(STAR_RATING) AS avg_rating, COUNT(DISTINCT(PRODUCT_ID)) AS no_of_products FROM STAGING_REVIEWS GROUP BY PRODUCT_PARENT) WHERE no_of_products > 5 AND avg_rating < 2"*/
  /*" SELECT count(distinct product_id) from STAGING_REVIEWS"
  " SELECT product_title from STAGING_REVIEWS limit 10"
  " SELECT distinct year from STAGING_REVIEWS order by year"*/
  /*" SELECT count(*), year from STAGING_REVIEWS group by year order by count(*) desc"*/
  " SELECT count(*), spark_partition_id() from STAGING_REVIEWS group by spark_part/ition_id() order by count(*) desc"
  /*" SELECT distinct customer_id from STAGING_REVIEWS"*/
  " SELECT count(distinct customer_id) from STAGING_REVIEWS"
  " SELECT customer_id, count(*)  from STAGING_REVIEWS where review_body is not null group by customer_id"
  /*" SELECT customer_id, count(*)  from STAGING_REVIEWS where review_body is not null group by customer_id order by count(*) desc limit 20"*/
  /*" SELECT product_title from STAGING_REVIEWS where product_title like '%iPhone%' limit 20"*/
  /*" SELECT first,last,address,zip, total,rank() over (partition by zip order by total desc) as rnk from (SELECT patient, sum(cost) as total from (SELECT patient, cost from encounters  union  SELECT patient, cost from immunizations union SELECT patient, cost from medications   union  SELECT patient, cost from procedures) a group by patient) b  join patients p on id = patient having rnk < 11 order by zip,total desc",
  " SELECT * FROM (SELECT concat(p.first, ' ', p.LAST) AS name,p.address,m.total_cost AS expenses, p.zip,RANK() OVER (PARTITION BY zip ORDER BY m.total_cost DESC ) AS RANK  FROM patients p JOIN (SELECT patient, SUM(cost) AS total_cost  FROM ((SELECT patient, totalcost AS cost  FROM medications)  UNION ALL (SELECT patient, cost FROM immunizations )  UNION ALL (SELECT patient,cost FROM encounters)  UNION ALL (SELECT patient, cost FROM procedures))  GROUP BY patient ) m ON p.id = m.patient )  WHERE RANK <= 10",
  " SELECT * from (SELECT rank() over (order by tot_expense desc) as rank,  tot_expense,address,patname,zip from (SELECT sum(expense) tot_expense, address,patname,zip from (SELECT sum(enc.cost) as expense,pat.address,  concat(pat.first,' ',pat.last) as PatName,pat.zip from patients as pat inner join encounters as enc on pat.id =  enc.patient   group by pat.first,pat.last,pat.address,pat.zip union SELECT sum(proc.cost) as expense,pat.address, concat(pat.first,' ',pat.last) as PatName, pat.zip from patients as pat inner join procedures as proc on pat.id =  proc.patient  group by pat.first,pat.last,pat.address,pat.zip union  SELECT sum(imm.cost) as expense,pat.address, concat(pat.first,' ',pat.last) as PatName, pat.zip from patients as pat inner join immunizations as imm on pat.id =  imm.patient   group by pat.first,pat.last,pat.address,pat.zip union SELECT sum(med.cost) as expense,pat.address, concat(pat.first,' ',pat.last) as PatName, pat.zip from patients as pat  inner join medications as med on pat.id =  med.patient   group by pat.first,pat.last,pat.address,pat.zip)  group by address,patname,zip)) as res where rank <=10",
  " SELECT zip, id, tot_cost, rank  from (SELECT zip, id, tot_cost, dense_rank() over (partition by zip order by tot_cost desc) as rank  from (SELECT p.id, p.zip, (e.cost + i.cost + m.cost + pr.cost) tot_cost  from patients p, encounters e, immunizations i, medications m, procedures pr  where p.id = e.patient and e.patient = i.patient and i.patient = m.patient  and m.patient = pr.patient and e.id = i.encounter and e.id = m.encounter and e.id = pr.encounter))  where rank < 11",
  " SELECT * FROM (SELECT PREFIX,FIRST,LAST,SUFFIX,MAIDEN,ZIP,EXPENSES,row_number()  over(partition by t1.ZIP order by EXPENSES desc) as RANK FROM PATIENTS t1 INNER JOIN (SELECT  p.patient, sum(p.cost) AS EXPENSES   FROM (SELECT COST, PATIENT FROM Immunizations   UNION  SELECT COST, PATIENT FROM Encounters   UNION   SELECT COST, PATIENT FROM Medications UNION SELECT COST, PATIENT FROM Procedures) as p  group by p.patient) ON t1.ID = p.PATIENT)  where RANK <= 10 ORDER BY ZIP",
  " With patients_with_totalcost as (SELECT p.*, (e.Ecost+i.Icost+m.Mcost+pr.Pcost) as TotalCost  from patients p inner join (SELECT patient, sum(cost) as Ecost  from Encounters group by patient) e on p.ID = e.patient inner join (SELECT patient, sum(cost) as Icost  from Immunizations group by patient) i on p.ID = i.patient inner join (SELECT patient, sum(cost) as Mcost  from Medications group by patient) m on p.ID = m.patient inner join (SELECT patient, sum(cost) as Pcost  from Procedures group by patient) pr on p.ID = pr.patient), Topten as (SELECT *, RANK() over (PARTITION BY zip  order by totalcost desc) AS RowNo  FROM patients_with_totalcost)  SELECT zip,first,last,address,city,totalcost from Topten WHERE RowNo <= 10",
  " SELECT p.zip,p.FIRST,p.ADDRESS,m.TOTALCOST from PATIENTS p , medications m where p.ID = m.PATIENT  group by p.zip,p.FIRST,p.ADDRESS,m.totalcost order by m.totalcost desc limit 10",
  " SELECT DISTINCT sqei.ID,sqei.Zip,(TotalEICost+m.cost) as TotalEMICost  FROM Medications m JOIN (SELECT DISTINCT p.ID,p.Zip,(e.cost + i.cost ) as TotalEICost  FROM Patients p  JOIN Encounters e ON p.ID=e.Patient  JOIN Immunizations i ON i.Patient=p.ID) sqei ON sqei.ID=m.Patient  ORDER BY TotalEMICost DESC",
  " SELECT description, sum(cost) as total from medications group by description order by total desc limit 10",
  " SELECT * FROM (SELECT code, expenses, RANK() OVER ( ORDER BY expenses DESC ) AS RANK  FROM  (SELECT code, SUM(TOTALCOST) AS expenses   FROM medications   GROUP BY code )) WHERE RANK <= 10",
  " SELECT description,sum(cost) as totcost  from medications group by description  order by totcost  desc  limit 10",
  " SELECT code,sum(cost) as totcost  from medications group by code  order by totcost  desc  limit 10",
  " SELECT sum(TOTALCOST) , DESCRIPTION  from medications group by DESCRIPTION  order by 1 desc limit 10",
  " SELECT description, sum(totalcost) as total_cost, count(totalcost) as cnt  from medications group by description  order by total_cost desc limit 10",
  " SELECT CODE, DESCRIPTION, COUNT(CODE) AS SPEND_COUNT  FROM MEDICATIONS GROUP BY CODE, DESCRIPTION  ORDER BY SPEND_COUNT DESC LIMIT 10",
  " SELECT code, description, sum(cost) medcosts  from medications group by code, description  order by medcosts desc limit 10",
  " SELECT code, sum(cost) as total_cost  from medications group by code  order by sum(cost) desc limit 10",
  " SELECT CAST(sum(m.TOTALCOST) as int) as medicost,m.DESCRIPTION  from MEDICATIONS m ,PATIENTS p  where p.ID=m.PATIENT  group by m.DESCRIPTION order by medicost desc limit 10",
  " SELECT code,description, max(totalcost) costs  from medications  group by code,description order by costs desc limit 10",
  " SELECT DISTINCT code,description,sum(cost) OVER (PARTITION BY code) AS sum FROM Medications ORDER BY sum DESC LIMIT 10",
  " SELECT medi.description, max(medi.cost) amount  from patients p, medications medi  where p.id=medi.patient group by medi.description order by amount desc limit 10",
  " with disordered_ethnics as (SELECT p.ethnicity, count(1) as disordered from weak_hearts t join patients p on t.patient = p.id group by ethnicity) SELECT * from (SELECT * from disordered_ethnics order by disordered desc limit 10 union SELECT * from disordered_ethnics order by disordered asc  limit 5 ) order by disordered desc",
  " SELECT * FROM  ( (SELECT * FROM (SELECT ethnicity, COUNT,RANK() OVER (ORDER BY COUNT DESC)  AS RANK FROM depression_patients_group_by_ethnicity) WHERE RANK <= 10)  UNION ALL  (SELECT * FROM (SELECT ethnicity,COUNT,- RANK() OVER (ORDER BY COUNT ASC)  AS RANK FROM depression_patients_group_by_ethnicity ) WHERE RANK >= -5 ))  ORDER BY RANK",
  " SELECT * from (SELECT count(pat.id) numppl,ethnicity from encounters enc inner join patients pat on pat.id = enc.patient where REASONDESCRIPTION = 'Major depression disorder' group by ethnicity order by numppl desc limit 10 union SELECT count(pat.id) numppl,ethnicity from encounters enc inner join patients pat on pat.id = enc.patient where REASONDESCRIPTION = 'Major depression disorder' group by ethnicity order by numppl  limit 5 )  as res order by numppl desc",
  " SELECT ethnicity, cnt  from (SELECT * from hviewethgroup limit 10  union SELECT * from (SELECT * from hviewethgroup order by cnt) limit 5)  order by cnt desc",
  " SELECT Ethnicity,count(*) Count  from patients where id  in (SELECT PATIENT from conditions where DESCRIPTION like 'Major depression disorder' and DESCRIPTION not like 'Major depression  single episode') group by Ethnicity order by 2 desc limit 10 Union SELECT Ethnicity,count(*) Count from patients where id in (SELECT PATIENT from conditions where DESCRIPTION like  'Major depression disorder' and DESCRIPTION not like 'Major depression  single episode') group by Ethnicity order by 2 asc limit 5",
  " SELECT * FROM ((SELECT Ethnicity, COUNT(Ethnicity) AS Ethnicity_Count  FROM PATIENTS AS P INNER JOIN (SELECT PATIENT FROM CONDITIONS WHERE DESCRIPTION='Major depression disorder') AS C   ON  C.PATIENT=P.ID  GROUP BY  Ethnicity ORDER BY Ethnicity_Count DESC  LIMIT 10) UNION (SELECT Ethnicity, COUNT(Ethnicity) AS Ethnicity_Count FROM PATIENTS AS P INNER JOIN (SELECT PATIENT FROM CONDITIONS WHERE DESCRIPTION='Major depression disorder') AS C ON  C.PATIENT=P.ID GROUP BY  Ethnicity ORDER BY Ethnicity_Count ASC LIMIT 5)) AS V  ORDER BY V.Ethnicity_Count",
  " SELECT * from  (SELECT * from ETHNICITY_COUNT_WITH_MAJOR_DEPRESSION where ETHNICITY in (SELECT ETHNICITY from ETHNICITY_WITHOUT_SINGLE_EPISODE) order by count desc limit 10 union SELECT * from ETHNICITY_COUNT_WITH_MAJOR_DEPRESSION where ETHNICITY in (SELECT ETHNICITY from ETHNICITY_WITHOUT_SINGLE_EPISODE) order by count asc limit 5) order by count",
  " SELECT ethnicity,max FROM (SELECT p.ethnicity,count(p.ID) as max  FROM PATIENTS p,conditions c  where p.ID = c.PATIENT AND c.description='Major depression disorder'  group by p.ethnicity order by max desc limit 10 UNION SELECT p.ethnicity,count(p.ID)  as min FROM PATIENTS p,conditions c  where p.ID = c.PATIENT AND c.description='Major depression disorder'  group by p.ethnicity order by min asc limit 5)  group by ethnicity,max order by max desc",
  " SELECT COUNT(ethnicity) as PATIENT_COUNT, ethnicity  FROM patients  WHERE id IN (SELECT patient FROM conditions WHERE description = 'Major depression disorder')  GROUP BY ethnicity ORDER BY COUNT(ethnicity) DESC LIMIT 10)  UNION  (SELECT COUNT(ethnicity) as PATIENT_COUNT, ethnicity  FROM patients  WHERE id IN (SELECT patient FROM conditions WHERE description = 'Major depression disorder')  GROUP BY ethnicity ORDER BY COUNT(ethnicity) ASC LIMIT 5)",
  " SELECT * FROM  (SELECT * FROM (SELECT COUNT(1) as eth_count,p.Ethnicity  FROM Patients p JOIN Conditions c ON c.Patient=p.ID  WHERE c.description='Major depression disorder' AND c.Description<>'Major depression  single episode'  GROUP BY p.Ethnicity ORDER BY eth_count DESC LIMIT 10) a  union  SELECT * FROM (SELECT COUNT(1) as eth_count,p.Ethnicity  FROM Patients p  JOIN Conditions c ON c.Patient=p.ID  WHERE c.description='Major depression disorder' AND c.Description<>'Major depression  single episode'  GROUP BY p.Ethnicity ORDER BY eth_count ASC LIMIT 5) b) ORDER BY eth_count DESC",
  " SELECT p1.ethnicity from patients p1  join conditions c1 on p1.id=c1.patient  where c1.Description='Major depression disorder' AND c1.patient not in  (SELECT c1.patient from conditions c1 where c1.description='Major depression  single episode') group by ethnicity order by count(ID) desc limit 10",
  " SELECT count(1) as count, p.ethnicity from patients p, conditions condi  where p.id=condi.patient and condi.description='Major depression disorder'   group by ethnicity order by count desc limit 10  union  SELECT count(1) as count, p.ethnicity from patients p, conditions condi  where p.id=condi.patient and condi.description='Major depression disorder'   group by ethnicity order by count  limit 5",
  " SELECT ethnicity, patients_count FROM ((SELECT count(*) as patients_count, ethnicity from patients  where id in (SELECT patient from conditions   where description LIKE 'Major depression%' AND description <> 'Major depression  single episode')  group by ethnicity order by count(ethnicity) desc limit 10) union (SELECT count(*) as patients_count, ethnicity from patients  where id in (SELECT patient from conditions where description LIKE 'Major depression%' AND description <> 'Major depression  single episode') group by ethnicity order by count(ethnicity) asc limit 5)) ORDER BY patients_count desc",
  " SELECT * from (SELECT count(1) numberOfPatient, p.ethnicity, concat('1',' Top 10 ethnicities') as filter  from Patients p ,encounters e, careplans ca, conditions c where p.id=e.patient and p.id = ca.patient and p.id=c.patient and  e.reasoncode=370143000 and e.reasoncode<>36923009 and ca.reasoncode=370143000 and  ca.reasoncode<>36923009 and c.code = 370143000 and c.code<>36923009  group by p.ethnicity order by numberOfPatient desc limit 10  union SELECT count(1) numberOfPatient, p.ethnicity, concat('2',' Bottom 5 Ethnicities') as filter  from Patients p ,encounters e, careplans ca, conditions c  where p.id=e.patient and p.id = ca.patient and p.id=c.patient and e.reasoncode=370143000  and e.reasoncode<>36923009 and ca.reasoncode=370143000 and ca.reasoncode<>36923009 and  c.code = 370143000 and c.code<>36923009 group by p.ethnicity  order by numberOfPatient asc limit 5) order by filter asc,numberOfPatient desc",
  " SELECT description, count(1) as uses from medications where reasondescription = 'Hypertension' group by description order by uses desc",
  " SELECT code, description, SUM(dispenses) AS dispenses  FROM MEDICATIONS  WHERE reasondescription = 'Hypertension'  GROUP BY code, description ORDER BY DISPENSES LIMIT 10",
  " SELECT p.first, p.last from q join patients p on id = patient where id in (SELECT patient from weak_hearts)",
  " SELECT c.patient FROM (SELECT DISTINCT patient FROM CONDITIONS WHERE description = 'Major depression disorder') c LEFT JOIN (SELECT DISTINCT patient FROM careplans WHERE description  IN ('Anti-suicide psychotherapy','Psychiatry care plan','Major depressive disorder clinical management plan') AND reasondescription = 'Major depression disorder') cp  ON c.patient = cp.patient WHERE cp.patient IS NULL",
  " SELECT zip, sum(expired) as unshielded from patients p join (SELECT patient,  case when stop is null then 0 else 1 end as expired from careplans) c on c.patient = p.id  where deathdate is null group by zip order by unshielded desc limit 10",
  " SELECT * FROM (SELECT city, state , zip, RANK() OVER (ORDER BY COUNT DESC) AS RANK, COUNT FROM (SELECT COUNT(id) AS COUNT, zip, city, state FROM  (SELECT DISTINCT p.id, p.zip, p.city, p.STATE  FROM PATIENTS p LEFT JOIN (SELECT DISTINCT patient FROM careplans WHERE stop IS NULL) c ON p.id = c.patient WHERE c.patient IS NULL AND p.deathdate IS NULL ) GROUP BY zip, city, state ) )  WHERE RANK <= 10",
  " SELECT *, rank() over(partition by description order by allergics desc) as rnk from (SELECT zip, description, count(*) as allergics from patients p join allergies a on a.patient = p.id where description in ('Allergy to tree pollen', 'House dust mite allergy') group by zip, a.description) q  having rnk < 3 order by description, rnk",
  " SELECT * FROM (SELECT COUNT, description, zip, city, state, RANK()  OVER ( PARTITION BY description ORDER BY COUNT DESC) AS RANK  FROM (SELECT COUNT(patient) COUNT, description, zip, city, state  FROM (SELECT a.patient, a.description, p.zip, p.city, p.state  FROM allergies a JOIN patients p ON a.patient = p.id  WHERE a.description IN ('Allergy to tree pollen', 'House dust mite allergy') )  GROUP BY description, zip, city, state)) WHERE RANK <= 2",
  " SELECT *, dense_rank() over(distribute by age_group order by diseasecount desc) as rnk from (SELECT age_group, c.description, count(1) as diseasecount from conditions c join (SELECT  *, cast((year(coalesce(deathdate, current_timestamp())) - year(birthdate))/10 as int)  as age_group from patients) p on c.patient = p.id group by age_group, c.description) q having rnk < 4 order by age_group",
  " SELECT condition_code, description, CONCAT(age_range, '0 to ', age_range + 1, '0')  AS age_range, COUNT, RANK FROM  (SELECT code AS condition_code, description, age_range, COUNT,  RANK() OVER (PARTITION BY age_range ORDER BY COUNT DESC) AS RANK FROM (SELECT COUNT(*) COUNT, code, description, age_range FROM (SELECT p.id, c.description, c.code,  CEIL(datediff(COALESCE( p.DEATHDATE, CURRENT_DATE()), p.birthdate)/ 3650) AS age_range FROM conditions c JOIN patients p ON p.id = c.patient) WHERE age_range >= 0 GROUP BY code, description, age_range)) WHERE RANK <= 3",
  " with temp as (SELECT c.description as condition, avg(pr.cost) as procedure_avg_cost from patients p join conditions c on c.patient = p.id join procedures pr on p.id = pr.patient  group by c.description) SELECT *, procedure_avg_cost - avg_cost as deviation   from temp t cross join (SELECT avg(procedure_avg_cost) as avg_cost from temp) q",
  " SELECT condition_code, CONDITION, average_cost_per_condition, average_cost_per_condition - average_cost  FROM  (SELECT reasoncode AS condition_code,reasondescription AS CONDITION,   total_cost / patient_count AS average_cost_per_condition, (SELECT SUM(totalcost) AS total_cost FROM MEDICATIONS)/(SELECT COUNT(DISTINCT patient) AS patient_count  FROM conditions) AS average_cost  FROM (SELECT reasoncode, reasondescription, SUM(totalcost) AS total_cost  FROM medications  GROUP BY reasoncode, reasondescription) m  JOIN (SELECT code, COUNT (DISTINCT patient) AS  patient_count  FROM conditions GROUP BY code) c ON m.reasoncode = c.code)",
  " SELECT code,YEAR(start), DESCRIPTION, dispenses, AVG(dispenses) OVER (PARTITION BY code ORDER BY year(start) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM medications",
  " SELECT * FROM patient_expenses pe WHERE expenses > (SELECT AVG(expenses) FROM patient_expenses WHERE city = pe.city)",
  " SELECT code, dispenses, description FROM (SELECT SUM(dispenses) AS dispenses,code, description  FROM MEDICATIONS GROUP BY code, description) WHERE dispenses > (SELECT approx_percentile(dispenses, 1.0, 100) FROM medications)",*/
  " SELECT hour(Trip_Pickup_DateTime) as  hour ,avg(trip_distance/((unix_timestamp(Trip_Dropoff_DateTime)-unix_timestamp(Trip_Pickup_DateTime))/3600000000)) as AVG_SPEED from NYCTAXI where hour(Trip_Pickup_DateTime) BETWEEN 8 AND 10 group by Trip_Pickup_DateTime order by hour limit 100"
  " SELECT dayofmonth(Trip_Pickup_DateTime) dayofweek,cast(100*AVG(trip_distance/(unix_timestamp(Trip_Dropoff_DateTime)-unix_timestamp(Trip_Pickup_DateTime)/3600000000))/100 as int)  speed FROM NYCTAXI WHERE Fare_Amt/trip_distance BETWEEN 2 AND 10 AND HOUR(Trip_Pickup_DateTime) BETWEEN 8 AND 18 GROUP BY 1 ORDER BY 1"
  " SELECT dayofmonth(Trip_Pickup_DateTime) AS day, month(Trip_Pickup_DateTime) as month,SUM(Total_Amt) AS total_revenue FROM NYCTAXI GROUP BY dayofmonth(Trip_Pickup_DateTime),month(Trip_Pickup_DateTime)"
  " SELECT vendor_name, COUNT(*) AS number_of_trips FROM NYCTAXI GROUP BY vendor_name"
  " SELECT mm as weekofyear,sum(Total_Amt) as total_revenue from (SELECT weekofyear(Trip_Pickup_DateTime) as mm, Total_Amt from NYCTAXI) ff group by mm order by mm"
  " SELECT avg(trip_distance),vendor_name from NYCTAXI group by vendor_name"
  " SELECT sum(passenger_count) as passengerCnt_perTaxi ,vendor_name from NYCTAXI group by vendor_name"
  " SELECT vendor_name, COUNT(*) AS number_of_trips FROM NYCTAXI GROUP BY vendor_name"
  " SELECT tipped, COUNT(*) AS tip_freq FROM (SELECT CASE WHEN (tip_amt > 0) THEN 1 ELSE 0 END AS tipped, tip_amt FROM NYCTAXI WHERE Trip_Pickup_DateTime BETWEEN cast('2013-01-01' as timestamp) AND cast('2013-12-31' as timestamp)) tc GROUP BY tipped"
  " SELECT SUM(FLOAT(Total_Amt)) FROM  NYCTAXI WHERE cast(YEAR(TIMESTAMP(Trip_Pickup_DateTime)) as int ) = 2013 AND cast(MONTH(TIMESTAMP(Trip_Pickup_DateTime)) as int) = 12"
  " SELECT ROUND(Start_Lat, 4) as lat , ROUND(Start_Lon, 4) as lon, COUNT(*) as num_pickups FROM NYCTAXI  WHERE (Start_Lat BETWEEN 40.61 AND 40.91) AND (Start_Lon BETWEEN -74.06 AND -73.77) GROUP BY 1,2 ORDER BY num_pickups desc"
  " SELECT to_date(Trip_Dropoff_DateTime), COUNT(*) AS c  FROM NYCTAXI GROUP BY to_date(Trip_Dropoff_DateTime)"
  " SELECT hour(NYCTAXI.Trip_Pickup_DateTime),sum(Fare_Amt),sum(surcharge),sum(tip_amt) from NYCTAXI group by 1 order by sum(Fare_Amt) desc limit 30"
  " SELECT sum(passenger_count),sum(tip_amt),payment_type from NYCTAXI  group by payment_type order by 2 desc limit 30"
  " SELECT ROUND(SUM(FLOAT(Total_Amt)),2) AS amt,date_format(Trip_Pickup_DateTime,'dd.mm.yyyy'),vendor_name FROM NYCTAXI GROUP BY 2,vendor_name ORDER BY amt LIMIT 10 "
  " SELECT ROUND(Start_Lat, 4) as lat , ROUND(Start_Lon, 4) as lon, COUNT(*) as num_pickups ,ROUND(sum(Total_Amt),2) as totalAmt FROM NYCTAXI  WHERE (Start_Lat BETWEEN 40.61 AND 40.91) AND (Start_Lon BETWEEN -74.06 AND -73.77) GROUP BY 1,2 ORDER BY num_pickups desc"
  /*" SELECT hour(Trip_Pickup_DateTime) as  hour ,avg(trip_distance/((unix_timestamp(Trip_Dropoff_DateTime)-unix_timestamp(Trip_Pickup_DateTime))/3600000000)) as AVG_SPEED from STAGING_NYCTAXI where hour(Trip_Pickup_DateTime) BETWEEN 8 AND 10 group by Trip_Pickup_DateTime order by hour limit 100"
  " SELECT dayofmonth(Trip_Pickup_DateTime) dayofweek,cast(100*AVG(trip_distance/(unix_timestamp(Trip_Dropoff_DateTime)-unix_timestamp(Trip_Pickup_DateTime)/3600000000))/100 as int)  speed FROM STAGING_NYCTAXI WHERE Fare_Amt/trip_distance BETWEEN 2 AND 10 AND HOUR(Trip_Pickup_DateTime) BETWEEN 8 AND 18 GROUP BY 1 ORDER BY 1"*/
 /* " SELECT dayofmonth(Trip_Pickup_DateTime) AS day, month(Trip_Pickup_DateTime) as month,SUM(Total_Amt) AS total_revenue FROM STAGING_NYCTAXI GROUP BY dayofmonth(Trip_Pickup_DateTime),month(Trip_Pickup_DateTime)"*/
 /* " SELECT vendor_name, COUNT(*) AS number_of_trips FROM STAGING_NYCTAXI GROUP BY vendor_name"*/
  " SELECT mm as weekofyear,sum(Total_Amt) as total_revenue from (SELECT weekofyear(Trip_Pickup_DateTime) as mm, Total_Amt from STAGING_NYCTAXI) ff group by mm order by mm"
  " SELECT avg(trip_distance),vendor_name from STAGING_NYCTAXI group by vendor_name"
  " SELECT sum(passenger_count) as passengerCnt_perTaxi ,vendor_name from STAGING_NYCTAXI group by vendor_name"
  /*" SELECT vendor_name, COUNT(*) AS number_of_trips FROM STAGING_NYCTAXI GROUP BY vendor_name"*/
  " SELECT tipped, COUNT(*) AS tip_freq FROM (SELECT CASE WHEN (tip_amt > 0) THEN 1 ELSE 0 END AS tipped, tip_amt FROM STAGING_NYCTAXI WHERE Trip_Pickup_DateTime BETWEEN cast('2013-01-01' as timestamp) AND cast('2013-12-31' as timestamp)) tc GROUP BY tipped"
  " SELECT SUM(FLOAT(Total_Amt)) FROM  STAGING_NYCTAXI WHERE cast(YEAR(TIMESTAMP(Trip_Pickup_DateTime)) as int ) = 2013 AND cast(MONTH(TIMESTAMP(Trip_Pickup_DateTime)) as int) = 12"
  " SELECT ROUND(Start_Lat, 4) as lat , ROUND(Start_Lon, 4) as lon, COUNT(*) as num_pickups FROM STAGING_NYCTAXI  WHERE (Start_Lat BETWEEN 40.61 AND 40.91) AND (Start_Lon BETWEEN -74.06 AND -73.77) GROUP BY 1,2 ORDER BY num_pickups desc"
  " SELECT to_date(Trip_Dropoff_DateTime), COUNT(*) AS c  FROM STAGING_NYCTAXI GROUP BY to_date(Trip_Dropoff_DateTime)"
  /*" SELECT hour(STAGING_NYCTAXI.Trip_Pickup_DateTime),sum(Fare_Amt),sum(surcharge),sum(tip_amt) from STAGING_NYCTAXI group by 1 order by sum(Fare_Amt) desc limit 30"
  " SELECT sum(passenger_count),sum(tip_amt),payment_type from STAGING_NYCTAXI  group by payment_type order by 2 desc limit 30"
  " SELECT ROUND(SUM(FLOAT(Total_Amt)),2) AS amt,date_format(Trip_Pickup_DateTime,'dd.mm.yyyy'),vendor_name FROM STAGING_NYCTAXI GROUP BY 2,vendor_name ORDER BY amt LIMIT 10"
  " SELECT ROUND(Start_Lat, 4) as lat , ROUND(Start_Lon, 4) as lon, COUNT(*) as num_pickups ,ROUND(sum(Total_Amt),2) as totalAmt FROM STAGING_NYCTAXI  WHERE (Start_Lat BETWEEN 40.61 AND 40.91) AND (Start_Lon BETWEEN -74.06 AND -73.77) GROUP BY 1,2 ORDER BY num_pickups desc"*/

;