-- ####################################################################################################################################################################################################################################
-- Create Schema -> Array Type
CREATE SCHEMA ST;

-- Create Table
CREATE TABLE IF NOT EXISTS ST.Student(rollno Int, name String, marks ARRAY<Double>) USING column;

--Insert the values (data)
INSERT INTO ST.Student SELECT 1,'Mohit Shukla', Array(97.8,85.2,63.9,45.2,75.2,96.5);
INSERT INTO ST.Student SELECT 2,'Nalini Gupta',Array(89.3,56.3,89.1,78.4,84.1,99.2);
INSERT INTO ST.Student SELECT 3,'Kareena Kapoor',Array(99.9,25.3,45.8,65.8,77.9,23.1);
INSERT INTO ST.Student SELECT 4,'Salman Khan',Array(99.9,89.2,85.3,90.2,83.9,96.1);
INSERT INTO ST.Student SELECT 5,'Aranav Goswami',Array(90.1,80.1,70.1,60.1,50.1,40.1);
INSERT INTO ST.Student SELECT 6,'Sudhir Chudhari',Array(81.1,81.2,81.3,81.4,81.5,81.6);
INSERT INTO ST.Student SELECT 7,'Anjana Kashyap',Array(71.2,65.0,52.3,89.4,95.1,90.9);
INSERT INTO ST.Student SELECT 8,'Navika Kumar',Array(95.5,75.5,55.5,29.3,27.4,50.9);
INSERT INTO ST.Student SELECT 9,'Atul Singh',Array(40.1,42.3,46.9,47.8,44.4,42.0);
INSERT INTO ST.Student SELECT 10,'Dheeraj Sen',Array(62.1,50.7,52.3,67.9,69.9,66.8);

-- Create Views
CREATE TEMPORARY VIEW StudentMark AS SELECT rollno,name,explode(marks) AS Marks FROM ST.Student;

-- Queries
SELECT * FROM ST.Student ORDER BY rollno;
SELECT rollno, marks[0] AS Maths, marks[1] AS Science, marks[2] AS English, marks[3] AS Computer, marks[4] AS Music, marks[5] FROM ST.Student WHERE name = 'Salman Khan';
SELECT rollno, name, explode(marks) as Marks FROM ST.Student;
SELECT name,SUM(Marks) AS Total FROM StudentMark GROUP BY name ORDER BY Total DESC;
SELECT name,MAX(Marks),MIN(Marks) FROM StudentMark GROUP BY name;

-- Clean up -> Array Type
DROP TABLE IF EXISTS ST.Student;
DROP VIEW IF EXISTS StudentMark;
DROP SCHEMA ST;

-- ####################################################################################################################################################################################################################################

-- Create Schema -> Map Type
CREATE SCHEMA ST;

-- Create Table
CREATE TABLE IF NOT EXISTS ST.StudentMarksRecord (rollno Integer, name String,Maths MAP<STRING,DOUBLE>,Science MAP<STRING,DOUBLE>, English MAP<STRING,DOUBLE>, Computer MAP<STRING,DOUBLE>, Music MAP<STRING,Double>, History MAP<STRING,DOUBLE>) USING column;

--Insert the values (data)
INSERT INTO ST.StudentMarksRecord SELECT 1,'Mohit Shukla',MAP('maths',97.8),MAP('science',85.2), MAP('english',63.9),MAP('computer',45.2),MAP('music',75.2),MAP('history',96.5);
INSERT INTO ST.StudentMarksRecord SELECT 2,'Nalini Gupta',MAP('maths',89.3),MAP('science',56.3), MAP('english',89.1),MAP('computer',78.4),MAP('music',84.1),MAP('history',99.2);
INSERT INTO ST.StudentMarksRecord SELECT 3,'Kareena Kapoor',MAP('maths',99.9),MAP('science',25.3), MAP('english',45.8),MAP('computer',65.8),MAP('music',77.9),MAP('history',23.1);
INSERT INTO ST.StudentMarksRecord SELECT 4,'Salman Khan',MAP('maths',99.9),MAP('science',89.2), MAP('english',85.3),MAP('computer',90.2),MAP('music',83.9),MAP('history',96.1);
INSERT INTO ST.StudentMarksRecord SELECT 5,'Aranav Goswami',MAP('maths',90.1),MAP('science',80.1), MAP('english',70.1),MAP('computer',60.1),MAP('music',50.1),MAP('history',40.1);
INSERT INTO ST.StudentMarksRecord SELECT 6,'Sudhir Chudhari',MAP('maths',81.1),MAP('science',81.2), MAP('english',81.3),MAP('computer',81.4),MAP('music',81.5),MAP('history',81.6);
INSERT INTO ST.StudentMarksRecord SELECT 7,'Anjana Kashyap',MAP('maths',71.2),MAP('science',65.0), MAP('english',52.3),MAP('computer',89.4),MAP('music',95.1),MAP('history',90.9);
INSERT INTO ST.StudentMarksRecord SELECT 8,'Navika Kumar',MAP('maths',95.5),MAP('science',75.5), MAP('english',55.5),MAP('computer',29.3),MAP('music',27.4),MAP('history',50.9);
INSERT INTO ST.StudentMarksRecord SELECT 9,'Atul Singh',MAP('maths',40.1),MAP('science',42.3), MAP('english',46.9),MAP('computer',47.8),MAP('music',44.4),MAP('history',42.0);
INSERT INTO ST.StudentMarksRecord SELECT 10,'Dheeraj Sen',MAP('maths',62.1),MAP('science',50.7), MAP('english',52.3),MAP('computer',67.9),MAP('music',69.9),MAP('history',66.8);

-- Queries
SELECT * FROM ST.StudentMarksRecord ORDER BY rollno;
SELECT rollno, name,Maths['maths'],Science['science'] AS SCI ,English['english'] AS ENG,Computer['computer'],Music['music'],History['history'] FROM ST.StudentMarksRecord WHERE name = 'Salman Khan';
SELECT name, SUM(Maths['maths'] + Science['science'] + English['english'] + Computer['computer'] + Music['music'] + History['history']) AS Total FROM ST.StudentMarksRecord GROUP BY name ORDER BY Total DESC;
SELECT name,SUM(Maths['maths'] + Science['science'] + English['english'] + Computer['computer'] + Music['music'] + History['history']) AS Total, CASE WHEN SUM(Maths['maths'] + Science['science'] + English['english'] + Computer['computer'] + Music['music'] + History['history']) >= 500 THEN 'A' WHEN SUM(Maths['maths'] + Science['science'] + English['english'] + Computer['computer'] + Music['music'] + History['history']) >=400 THEN 'B' WHEN SUM(Maths['maths'] + Science['science'] +    English['english'] + Computer['computer'] + Music['music'] + History['history']) >= 300 THEN 'C' ELSE 'FAIL' END AS Grade FROM ST.StudentMarksRecord GROUP BY name ORDER BY Total DESC;
SELECT name,(SUM(Maths['maths'] + Science['science'] + English['english'] + Computer['computer'] + Music['music'] + History['history'])*100.0/600.0) AS Percentage FROM ST.StudentMarksRecord GROUP BY name ORDER BY Percentage DESC;
SELECT name, MAX(marks) AS Max, MIN(marks) AS Min FROM (SELECT name, Maths['maths'] AS marks FROM ST.StudentMarksRecord UNION ALL SELECT name, Science['science'] AS marks FROM ST.StudentMarksRecord UNION ALL SELECT name, English['english'] AS marks FROM ST.StudentMarksRecord UNION ALL SELECT name, Computer['computer'] AS marks FROM ST.StudentMarksRecord UNION ALL SELECT name,Music['music'] AS marks FROM ST.StudentMarksRecord UNION ALL SELECT name,History['history'] AS marks FROM ST.StudentMarksRecord) GROUP BY name;

-- Clean up -> Map Type
DROP TABLE IF EXISTS ST.StudentMarksRecord;
DROP SCHEMA IF EXISTS ST;
-- ####################################################################################################################################################################################################################################

-- Create Schema -> Struct Type
CREATE SCHEMA CR;

-- Create Table
CREATE TABLE IF NOT EXISTS CR.CricketRecord(name String,TestRecord STRUCT<batStyle:String,Matches:Long,Runs:Int,Avg:Double>) USING column;

--Insert the values (data)
INSERT INTO CR.CricketRecord SELECT 'Sachin Tendulkar',STRUCT('Right Hand',200,15921,53.79);
INSERT INTO CR.CricketRecord SELECT 'Saurav Ganguly',STRUCT('Left Hand',113,7212,51.26);
INSERT INTO CR.CricketRecord SELECT 'Rahul Drvaid',STRUCT('Right Hand',164,13288,52.31);
INSERT INTO CR.CricketRecord SELECT 'Yuvraj Singh',STRUCT('Left Hand',40,1900,33.93);
INSERT INTO CR.CricketRecord SELECT 'MahendraSingh Dhoni',STRUCT('Right Hand',90,4876,38.09);
INSERT INTO CR.CricketRecord SELECT 'Kapil Dev',STRUCT('Right Hand',131,5248,31.05);
INSERT INTO CR.CricketRecord SELECT 'Zahir Khan',STRUCT('Right Hand',92,1230,11.94);
INSERT INTO CR.CricketRecord SELECT 'Gautam Gambhir',STRUCT('Left Hand',58,4154,41.96);
INSERT INTO CR.CricketRecord SELECT 'VVS Laxman',STRUCT('Right Hand',134,8781,45.5);
INSERT INTO CR.CricketRecord SELECT 'Virendra Sehwag',STRUCT('Right Hand',104,8586,49.34);
INSERT INTO CR.CricketRecord SELECT 'Sunil Gavaskar',STRUCT('Right Hand',125,10122,51.12);
INSERT INTO CR.CricketRecord SELECT 'Anil Kumble',STRUCT('Right Hand',132,2506,17.65);

-- Queries
SELECT name, TestRecord.Runs, TestRecord.Avg FROM CR.CricketRecord ORDER BY TestRecord.Runs DESC;
SELECT SUM(TestRecord.Runs) AS TotalRuns FROM CR.CricketRecord;
SELECT name FROM CR.CricketRecord WHERE TestRecord.batStyle = 'Left Hand';
SELECT name, TestRecord.batStyle,TestRecord.Matches,TestRecord.Runs,TestRecord.Avg FROM CR.CricketRecord ORDER BY TestRecord.Matches DESC;

-- Clean up -> Struct Type
DROP TABLE IF EXISTS CR.CricketRecord;
DROP SCHEMA CR;
-- ####################################################################################################################################################################################################################################

-- Create Schema -> All types Mixed
CREATE SCHEMA T20;

-- Create Table
CREATE TABLE IF NOT EXISTS T20.TwentyTwenty(name String,LastThreeMatchPerformance ARRAY<Double>,Roll MAP<SMALLINT,STRING>,Profile STRUCT<Matches:Long,Runs:Int,SR:Double,isPlaying:Boolean>) USING column;

--Insert the values (data)
INSERT INTO T20.TwentyTwenty SELECT 'M S Dhoni',ARRAY(37,25,58),MAP(1,'WicketKeeper'),STRUCT(93,1487,127.09,true);
INSERT INTO T20.TwentyTwenty SELECT 'Yuvaraj Singh',ARRAY(68,72,21),MAP(2,'AllRounder'),STRUCT(58,1177,136.38,false);
INSERT INTO T20.TwentyTwenty SELECT 'Viral Kohli',ARRAY(52,102,23),MAP(3,'Batsmen'),STRUCT(65,2167,136.11,true);
INSERT INTO T20.TwentyTwenty SELECT 'Gautam Gambhir',ARRAY(35,48,74),MAP(3,'Batsmen'),STRUCT(37,932,119.02,false);
INSERT INTO T20.TwentyTwenty SELECT 'Rohit Sharma',ARRAY(0,56,44),MAP(3,'Batsmen'),STRUCT(90,2237,138.17,true);
INSERT INTO T20.TwentyTwenty SELECT 'Ravindra Jadeja',ARRAY(15,25,33),MAP(2,'AllRounder'),STRUCT(40,116,93.54,true);
INSERT INTO T20.TwentyTwenty SELECT 'Virendra Sehwag',ARRAY(5,45,39),MAP(3,'Batsmen'),STRUCT(19,394,145.39,false);
INSERT INTO T20.TwentyTwenty SELECT 'Hardik Pandya',ARRAY(27,14,19),MAP(2,'AllRounder'),STRUCT(35,271,153.10,true);
INSERT INTO T20.TwentyTwenty SELECT 'Suresh Raina',ARRAY(31,26,48),MAP(3,'Batsmen'),STRUCT(78,1605,134.87,false);
INSERT INTO T20.TwentyTwenty SELECT 'Harbhajan Singh',ARRAY(23,5,11),MAP(4,'Bowler'),STRUCT(28,108,124.13,false);
INSERT INTO T20.TwentyTwenty SELECT 'Ashish Nehra',ARRAY(2,1,5),MAP(4,'Bowler'),STRUCT(27,28,71.79,false);
INSERT INTO T20.TwentyTwenty SELECT 'Kuldeep Yadav',ARRAY(3,3,0),MAP(4,'Bowler'),STRUCT(17,20,100.0,true);
INSERT INTO T20.TwentyTwenty SELECT 'Parthiv Patel',ARRAY(29,18,9),MAP(1,'WicketKeeper'),STRUCT(2,36,112.50,false);
INSERT INTO T20.TwentyTwenty SELECT 'Ravichandran Ashwin',ARRAY(15,7,12),MAP(4,'Bowler'),STRUCT(46,123,106.95,true);
INSERT INTO T20.TwentyTwenty SELECT 'Irfan Pathan',ARRAY(17,23,18),MAP(2,'AllRounder'),STRUCT(24,172,119.44,false);

-- Queries
SELECT * FROM T20.TwentyTwenty ORDER BY name;
SELECT name, SUM(LastThreeMatchPerformance[0] + LastThreeMatchPerformance[1] + LastThreeMatchPerformance[2]) AS RunsScored FROM T20.TwentyTwenty WHERE Roll[1] = 'WicketKeeper' GROUP BY name;
SELECT name, LastThreeMatchPerformance[2] AS RunsScoredinLastMatch, Profile.Matches,Profile.SR,Profile.Runs FROM T20.TwentyTwenty WHERE Profile.Runs >= 1000 ORDER BY Profile.Runs DESC;
SELECT COUNT(*) AS AllRounder FROM T20.TwentyTwenty WHERE Roll[2] = 'AllRounder';
SELECT name, Profile.SR,Profile.Runs FROM T20.TwentyTwenty ORDER BY Profile.SR DESC;


-- Clean up -> All types Mixed

DROP TABLE IF EXISTS T20.TwentyTwenty;
DROP SCHEMA T20;

-- ####################################################################################################################################################################################################################################

-- Create Schema -> Arrays of Struct
CREATE SCHEMA TW;

-- Create Table
CREATE TABLE IF NOT EXISTS TW.TwoWheeler (brand String,BikeInfo ARRAY< STRUCT <type:String,cc:Double,noofgears:BigInt,instock:Boolean>>) USING column;

--Insert the values (data)
INSERT INTO TW.TwoWheeler SELECT 'Honda',ARRAY(STRUCT('Street Bike',149.1,5,false));
INSERT INTO TW.TwoWheeler SELECT 'TVS',ARRAY(STRUCT('Scooter',110,0,true));
INSERT INTO TW.TwoWheeler SELECT 'Honda',ARRAY(STRUCT('Scooter',109.19,0,true));
INSERT INTO TW.TwoWheeler SELECT 'Royal Enfield',ARRAY(STRUCT('Cruiser',346.0,5,true));
INSERT INTO TW.TwoWheeler SELECT 'Suzuki', ARRAY(STRUCT('Cruiser',154.9,5,true));
INSERT INTO TW.TwoWheeler SELECT 'Yamaha', ARRAY(STRUCT('Street Bike',149,5,false));
INSERT INTO TW.TwoWheeler SELECT 'Bajaj',ARRAY(STRUCT('Street Bike',220.0,5,true));
INSERT INTO TW.TwoWheeler SELECT 'Kawasaki',ARRAY(STRUCT('Sports Bike',296.0,5,false));
INSERT INTO TW.TwoWheeler SELECT 'Vespa',ARRAY(STRUCT('Scooter',125.0,0,true));
INSERT INTO TW.TwoWheeler SELECT 'Mahindra',ARRAY(STRUCT('Scooter',109.0,0,false));

-- Queries

SELECT * FROM TW.TwoWheeler;
SELECT brand FROM TW.TwoWheeler WHERE BikeInfo[0].type = 'Scooter';
SELECT brand,BikeInfo[0].cc FROM TW.TwoWheeler WHERE BikeInfo[0].cc >= 149.0 ORDER BY BikeInfo[0].cc DESC;
SELECT brand,COUNT(BikeInfo[0].type) FROM TW.TwoWheeler WHERE BikeInfo[0].type = 'Cruiser' GROUP BY brand;
SELECT brand, BikeInfo[0].type AS Style, BikeInfo[0].instock AS Available FROM TW.TwoWheeler;

-- Clean up -> Arrays of Struct
DROP TABLE IF EXISTS TW.TwoWheeler;
DROP SCHEMA TW;

-- ####################################################################################################################################################################################################################################

-- Create Schema -> Map(String,Array(String))
CREATE SCHEMA FP;

-- Create Table
CREATE TABLE IF NOT EXISTS FP.FamousPeople(country String,celebrities MAP<String,Array<String>>) USING column;

--Insert the values (data)
INSERT INTO FP.FamousPeople  SELECT 'United States', MAP('Presidents',ARRAY('George Washington','Abraham Lincoln','Thomas Jefferson', 'John F. Kennedy','Franklin D. Roosevelt'));
INSERT INTO FP.FamousPeople  SELECT 'India', MAP('Prime Ministers',ARRAY('Jawaharlal Nehru','Indira Gandhi', 'Lal Bahadur Shastri','Narendra Modi','PV Narsimha Rao'));
INSERT INTO FP.FamousPeople  SELECT 'India', MAP('Actors',ARRAY('Amithab Bachhan','Sanjeev Kumar','Dev Anand', 'Akshay Kumar','Shahrukh Khan','Salman Khan'));
INSERT INTO FP.FamousPeople  SELECT 'United States', MAP('Actors',ARRAY('Brad Pitt','Jim Carry','Bruce Willis', 'Tom Cruise','Michael Douglas','Dwayne Johnson'));
INSERT INTO FP.FamousPeople  SELECT 'India', MAP('Authors',ARRAY('Chetan Bhagat','Jay Vasavada','Amish Tripathi', 'Khushwant Singh','Premchand','Kalidas'));
INSERT INTO FP.FamousPeople  SELECT 'United States', MAP('Authors',ARRAY('Mark Twain','Walt Whitman','J.D. Salinger', 'Emily Dickinson','Willa Cather','William Faulkner'));
CREATE TEMPORARY VIEW FamousPeopleView AS  SELECT country, explode(celebrities) FROM FP.FamousPeople;

-- Queries
SELECT * FROM FamousPeopleView;
SELECT country,value[0],value[1],value[2],value[3],value[4] FROM FamousPeopleView WHERE key = 'Prime Ministers';
SELECT country,value FROM FamousPeopleView WHERE key = 'Authors';

-- Clean up -> Map(String,Array(String))
DROP TABLE IF EXISTS FP.FamousPeople;
DROP VIEW IF EXISTS FamousPeopleView;
DROP SCHEMA FP;
