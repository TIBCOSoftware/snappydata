/*
 * Copyright (c) 2018 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.hydra.spva

import org.apache.spark.sql.{DataFrame, SQLContext, SnappyContext}

object SPVAQueries {
  var snc: SnappyContext = _
  var dataFilesLocation: String = _
  // From each zipcode list down top 10 people whose medical expenses have been the highest.
  // List their names, address and expenses and grouped by zip code and rank them by expenses in descending order.
  // 3121 rows
  val Q1_1: String = "select first,last,address,zip, total,rank() over (partition by zip order by total desc) as rnk " +
      "from (select patient, sum(cost) as total from (select patient, cost from spd.encounters " +
      " union  select patient, cost from spd.immunizations union select patient, cost from spd.medications  " +
      " union  select patient, cost from spd.procedures) a group by patient) b " +
      " join spd.patients p on id = patient having rnk < 11 order by zip,total desc"

  // 3121 rows
  val Q1_2: String = "SELECT * FROM (SELECT concat(p.first, ' ', p.LAST) AS name,p.address,m.total_cost AS expenses," +
      " p.zip,RANK() OVER (PARTITION BY zip ORDER BY m.total_cost DESC ) AS RANK " +
      " FROM SPD.patients p JOIN (SELECT patient, SUM(cost) AS total_cost " +
      " FROM ((SELECT patient, totalcost AS cost " +
      " FROM SPD.medications) " +
      " UNION ALL (SELECT patient, cost FROM SPD.immunizations ) " +
      " UNION ALL (SELECT patient,cost FROM SPD.encounters) " +
      " UNION ALL (SELECT patient, cost FROM SPD.procedures)) " +
      " GROUP BY patient ) m ON p.id = m.patient ) " +
      " WHERE RANK <= 10 "

  // 10 rows
  val Q1_3: String = "select * from (select rank() over (order by tot_expense desc) as rank, " +
      " tot_expense,address,patname,zip from(select sum(expense) tot_expense," +
      " address,patname,zip from(select sum(enc.cost) as expense,pat.address, " +
      " concat(pat.first,' ',pat.last) as PatName,pat.zip from spd.patients as pat" +
      " inner join spd.encounters as enc on pat.id =  enc.patient  " +
      " group by pat.first,pat.last,pat.address,pat.zip" +
      " union " +
      " select sum(proc.cost) as expense,pat.address, concat(pat.first,' ',pat.last) as PatName," +
      " pat.zip from spd.patients as pat inner join spd.procedures as proc on pat.id =  proc.patient " +
      " group by pat.first,pat.last,pat.address,pat.zip" +
      " union " +
      " select sum(imm.cost) as expense,pat.address, concat(pat.first,' ',pat.last) as PatName," +
      " pat.zip from spd.patients as pat inner join spd.immunizations as imm on pat.id =  imm.patient  " +
      " group by pat.first,pat.last,pat.address,pat.zip" +
      " union " +
      " select sum(med.cost) as expense,pat.address, concat(pat.first,' ',pat.last) as PatName," +
      " pat.zip from spd.patients as pat " +
      " inner join spd.medications as med on pat.id =  med.patient  " +
      " group by pat.first,pat.last,pat.address,pat.zip) " +
      " group by address,patname,zip)) as res where rank <=10"

  // 3445 rows
  val Q1_4: String = "select zip, id, tot_cost, rank " +
      " from (select zip, id, tot_cost, dense_rank() over (partition by zip order by tot_cost desc) as rank " +
      " from (select p.id, p.zip, (e.cost + i.cost + m.cost + pr.cost) tot_cost " +
      " from spd.patients p, spd.encounters e, spd.immunizations i, spd.medications m, spd.procedures pr " +
      " where p.id = e.patient and e.patient = i.patient and i.patient = m.patient " +
      " and m.patient = pr.patient and e.id = i.encounter and e.id = m.encounter and e.id = pr.encounter)) " +
      " where rank < 11"

  // 3121 rows
  val Q1_5: String = "SELECT * FROM (SELECT PREFIX,FIRST,LAST,SUFFIX,MAIDEN,ZIP,EXPENSES,row_number() " +
      " over(partition by t1.ZIP order by EXPENSES desc) as RANK" +
      " FROM SPD.PATIENTS t1" +
      " INNER JOIN (SELECT  p.patient, sum(p.cost) AS EXPENSES  " +
      " FROM (SELECT COST, PATIENT FROM SPD.Immunizations  " +
      " UNION " +
      " SELECT COST, PATIENT FROM spd.Encounters  " +
      " UNION  " +
      " SELECT COST, PATIENT FROM spd.Medications" +
      " UNION" +
      " SELECT COST, PATIENT FROM spd.Procedures) as p " +
      " group by p.patient) ON t1.ID = p.PATIENT) " +
      " where RANK <= 10 ORDER BY ZIP"

  // 3166 rows
  val Q1_6: String = "With patients_with_totalcost as(select p.*, (e.Ecost+i.Icost+m.Mcost+pr.Pcost) as TotalCost " +
      " from spd.patients p" +
      " inner join (select patient, sum(cost) as Ecost " +
      " from spd.Encounters group by patient) e on p.ID = e.patient" +
      " inner join (select patient, sum(cost) as Icost " +
      " from spd.Immunizations group by patient) i on p.ID = i.patient" +
      " inner join (select patient, sum(cost) as Mcost " +
      " from spd.Medications group by patient) m on p.ID = m.patient" +
      " inner join (select patient, sum(cost) as Pcost " +
      " from spd.Procedures group by patient) pr on p.ID = pr.patient)," +
      " Topten as(SELECT *, RANK() over (PARTITION BY zip " +
      " order by totalcost desc) AS RowNo " +
      " FROM patients_with_totalcost) " +
      " select zip,first,last,address,city,totalcost from Topten WHERE RowNo <= 10"

  // 10 rows
  val Q1_7: String = "select p.zip,p.FIRST,p.ADDRESS,m.TOTALCOST from SPD.PATIENTS p ," +
      " SPD.medications m where p.ID = m.PATIENT " +
      " group by p.zip,p.FIRST,p.ADDRESS,m.totalcost order by m.totalcost desc limit 10"

  val Q1_8: String = "SELECT DISTINCT sqei.ID,sqei.Zip,(TotalEICost+m.cost) as TotalEMICost " +
      " FROM SPD.Medications m JOIN (select DISTINCT p.ID,p.Zip,(e.cost + i.cost ) as TotalEICost " +
      " FROM SPD.Patients p " +
      " JOIN SPD.Encounters e ON p.ID=e.Patient " +
      " JOIN SPD.Immunizations i ON i.Patient=p.ID) sqei ON sqei.ID=m.Patient " +
      " ORDER BY TotalEMICost DESC"

  val Q1_9: String = "select pa.first,pa.last, medi.cost " +
      " from spd.patients pa, spd.medications medi " +
      " where pa.id=medi.patient and pa.zip=1267 " +
      " order by medi.cost desc limit 10"

  // Top 10 medicines on which the entire population has spent the most.
  val Q2_1: String = "select description, sum(cost) as total from spd.medications group by description " +
      " order by total desc limit 10"

  val Q2_2: String = "SELECT * FROM (" +
      " SELECT code, expenses, RANK() OVER (" +
      " ORDER BY expenses DESC ) AS RANK " +
      " FROM " +
      " (SELECT code, SUM(TOTALCOST) AS expenses " +
      "  FROM SPD.medications " +
      "  GROUP BY code ))" +
      " WHERE RANK <= 10"

  val Q2_3: String = "select description,sum(cost) as totcost " +
      " from spd.medications group by description " +
      " order by totcost  desc  limit 10 "

  val Q2_4: String = "select code,sum(cost) as totcost " +
      " from spd.medications group by code " +
      " order by totcost  desc  limit 10 "

  val Q2_5: String = "select sum(TOTALCOST) , DESCRIPTION " +
      " from SPD.medications group by DESCRIPTION " +
      " order by 1 desc limit 10"

  val Q2_6: String = "select description, sum(totalcost) as total_cost, count(totalcost) as cnt " +
      " from spd.medications group by description " +
      " order by total_cost desc limit 10"

  val Q2_7: String = "SELECT CODE, DESCRIPTION, COUNT(CODE) AS SPEND_COUNT " +
      " FROM spd.MEDICATIONS GROUP BY CODE, DESCRIPTION " +
      " ORDER BY SPEND_COUNT DESC LIMIT 10"

  val Q2_8: String = "select code, description, sum(cost) medcosts " +
      " from spd.medications group by code, description " +
      " order by medcosts desc limit 10"

  val Q2_9: String = "select code, sum(cost) as total_cost " +
      " from spd.medications group by code " +
      " order by sum(cost) desc limit 10"

  val Q2_10: String = "SELECT CAST(sum(m.TOTALCOST) as int) as medicost,m.DESCRIPTION " +
      " from SPD.MEDICATIONS m ,SPD.PATIENTS p " +
      " where p.ID=m.PATIENT " +
      " group by m.DESCRIPTION order by medicost desc limit 10"

  val Q2_11: String = "select code,description, max(totalcost) costs " +
      " from spd.medications " +
      " group by code,description order by costs desc limit 10"

  val Q2_12: String = "SELECT DISTINCT code,description,sum(cost) OVER (PARTITION BY code) AS sum " +
      " FROM SPD.Medications ORDER BY sum DESC LIMIT 10"

  val Q2_13: String = "select medi.description, max(medi.cost) amount " +
      " from spd.patients p, spd.medications medi " +
      " where p.id=medi.patient group by medi.description order by amount desc limit 10"

  // Find out the top 10 and bottom 5 ethnicities which has maximum and minimum number of patients
  // with 'Major depression disorder' excluding 'Major depression  single episode'
  val Q3_1_1: String = "create table weak_hearts as " +
      " select patient from " +
      " (select patient, " +
      "       case when description = 'Major depression  single episode' then 1 else 0 end as attack_once," +
      "       case when description = 'Major depression disorder'        then 1 else 0 end as attacked " +
      " from spd.conditions) c " +
      " group by patient having sum(attack_once) = 0 and  sum(attacked) > 0"

  // 15 rows
  val Q3_1_2: String = "with disordered_ethnics as (select p.ethnicity, count(1) as disordered " +
      " from weak_hearts t " +
      " join spd.patients p on t.patient = p.id " +
      " group by ethnicity) " +
      " select * from (select * from disordered_ethnics order by disordered desc limit 10" +
      " union" +
      " select * from disordered_ethnics order by disordered asc  limit 5)" +
      " order by disordered desc"

  val Q3_2_1: String = " CREATE VIEW IF NOT EXISTS SPD.depression_patients_group_by_ethnicity " +
      " AS SELECT p.ethnicity,COUNT(1) " +
      " AS COUNT FROM SPD.conditions c " +
      " JOIN SPD.patients p ON p.id = c.patient " +
      " WHERE description LIKE '%Major depression disorder%'" +
      " GROUP BY p.ethnicity"

  val Q3_2_2: String = "SELECT * FROM " +
      " ( (SELECT * FROM (SELECT ethnicity, COUNT,RANK() OVER (ORDER BY COUNT DESC) " +
      " AS RANK FROM SPD.depression_patients_group_by_ethnicity) WHERE RANK <= 10) " +
      " UNION ALL " +
      " (SELECT * FROM (SELECT ethnicity,COUNT,- RANK() OVER (ORDER BY COUNT ASC) " +
      " AS RANK FROM SPD.depression_patients_group_by_ethnicity )" +
      " WHERE RANK >= -5 )) " +
      " ORDER BY RANK"

  val Q3_3: String = "select * from (select count(pat.id) numppl,ethnicity " +
      " from spd.encounters enc " +
      " inner join spd.patients pat on pat.id = enc.patient " +
      " where REASONDESCRIPTION = 'Major depression disorder' group by ethnicity order by numppl desc limit 10 " +
      " union " +
      " select count(pat.id) numppl,ethnicity " +
      " from spd.encounters enc " +
      " inner join spd.patients pat on pat.id = enc.patient " +
      " where REASONDESCRIPTION = 'Major depression disorder' group by ethnicity order by numppl  limit 5 ) " +
      " as res order by numppl desc"

  val Q3_4_1: String = "create view hview as select p.id, p.ETHNICITY " +
      " from spd.patients p, spd.conditions c " +
      " where p.id = c.patient and c.description = 'Hypertension'"

  val Q3_4_2: String = "create view hviewethgroup " +
      " as select count(*) cnt, ETHNICITY " +
      " from hview " +
      " group by ETHNICITY order by cnt desc"

  val Q3_4_3: String = "select ethnicity, cnt " +
      " from (select * from hviewethgroup limit 10 " +
      " union select * from (select * from hviewethgroup order by cnt) limit 5) " +
      " order by cnt desc"

  val Q3_5: String = "select Ethnicity,count(*) Count " +
      " from SPD.patients where id " +
      " in (select PATIENT from SPD.conditions where DESCRIPTION like " +
      "'Major depression disorder' and DESCRIPTION not like 'Major depression  single episode')" +
      " group by Ethnicity order by 2 desc limit 10" +
      " Union" +
      " select Ethnicity,count(*) Count from SPD.patients" +
      " where id in (select PATIENT from SPD.conditions where DESCRIPTION like " +
      " 'Major depression disorder' and DESCRIPTION not like 'Major depression  single episode')" +
      " group by Ethnicity order by 2 asc limit 5"

  val Q3_6: String = "SELECT * FROM ((SELECT Ethnicity, COUNT(Ethnicity) AS Ethnicity_Count " +
      " FROM spd.PATIENTS AS P  " +
      " INNER JOIN (select PATIENT FROM spd.CONDITIONS WHERE DESCRIPTION='Major depression disorder') AS C " +
      " ON  C.PATIENT=P.ID  GROUP BY  Ethnicity ORDER BY Ethnicity_Count DESC  LIMIT 10)" +
      " UNION " +
      " (SELECT Ethnicity, COUNT(Ethnicity) AS Ethnicity_Count FROM spd.PATIENTS AS P" +
      "  INNER JOIN (select PATIENT FROM spd.CONDITIONS WHERE DESCRIPTION='Major depression disorder') AS C" +
      " ON  C.PATIENT=P.ID  GROUP BY  Ethnicity ORDER BY Ethnicity_Count ASC LIMIT 5)) AS V " +
      " ORDER BY V.Ethnicity_Count"

  val Q3_7_1: String = "drop view if exists spd.top_ethnicity_with_major_depression"

  val Q3_7_2: String = "create view spd.ethnicity_count_with_major_depression " +
      " as select p.ETHNICITY, count(1) as count " +
      " from SPD.PATIENTS as p, SPD.CONDITIONS as c " +
      " where p.ID = c.PATIENT and c.DESCRIPTION like 'Major depression disorder'" +
      " group by 1"

  val Q3_7_3: String = "drop view if exists spd.ethnicity_without_single_episode"

  val Q3_7_4: String = "create view spd.ethnicity_without_single_episode " +
      " as select distinct(p2.ETHNICITY) from SPD.PATIENTS as p2, SPD.CONDITIONS as c2 " +
      " where p2.ID = c2.PATIENT and c2.DESCRIPTION not like 'Major depression  single episode'"

  val Q3_7_5: String = "select * from " +
      " (select * from spd.ETHNICITY_COUNT_WITH_MAJOR_DEPRESSION " +
      "   where ETHNICITY in (select ETHNICITY from spd.ETHNICITY_WITHOUT_SINGLE_EPISODE)" +
      "   order by count desc limit 10" +
      " union" +
      " select * from spd.ETHNICITY_COUNT_WITH_MAJOR_DEPRESSION" +
      " where ETHNICITY in (select ETHNICITY from spd.ETHNICITY_WITHOUT_SINGLE_EPISODE) " +
      " order by count asc limit 5) order by count"

  val Q3_8: String = "SELECT ethnicity,max " +
      " FROM(SELECT p.ethnicity,count(p.ID) as max " +
      " FROM spd.PATIENTS p,spd.conditions c " +
      " where p.ID = c.PATIENT AND c.description='Major depression disorder' " +
      " group by p.ethnicity order by max desc limit 10 UNION SELECT p.ethnicity,count(p.ID) " +
      " as min FROM spd.PATIENTS p,spd.conditions c " +
      " where p.ID = c.PATIENT AND c.description='Major depression disorder' " +
      " group by p.ethnicity order by min asc limit 5) " +
      " group by ethnicity,max order by max desc"

  val Q3_9: String = "(SELECT COUNT(ethnicity) as PATIENT_COUNT, ethnicity " +
      " FROM spd.patients " +
      " WHERE id IN (SELECT patient FROM spd.conditions WHERE description = 'Major depression disorder') " +
      " GROUP BY ethnicity ORDER BY COUNT(ethnicity) DESC LIMIT 10) " +
      " UNION " +
      " (SELECT COUNT(ethnicity) as PATIENT_COUNT, ethnicity " +
      " FROM spd.patients " +
      " WHERE id IN (SELECT patient FROM spd.conditions WHERE description = 'Major depression disorder') " +
      " GROUP BY ethnicity ORDER BY COUNT(ethnicity) ASC LIMIT 5)"

  val Q3_10: String = "SELECT * FROM " +
      " (SELECT * FROM (SELECT COUNT(1) as eth_count,p.Ethnicity " +
      " FROM SPD.Patients p JOIN SPD.Conditions c ON c.Patient=p.ID " +
      " WHERE c.description='Major depression disorder' AND c.Description<>'Major depression  single episode' " +
      " GROUP BY p.Ethnicity ORDER BY eth_count DESC LIMIT 10) a " +
      " union " +
      " SELECT * FROM (SELECT COUNT(1) as eth_count,p.Ethnicity " +
      " FROM SPD.Patients p " +
      " JOIN SPD.Conditions c ON c.Patient=p.ID " +
      " WHERE c.description='Major depression disorder' AND c.Description<>'Major depression  single episode' " +
      " GROUP BY p.Ethnicity ORDER BY eth_count ASC LIMIT 5) b) ORDER BY eth_count DESC"

  val Q3_11: String = "select p1.ethnicity from spd.patients p1 " +
      " join spd.conditions c1 on p1.id=c1.patient " +
      " where c1.Description='Major depression disorder' AND c1.patient not in " +
      " (select c1.patient from spd.conditions c1 where c1.description='Major depression  single episode')" +
      " group by ethnicity order by count(ID) desc limit 10"

  val Q3_12: String = "select count(1) as count, p.ethnicity " +
      " from spd.patients p, spd.conditions condi " +
      " where p.id=condi.patient and condi.description='Major depression disorder'  " +
      " group by ethnicity order by count desc limit 10 " +
      " union  " +
      " select count(1) as count, p.ethnicity from spd.patients p, spd.conditions condi " +
      " where p.id=condi.patient and condi.description='Major depression disorder'  " +
      " group by ethnicity order by count  limit 5"


  val Q3_13: String = "SELECT ethnicity, patients_count " +
      " FROM ((select count(*) as patients_count, ethnicity from spd.patients " +
      " where id in (select patient from spd.conditions  " +
      " where description LIKE 'Major depression%' AND description <> 'Major depression  single episode') " +
      " group by ethnicity order by count(ethnicity) desc limit 10)" +
      " union" +
      " (select count(*) as patients_count, ethnicity from spd.patients " +
      " where id in (select patient from spd.conditions" +
      " where description LIKE 'Major depression%' AND description <> 'Major depression  single episode')" +
      " group by ethnicity order by count(ethnicity) asc limit 5)) ORDER BY patients_count desc"


  val Q3_14: String = "select * from " +
      " (select count(1) numberOfPatient, p.ethnicity, concat('1',' Top 10 ethnicities') as filter " +
      " from spd.Patients p ,spd.encounters e, spd.careplans ca, spd.conditions c" +
      " where p.id=e.patient and p.id = ca.patient and p.id=c.patient and " +
      " e.reasoncode=370143000 and e.reasoncode<>36923009 and ca.reasoncode=370143000 and " +
      " ca.reasoncode<>36923009 and c.code = 370143000 and c.code<>36923009 " +
      " group by p.ethnicity order by numberOfPatient desc limit 10 " +
      " union" +
      " select count(1) numberOfPatient, p.ethnicity, concat('2',' Bottom 5 Ethnicities') as filter " +
      " from spd.Patients p ,spd.encounters e, spd.careplans ca, spd.conditions c " +
      " where p.id=e.patient and p.id = ca.patient and p.id=c.patient and e.reasoncode=370143000 " +
      " and e.reasoncode<>36923009 and ca.reasoncode=370143000 and ca.reasoncode<>36923009 and " +
      " c.code = 370143000 and c.code<>36923009 group by p.ethnicity " +
      " order by numberOfPatient asc limit 5)" +
      " order by filter asc,numberOfPatient desc"

  // Find out which medicines have been used the most by patients with 'Hypertension'
  val Q4_1: String = "select description, count(1) as uses from spd.medications where " +
      " reasondescription = 'Hypertension' " +
      " group by description order by uses desc"

  val Q4_2: String = "SELECT code, description, SUM(dispenses) AS dispenses " +
      " FROM SPD.MEDICATIONS " +
      " WHERE reasondescription = 'Hypertension' " +
      " GROUP BY code, description ORDER BY DISPENSES LIMIT 10"

  // Also from these patients find out patients who do not have care plans of either 'Anti-suicide psychotherapy' or
  // 'Psychiatry care plan' or 'Major depressive disorder clinical management plan'
  // table q had to be created seperately. if used as a sub query wrong plan gets generated in snappy
  val Q5_1_1: String = "create table q as " +
      " select patient from (" +
      " select  *, " +
      "         case when description in ('Anti-suicide psychotherapy', 'Psychiatry care plan', " +
      "                                   'Major depressive disorder clinical management plan') then 1 else 0 end " +
      " as coverage from spd.careplans) c  " +
      " group by patient having sum(coverage) = 0 "

  val Q5_1_2: String = "select p.first, p.last from q " +
      " join spd.patients p on id = patient " +
      " where id in (select patient from weak_hearts)"

  val Q5_2: String = "SELECT c.patient FROM" +
      " (SELECT DISTINCT patient FROM spd.CONDITIONS WHERE description = 'Major depression disorder') c " +
      " LEFT JOIN (SELECT DISTINCT patient FROM spd.careplans " +
      " WHERE description " +
      " IN ('Anti-suicide psychotherapy','Psychiatry care plan','Major depressive disorder clinical management plan')" +
      " AND reasondescription = 'Major depression disorder') cp " +
      " ON c.patient = cp.patient WHERE cp.patient IS NULL"

  // Find out top 10 areas which has the maximum number of patients with expired care plans
  // ( exclude patients who have already died )
  val Q6_1: String = "select zip, sum(expired) as unshielded from spd.patients p join (select patient, " +
      " case when stop is null then 0 else 1 end as expired from spd.careplans) c on c.patient = p.id " +
      " where deathdate is null group by zip order by unshielded desc limit 10"

  val Q6_2: String = "SELECT * FROM (SELECT city, state , zip, RANK() OVER (ORDER BY COUNT DESC) AS RANK, " +
      " COUNT FROM (SELECT COUNT(id) AS COUNT, zip, city, state FROM " +
      " (SELECT DISTINCT p.id, p.zip, p.city, p.STATE FROM SPD.PATIENTS p " +
      "  LEFT JOIN (SELECT DISTINCT patient FROM SPD.careplans WHERE stop IS NULL) c " +
      " ON p.id = c.patient WHERE c.patient IS NULL AND p.deathdate IS NULL ) " +
      " GROUP BY zip, city, state ) ) " +
      " WHERE RANK <= 10"

  // Which area has 1st and 2nd highest allergies in each of the following category
  // 'Allergy to tree pollen' and 'House dust mite allergy'
  val Q7_1: String = "select *, rank() over(partition by description order by allergics desc) as rnk " +
      " from (select zip, description, count(*) as allergics from spd.patients p " +
      " join spd.allergies a on a.patient = p.id" +
      " where description in ('Allergy to tree pollen', 'House dust mite allergy') group by zip, a.description) q " +
      " having rnk < 3 order by description, rnk"

  val Q7_2: String = "SELECT * FROM ( SELECT COUNT, description, zip, city, state, RANK() " +
      " OVER ( PARTITION BY description ORDER BY COUNT DESC) AS RANK " +
      " FROM (SELECT COUNT(patient) COUNT, description, zip, city, state " +
      " FROM (SELECT a.patient, a.description, p.zip, p.city, p.state " +
      " FROM SPD.allergies a JOIN SPD.patients p ON a.patient = p.id " +
      " WHERE a.description IN ('Allergy to tree pollen', 'House dust mite allergy') ) " +
      " GROUP BY description, zip, city, state)) WHERE RANK <= 2"

  // Find out the top 3 risky diseases in age groups of 10 year slabs: 0-10, 10-20, .., 90-100
  val Q8_1: String = "select *, dense_rank() over(distribute by age_group order by diseasecount desc) as rnk " +
      " from (select age_group, c.description, count(1) as diseasecount from spd.conditions c " +
      " join (select  *, cast((year(coalesce(deathdate, current_timestamp())) - year(birthdate))/10 as int) " +
      " as age_group from spd.patients) p on c.patient = p.id" +
      " group by age_group, c.description) q having rnk < 4 order by age_group"

  val Q8_2: String = "SELECT condition_code, description, CONCAT(age_range, '0 to ', age_range + 1, '0') " +
      " AS age_range, COUNT, RANK FROM " +
      " ( SELECT code AS condition_code, description, age_range, COUNT, " +
      " RANK() OVER (PARTITION BY age_range ORDER BY COUNT DESC) AS RANK " +
      " FROM (SELECT COUNT(*) COUNT, code, description, age_range " +
      " FROM (SELECT p.id, c.description, c.code, " +
      " CEIL(datediff(COALESCE( p.DEATHDATE, CURRENT_DATE()), p.birthdate)/ 3650) AS age_range " +
      " FROM SPD.conditions c " +
      " JOIN SPD.patients p ON p.id = c.patient) " +
      " WHERE age_range >= 0 " +
      " GROUP BY code, description, age_range)) " +
      " WHERE RANK <= 3"

  // Find the average cost incurred on each medical condition across the population,
  // and its difference from the total average cost across all conditions.
  val Q9_1: String = "with temp as (select c.description as condition, avg(pr.cost) as procedure_avg_cost " +
      " from spd.patients p join spd.conditions c on c.patient = p.id join spd.procedures pr on p.id = pr.patient " +
      " group by c.description)" +
      " select *, procedure_avg_cost - avg_cost as deviation  " +
      " from temp t cross join (select avg(procedure_avg_cost) as avg_cost from temp) q"

  val Q9_2: String = "SELECT condition_code, CONDITION, average_cost_per_condition, " +
      " average_cost_per_condition - average_cost " +
      " FROM " +
      " (SELECT reasoncode AS condition_code,reasondescription AS CONDITION, " +
      "  total_cost / patient_count AS average_cost_per_condition," +
      " (SELECT SUM(totalcost) AS total_cost FROM SPD.MEDICATIONS)/(SELECT COUNT(DISTINCT patient) AS patient_count " +
      " FROM SPD.conditions) AS average_cost " +
      " FROM (SELECT reasoncode, reasondescription, SUM(totalcost) AS total_cost " +
      " FROM SPD.medications " +
      " GROUP BY reasoncode, reasondescription) m " +
      " JOIN (SELECT code, COUNT (DISTINCT patient) AS  patient_count " +
      " FROM SPD.conditions GROUP BY code) c ON m.reasoncode = c.code)"

  // moving average(with window of 3 rows) of yearly medication dispenses for each medicine.
  // (based on medication START date)

  val Q10: String = "SELECT code,YEAR(start), DESCRIPTION, dispenses, AVG(dispenses) " +
      " OVER (PARTITION BY code ORDER BY year(start) ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM SPD.medications"

  // Find patients with medical cost above average in their city.
  // Correlated subquery with view.
  val Q11_1: String = "CREATE VIEW IF NOT EXISTS SPD.patient_expenses " +
      " AS SELECT concat(p.first, ' ', p.LAST) AS name," +
      " p.city,m.total_cost AS expenses " +
      " FROM SPD.patients p " +
      " JOIN (SELECT patient, SUM(cost) AS total_cost " +
      " FROM ((SELECT patient, totalcost AS cost FROM SPD.medications)" +
      " UNION ALL " +
      " (SELECT patient, cost FROM SPD.immunizations )" +
      " UNION ALL " +
      " (SELECT patient, cost FROM SPD.encounters) " +
      " UNION ALL (SELECT patient,cost FROM SPD.procedures)) " +
      " GROUP BY patient ) m ON p.id = m.patient"

  val Q11_2: String = "SELECT * FROM SPD.patient_expenses pe" +
      " WHERE expenses > (SELECT AVG(expenses) FROM SPD.patient_expenses WHERE city = pe.city)"

  // Find medication falling above 0.5 percentile in the distribution of dispenses
  val Q12: String = "SELECT code, dispenses, description " +
      " FROM (SELECT SUM(dispenses) AS dispenses,code, description " +
      " FROM spd.MEDICATIONS GROUP BY code, description)" +
      " WHERE dispenses > (SELECT approx_percentile(dispenses, 1.0, 100) FROM SPD.medications)"

  val queries = List(
    "Q1_1" -> Q1_1,
    "Q1_2" -> Q1_2,
    "Q1_3" -> Q1_3,
    "Q1_4" -> Q1_4,
    "Q1_5" -> Q1_5,
    "Q1_6" -> Q1_6,
    "Q1_7" -> Q1_7,
    "Q1_8" -> Q1_8,
    "Q1_9" -> Q1_9,
    "Q2_1" -> Q2_1,
    "Q2_2" -> Q2_2,
    "Q2_3" -> Q2_3,
    "Q2_4" -> Q2_4,
    "Q2_5" -> Q2_5,
    "Q2_6" -> Q2_6,
    "Q2_7" -> Q2_7,
    "Q2_8" -> Q2_8,
    "Q2_9" -> Q2_9,
    "Q2_10" -> Q2_10,
    "Q2_11" -> Q2_11,
    "Q2_12" -> Q2_12,
    "Q2_13" -> Q2_13,
    "Q3_1_1" -> Q3_1_1,
    "Q3_1_2" -> Q3_1_2,
    "Q3_2_1" -> Q3_2_1,
    "Q3_2_2" -> Q3_2_2,
    "Q3_3" -> Q3_3,
    "Q3_4_1" -> Q3_4_1,
    "Q3_4_2" -> Q3_4_2,
    "Q3_4_3" -> Q3_4_3,
    "Q3_5" -> Q3_5,
    "Q3_6" -> Q3_6,
    "Q3_7_1" -> Q3_7_1,
    "Q3_7_2" -> Q3_7_2,
    "Q3_7_3" -> Q3_7_3,
    "Q3_7_4" -> Q3_7_4,
    "Q3_7_5" -> Q3_7_5,
    "Q3_8" -> Q3_8,
    "Q3_9" -> Q3_9,
    "Q3_10" -> Q3_10,
    "Q3_11" -> Q3_11,
    "Q3_12" -> Q3_12,
    "Q3_13" -> Q3_13,
    "Q3_14" -> Q3_14,
    "Q4_1" -> Q4_1,
    "Q4_2" -> Q4_2,
    "Q5_1_1" -> Q5_1_1,
    "Q5_1_2" -> Q5_1_2,
    "Q5_2" -> Q5_2,
    "Q6_1" -> Q6_1,
    "Q6_2" -> Q6_2,
    "Q7_1" -> Q7_1,
    "Q7_2" -> Q7_2,
    "Q8_1" -> Q8_1,
    "Q8_2" -> Q8_2,
    "Q9_1" -> Q9_1,
    "Q9_2" -> Q9_2,
    "Q10" -> Q10,
    "Q11_1" -> Q11_1,
    "Q11_2" -> Q11_2,
    "Q12" -> Q12
  )

  def patients(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks.spark" +
      ".csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/patients.csv")

  val patients_table = "create table SPD.patients (" +
      "ID         string, " +
      "BIRTHDATE  timestamp," +
      "DEATHDATE  timestamp," +
      "SSN        string, " +
      "DRIVERS    string, " +
      "PASSPORT   string, " +
      "PREFIX     string, " +
      "FIRST      string, " +
      "LAST       string, " +
      "SUFFIX     string, " +
      "MAIDEN     string, " +
      "MARITAL    string, " +
      "RACE       string, " +
      "ETHNICITY  string, " +
      "GENDER     string, " +
      "BIRTHPLACE string, " +
      "ADDRESS    string, " +
      "CITY       string, " +
      "STATE      string, " +
      "ZIP        int )"

  def encounters(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/encounters.csv")

  val encounters_table = "create table SPD.encounters (" +
      "ID  string, " +
      "START timestamp, " +
      "STOP timestamp, " +
      "PATIENT string, " +
      "ENCOUNTERCLASS string, " +
      "CODE  bigint, " +
      "DESCRIPTION  string, " +
      "COST  double, " +
      "REASONCODE  bigint, " +
      "REASONDESCRIPTION string)"

  def allergies(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks.spark" +
      ".csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/allergies.csv")

  val allergies_table = "create table SPD.allergies (" +
      "START timestamp, " +
      "STOP timestamp, " +
      "PATIENT string, " +
      "ENCOUNTER string, " +
      "CODE int, " +
      "DESCRIPTION  string )"

  def careplans(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/careplans.csv")

  val careplans_table = "create table SPD.careplans(" +
      "ID string, " +
      "START timestamp, " +
      "STOP  timestamp,  " +
      "PATIENT string,  " +
      "ENCOUNTER string, " +
      "CODE bigint, " +
      "DESCRIPTION string, " +
      "REASONCODE  bigint, " +
      "REASONDESCRIPTION string)"

  def conditions(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/conditions.csv")

  val conditions_table = "create table SPD.conditions(" +
      "START timestamp, " +
      "STOP timestamp, " +
      "PATIENT string, " +
      "ENCOUNTER string, " +
      "CODE  bigint, " +
      "DESCRIPTION string )"

  def imaging_studies(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/imaging_studies.csv")

  val imaging_studies_table = "create table SPD.imaging_studies (" +
      "ID  string, " +
      "DATE timestamp, " +
      "PATIENT string, " +
      "ENCOUNTER string, " +
      "BODYSITE_CODE int,  " +
      "BODYSITE_DESCRIPTION string, " +
      "MODALITY_CODE string, " +
      "MODALITY_DESCRIPTION string, " +
      "SOP_CODE string,  " +
      "SOP_DESCRIPTION  string)"

  def immunizations(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/immunizations.csv")


  val immunizations_table = "create table SPD.immunizations (" +
      "DATE timestamp, " +
      "PATIENT string, " +
      "ENCOUNTER string, " +
      "CODE int, " +
      "DESCRIPTION string, " +
      "COST double)"

  def medications(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks.spark" +
      ".csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/medications.csv")

  val medications_table = "create table SPD.medications(" +
      "START timestamp, " +
      "STOP  timestamp, " +
      "PATIENT   string, " +
      "ENCOUNTER string, " +
      "CODE int, " +
      "DESCRIPTION string, " +
      "COST double,  " +
      "DISPENSES int, " +
      "TOTALCOST double, " +
      "REASONCODE bigint, " +
      "REASONDESCRIPTION string) "

  def observations(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/observations.csv")

  val observations_table = "create table SPD.observations(" +
      "DATE  timestamp, " +
      "PATIENT string, " +
      "ENCOUNTER string, " +
      "CODE string, " +
      "DESCRIPTION string, " +
      "VALUE string, " +
      "UNITS string, " +
      "TYPE string) "

  def procedures(sqlContext: SQLContext): DataFrame = sqlContext.read.format("com.databricks" +
      ".spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("nullValue", "NULL")
      .option("maxCharsPerColumn", "4096")
      .load(s"${snc.getConf("dataFilesLocation")}/procedures.csv")

  val procedures_table = "create table SPD.procedures(" +
      "DATE timestamp, " +
      "PATIENT string, " +
      "ENCOUNTER string, " +
      "CODE string, " +
      "DESCRIPTION string, " +
      "COST double, " +
      "REASONCODE bigint, " +
      "REASONDESCRIPTION string)"

}