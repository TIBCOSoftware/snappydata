elapsedtime on;

select count(*) from mbl_test_scd;
select count(1) from mbl_test_scd;

SELECT CAST(ts AS TIMESTAMP) data_version,
      meet,
      beat,
      lose,
      ROUND(meet*100/(meet+beat+lose),2) AS meet_rate,
      ROUND(beat*100/(meet+beat+lose),2) AS beat_rate,
      ROUND(lose*100/(meet+beat+lose),2) AS lose_rate,
      ROUND((ROUND(beat*100/(meet+beat+lose),2)-2*ROUND(lose*100/(meet+beat+lose),2)),2) AS mbl
 FROM (
       SELECT meet,beat,lose, (meet+beat+lose) as ts
         FROM (
               SELECT meet,beat,lose
                 FROM (
                       SELECT count(1) as  meet,count(1) as beat, count(1) as lose,
                              detail.mt_poi_id,
                              detail.mt_room_id,
                              detail.mt_breakfast
                         FROM mbl_test_scd detail
                        WHERE (detail.DATEKEY = 1082203903 and detail.CHECKIN_DATE = 90990033 and detail.MT_ROOM_STATUS =27650 and (detail.COMP_ROOM_STATUS =10123   or detail.comp_site_id = 1848225246))
                        GROUP BY detail.mt_poi_id,
                                 detail.mt_room_id,
                                 detail.mt_breakfast
                      )
              )
 );