INSERT INTO Student SELECT id,'Sonia Sen'||id,Array(62.1,50.7,52.3,67.9,69.9,66.8) from range(11,1000);
PUT INTO AGREEMENT select id,abs(rand()*1000),abs(rand()*1000),'agree_cd','description','2018-01-01','2019-01-01',from_unixtime(unix_timestamp('2018-01-01 01:00:00')+floor(rand()*31536000)),from_unixtime(unix_timestamp('2019-01-01 01:00:00')+floor(rand()*31536000)),'src_sys_ref_id','src_sys_rec_id' FROM range(50000001,50002000);
UPDATE AGREEMENT set DESCR='Modified Description' where AGREE_ID=100;
DELETE FROM BANK WHERE BNK_ID < 350;