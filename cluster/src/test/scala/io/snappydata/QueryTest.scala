package io.snappydata
class QueryTest extends SnappyFunSuite {

  test("Test exists in select") {
    val snContext = org.apache.spark.sql.SnappyContext(sc)

    snContext.sql("CREATE TABLE titles(title_id varchar(20), title varchar(80) " +
        "not null, type varchar(12) not null, pub_id varchar(4), price int not null, " +
        "advance int not null , royalty int , ytd_sales int,notes varchar(200))")

    snContext.sql("insert into titles values ('1', 'Secrets',   'popular_comp', '1389', 20, 8000, 10, 4095,'Note 1')")
    snContext.sql("insert into titles values ('2', 'The',       'business',     '1389', 19, 5000, 10, 4095,'Note 2')")
    snContext.sql("insert into titles values ('3', 'Emotional', 'psychology',   '0736', 7,  4000, 10, 3336,'Note 3')")
    snContext.sql("insert into titles values ('4', 'Prolonged', 'psychology',   '0736', 19, 2000, 10, 4072,'Note 4')")
    snContext.sql("insert into titles values ('5', 'With',      'business',     '1389', 11, 5000, 10, 3876,'Note 5')")
    snContext.sql("insert into titles values ('6', 'Valley',    'mod_cook',     '0877', 9,  0,    12, 2032,'Note 6')")
    snContext.sql("insert into titles values ('7', 'Any?',      'trad_cook',    '0877', 14, 8000, 10, 4095,'Note 7')")
    snContext.sql("insert into titles values ('8', 'Fifty',     'trad_cook',    '0877', 11, 4000, 14, 1509,'Note 8')")

    snContext.sql("CREATE TABLE sales(stor_id varchar(4) not null, ord_num varchar(20) not null," +
        "qty int not null,payterms varchar(12) not null,title_id varchar(80))")

    snContext.sql("insert into sales values('1', 'QA7442.3',  75, 'ON Billing','1')")
    snContext.sql("insert into sales values('2', 'D4482',     10, 'Net 60',    '1')")
    snContext.sql("insert into sales values('3', 'N914008',   20, 'Net 30',    '2')")
    snContext.sql("insert into sales values('4', 'N914014',   25, 'Net 30',    '3')")
    snContext.sql("insert into sales values('5', '423LL922',  15, 'ON Billing','3')")
    snContext.sql("insert into sales values('6', '423LL930',  10, 'ON Billing','2')")

    val df = snContext.sql("SELECT  title, price FROM titles WHERE EXISTS (SELECT * FROM sales WHERE sales.title_id = titles.title_id AND qty >30)")

    df.show()

  }
}
