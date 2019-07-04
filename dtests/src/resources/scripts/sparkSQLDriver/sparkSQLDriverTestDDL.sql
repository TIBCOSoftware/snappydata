SHOW DATABASES;

DROP DATABASE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8 CASCADE;
CREATE DATABASE sf_tmp_db_38611be8de554936beae283efdbe3ee8;

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.info;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.info(
created_time TIMESTAMP, branch_name STRING, created_by STRING);

INSERT INTO sf_tmp_db_38611be8de554936beae283efdbe3ee8.info SELECT '2019-05-31 08:30:16', 'master', 'jstroems';

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_INT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_INT(
col_INT INT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_TINYINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_TINYINT(
col_TINYINT TINYINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_SMALLINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_SMALLINT(
col_SMALLINT SMALLINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_TINYINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_TINYINT(
col_TINYINT TINYINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_BIGINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_BIGINT(
col_BIGINT BIGINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_INT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_INT(
col_INT INT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_BIGINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_BIGINT(
col_BIGINT BIGINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_SMALLINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_SMALLINT(
col_SMALLINT SMALLINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_FLOAT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_FLOAT(
col_FLOAT FLOAT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_FLOAT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_FLOAT(
col_FLOAT FLOAT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_DOUBLE;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_DOUBLE(
col_DOUBLE DOUBLE);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_DOUBLE;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_DOUBLE(
col_DOUBLE DOUBLE);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_BOOLEAN;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_BOOLEAN(
col_BOOLEAN BOOLEAN);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_BOOLEAN;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_BOOLEAN(
col_BOOLEAN BOOLEAN);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_STRING;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_STRING(
col_STRING STRING);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_STRING;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_STRING(
col_STRING STRING);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_TIMESTAMP;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_schema_col_TIMESTAMP(
col_TIMESTAMP TIMESTAMP);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_TIMESTAMP;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_schema_col_TIMESTAMP(
col_TIMESTAMP TIMESTAMP);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TINYINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TINYINT(
col_TINYINT TINYINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_SMALLINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_SMALLINT(
col_SMALLINT SMALLINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TINYINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TINYINT(
col_TINYINT TINYINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_SMALLINT_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_SMALLINT_with_filter(
col_SMALLINT SMALLINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TINYINT_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TINYINT_with_filter(
col_TINYINT TINYINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TINYINT_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TINYINT_with_filter(
col_TINYINT TINYINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_INT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_INT(
col_INT INT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_FLOAT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_FLOAT(
col_FLOAT FLOAT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_SMALLINT_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_SMALLINT_with_filter(
col_SMALLINT SMALLINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_FLOAT_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_FLOAT_with_filter(
col_FLOAT FLOAT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_INT_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_INT_with_filter(
col_INT INT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BIGINT_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BIGINT_with_filter(
col_BIGINT BIGINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BIGINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BIGINT(
col_BIGINT BIGINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BIGINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BIGINT(
col_BIGINT BIGINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_SMALLINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_SMALLINT(
col_SMALLINT SMALLINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_DOUBLE;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_DOUBLE(
col_DOUBLE DOUBLE);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BIGINT_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BIGINT_with_filter(
col_BIGINT BIGINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_INT_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_INT_with_filter(
col_INT INT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_INT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_INT(
col_INT INT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_FLOAT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_FLOAT(
col_FLOAT FLOAT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_FLOAT_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_FLOAT_with_filter(
col_FLOAT FLOAT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_DOUBLE;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_DOUBLE(
col_DOUBLE DOUBLE);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TIMESTAMP;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TIMESTAMP(
col_TIMESTAMP TIMESTAMP);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TIMESTAMP_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TIMESTAMP_with_filter(
col_TIMESTAMP TIMESTAMP);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BOOLEAN;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BOOLEAN(
col_BOOLEAN BOOLEAN);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_DOUBLE_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_DOUBLE_with_filter(
col_DOUBLE DOUBLE);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BOOLEAN_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BOOLEAN_with_filter(
col_BOOLEAN BOOLEAN);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_DOUBLE_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_DOUBLE_with_filter(
col_DOUBLE DOUBLE);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_STRING;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_STRING(
col_STRING STRING);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_STRING_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_STRING_with_filter(
col_STRING STRING);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_STRING;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_STRING(
col_STRING STRING);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_STRING_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_STRING_with_filter(
col_STRING STRING);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BOOLEAN;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BOOLEAN(
col_BOOLEAN BOOLEAN);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TIMESTAMP_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TIMESTAMP_with_filter(
col_TIMESTAMP TIMESTAMP);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BOOLEAN_with_filter;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BOOLEAN_with_filter(
col_BOOLEAN BOOLEAN);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TIMESTAMP;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TIMESTAMP(
col_TIMESTAMP TIMESTAMP);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_TINYINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_TINYINT(
col_TINYINT TINYINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_SMALLINT_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_SMALLINT_with_filter_2(
col_SMALLINT SMALLINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TINYINT_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TINYINT_with_filter_2(
col_TINYINT TINYINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_INT_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_INT_with_filter_2(
col_INT INT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_SMALLINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_SMALLINT(
col_SMALLINT SMALLINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_SMALLINT_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_SMALLINT_with_filter_2(
col_SMALLINT SMALLINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_INT_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_INT_with_filter_2(
col_INT INT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_INT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_INT(
col_INT INT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TINYINT_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TINYINT_with_filter_2(
col_TINYINT TINYINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_BOOLEAN;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_BOOLEAN(
col_BOOLEAN BOOLEAN);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_FLOAT_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_FLOAT_with_filter_2(
col_FLOAT FLOAT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_BIGINT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_BIGINT(
col_BIGINT BIGINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_DOUBLE;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_DOUBLE(
col_DOUBLE DOUBLE);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BIGINT_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BIGINT_with_filter_2(
col_BIGINT BIGINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_DOUBLE_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_DOUBLE_with_filter_2(
col_DOUBLE DOUBLE);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BIGINT_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BIGINT_with_filter_2(
col_BIGINT BIGINT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_DOUBLE_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_DOUBLE_with_filter_2(
col_DOUBLE DOUBLE);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BOOLEAN_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_BOOLEAN_with_filter_2(
col_BOOLEAN BOOLEAN);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_FLOAT_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_FLOAT_with_filter_2(
col_FLOAT FLOAT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_STRING;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_STRING(
col_STRING STRING);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_FLOAT;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_FLOAT(
col_FLOAT FLOAT);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_STRING_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_STRING_with_filter_2(
col_STRING STRING);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_STRING_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_STRING_with_filter_2(
col_STRING STRING);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BOOLEAN_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_BOOLEAN_with_filter_2(
col_BOOLEAN BOOLEAN);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_all_data_types_50_rows;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_all_data_types_50_rows(
col_TINYINT TINYINT, col_SMALLINT SMALLINT, col_INT INT, col_BIGINT BIGINT, col_FLOAT FLOAT, col_DOUBLE DOUBLE, col_BOOLEAN BOOLEAN, col_STRING STRING, col_TIMESTAMP TIMESTAMP);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TIMESTAMP_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_for_cq_query_TIMESTAMP_with_filter_2(
col_TIMESTAMP TIMESTAMP);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TIMESTAMP_with_filter_2;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_query_col_TIMESTAMP_with_filter_2(
col_TIMESTAMP TIMESTAMP);

DROP TABLE IF EXISTS sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_TIMESTAMP;
CREATE TABLE sf_tmp_db_38611be8de554936beae283efdbe3ee8.tbl_special_chars_const_TIMESTAMP(
col_TIMESTAMP TIMESTAMP);
