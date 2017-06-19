#Using Data Generator

####You can use the data generator for a table/schema using DataGenerator.java byt followign the below mentioned steps

1. Create mapper file as per requirement.
```
    Format for mapper file:
	<schema-name>.<table-name>.<column-name> = <pattern>

    Available patterns:
	::format 999999   		                 -- number format of fixed length
	::format 999.99    		                -- decimal format of fixed length
	::random [5] length  	                  -- fixed length for both number and characters
	::ramdom [2-3] length	                  -- variable length values values for number or characters
	::valuelist {val1,val2,val3} 		      -- select value from given list sequentially
	::valuelist {val1,val2,val3} randomize	 -- select value randomly from given list
```

Example for mapper file can be found [here]().

2. Start row cluster using ‘./sbin/snappy-start-all rowstore’.

3. Run snappy-sql using ‘./bin/snappy-sql rowstore’ and connect using ‘connect client 'localhost:1527';’

4. Create the tables for which data is to be generated.

5. You need to use the data generator script - [gen.sh](gen.sh) for generating the data.
	*Before executing check the gen.sh script. gen.sh should use latest version snappydata-client jar under < snappydata-checkout-dir>/build-artifacts/scala-2.11/store/lib/*

6. Run the gen.sh script using :
```
 export SNAPPY_HOME=<product_checkout_dir>
 ./gen.sh [cc]  <mapper_file>  <num_rows> <full_table_name>
```

	where,
```
 [cc]              -- optional, for compiling the DataGenerator.java.
 <mapper_file>     -- path and name for mapper file created.
 <num_rows>        -- number of rows in to be generated for the table.
 <full_table_name> -- fully qualified tablename in upper case.
```

####Example for running the gen.sh script
```
./gen.sh [cc] order_details_mapper.txt 10000 APP.ORDER_DETAILS
```