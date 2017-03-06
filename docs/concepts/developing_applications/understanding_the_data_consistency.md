#Understanding the Data Consistency Model


## Coloum and Row Table

## Atomicity for Bulk Updates: SnappyData does not validate the constraints for all affected rows before applying a bulk update (a single DML statement that updates or inserts multiple rows). The design is optimized for applications where such violations are rare.