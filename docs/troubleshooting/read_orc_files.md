###<question>**Can I read orc files from `snappy` ?**
</question>

<solution> 
ORC is a supported format in Spark SQL.
```
create Table orcTable using orc options (path "path to orc file")
```

This would create a spark temporary table. You would need to explicitly load into a column table like - create table colTable using column options(..) as select * from orcTable ...
</solution>
