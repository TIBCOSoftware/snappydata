## Heading 
Text
## Heading 
Text
## Heading
Text


**Table:**

First Header | Second Header | Third Header
:----------- |:-------------:| -----------:
Left         | Center        | Right
Left         | Center        | Right


**Image Link:**
![SDE_Architecture](../Images/sde_architecture.png)

**Hyperlink:**
Refer to the [handbook](https://web.eecs.umich.edu/~mozafari/php/data/uploads/approx_chapter.pdf).

**Formatting:**
**Bold**, ++Underline++, *Italics*

**Bullets and Numbering:**
* Item 
* Item 
* Item 
	- sub-item
	- sub-item
	- sub-item

1. Item 1
2. Item 2
3. Item 3
	1.1 
	1.2
	1.3

**Code Example:**
```
CREATE EXTERNAL TABLE TAXIFARE USING parquet 
  options  (qcs 'hack_license', fraction '0.01') AS (SELECT * FROM TAXIFARE);

```

**Code Text:**
The values are `do_nothing`, `local_omit`, `strict`,  `run_on_full_table`, `partial_run_on_base_table`. The default value is `run_on_full_table`	

**Note: **
> Note: The value of the QCS column should not be empty or set to null for stratified sampling, or an error may be reported when the query is executed.


