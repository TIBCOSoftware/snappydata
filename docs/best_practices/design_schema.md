# Database design (schema definition):

## How to decide row table/vs column table (row partitioned/replicated)

## Partitioning scheme to use on column and row table/Collocated joins
(https://jira.snappydata.io/browse/CRQ-23)

## Redundancy - when to use

## Row table/column table/replicated tables

## Overflow configuration


<mark> Jags Comment</mark>
- - How to do physical design with SnappyData? Start with the "designing rowstore dbs" in our current rowstore docs. Needs a lot of refinement. A lot of the principles with colocation still apply.

- Using Row table or column table - When to use what?
Partitioning/Redundancy/Persistence

- Design Principles of Scalable, 

- Identify Entity Groups and Partitioning Keys 

- Replicate Dimension Tables 

- Example: Adapting a Database Schema for SnappyData

- Dealing with Many-to-Many Relationships 
http://rowstore.docs.snappydata.io/docs/data_management/database_design_chapter.html