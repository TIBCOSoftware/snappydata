# Dealing with Many-to-Many Relationships

Where tables have many-to-many relationships, you have a choice of strategies for handling queries that need to join non-collocated data.

For tables have many-to-many relationships, choose one of the following strategies to handle queries that need to join non-colocated data:
Use parallel, data-aware procedures to run the logic for the query on the member that stores some or all of the data (to minimize data distribution hops). The procedure should execute multiple queries and then join the results using application code. 

Split the query into multiple queries, and perform the join in the application client code.