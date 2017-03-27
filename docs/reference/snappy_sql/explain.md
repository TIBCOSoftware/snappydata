#Explain

```
EXPLAIN [EXTENDED | CODEGEN] statement
```

Provide detailed plan information about the given statement without actually running it. By default this only outputs information about the physical plan. Explaining `DESCRIBE TABLE` is not currently supported.

**EXTENDED**
Also output information about the logical plan before and after analysis and optimization.

**CODEGEN**
Output the generated code for the statement, if any.