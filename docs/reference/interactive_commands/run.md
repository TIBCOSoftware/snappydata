# run

Treats the value of the string as a valid file name, and redirects `snappy` processing to read from that file until it ends or an exit command is executed.

## Syntax

```pre
RUN String
```

## Description

Treats the value of the string as a valid file name, and redirects `snappy` processing to read from that file until it ends or an [Exit](exit.md) command is executed. If the end of the file is reached without `snappy` exiting, reading continues from the previous input source once the end of the file is reached. Files can contain Run commands.

`snappy` prints out the statements in the file as it executes them.

Any changes made to the `snappy` environment by the file are visible in the environment when processing resumes.

## Example

```pre
snappy> run 'ToursDB_schema.sql';
```


