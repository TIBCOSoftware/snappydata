# run

Treats the value of the string as a valid file name, and redirects `snappy-shell` processing to read from that file until it ends or an exit command is executed.

##Syntax

``` pre
RUN String
```

<a id="rtoolsijcomref28886__section_E7447AA805DB44358E25A9E840BC8704"></a>
##Description

Treats the value of the string as a valid file name, and redirects `snappy-shell` processing to read from that file until it ends or an <a href="exit.html#rtoolsijcomref33358" class="xref" title="Completes the gfxd application and halts processing.">Exit</a> command is executed. If the end of the file is reached without `snappy-shell` exiting, reading continues from the previous input source once the end of the file is reached. Files can contain Run commands.

`snappy-shell` prints out the statements in the file as it executes them.

Any changes made to the `snappy-shell` environment by the file are visible in the environment when processing resumes.

##Example

``` pre
snappy(PEERCLIENT)> run 'ToursDB_schema.sql';
```


