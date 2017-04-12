# max_view_log_size

## Description

Configures the maximum size, in bytes, of a locator's membership view log, which can be used to verify that your product usage is compliance with your SnappyData license. (See <a href="../../manage_guide/Topics/membership-logging.md#concept_077AEA19CC404F5DAA38D33F21A737D0" class="xref" title="Each SnappyData locator creates a log file that record the different membership views of the SnappyData distributed system. You can use the contents of these log files to verify that your product usage is compliance with your SnappyData license.">Product Usage Logging</a>.) The minimum file size is 1000000 bytes. If you specify a smaller value, SnappyData uses a value of 1000000 instead.

After the maximum file size is reached, the locator deletes the log file and begins recording messages in a new file.

## Default Value

5000000

## Property Type

system

## Prefix

n/a
