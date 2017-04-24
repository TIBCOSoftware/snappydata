# max_view_log_size

## Description

Configures the maximum size, in bytes, of a locator's membership view log, which can be used to verify that your product usage is compliance with your SnappyData license. (See <mark> TO BE CONFIRMED RowStore link [Product Usage Logging](http://rowstore.docs.snappydata.io/docs/manage_guide/Topics/membership-logging.html#concept_077AEA19CC404F5DAA38D33F21A737D0).) </mark> The minimum file size is 1000000 bytes. If you specify a smaller value, SnappyData uses a value of 1000000 instead.

After the maximum file size is reached, the locator deletes the log file and begins recording messages in a new file.

## Default Value

5000000

## Property Type

system

## Prefix

n/a
