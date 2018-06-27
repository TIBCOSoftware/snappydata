# Collecting System Statistics

Enable SnappyData system statistics using a system procedure, member boot properties, or connection properties.

You can enable statistics collection per-member using the boot properties:

-   [statistic-sampling-enabled](../reference/configuration_parameters/statistic-sampling-enabled.md)

-   [enable-time-statistics](../reference/configuration_parameters/enable-time-statistics.md)

-   [statistic-archive-file](../reference/configuration_parameters/statistic-archive-file.md)

!!! Note
	You must include the [statistic-archive-file](../reference/configuration_parameters/statistic-archive-file.md) and specify a valid file name in order to enable statistics collection.</p>
These boot properties help you configure a member's statistic archive file location and size:

-   [archive-disk-space-limit](../reference/configuration_parameters/archive-disk-space-limit.md)

-   [archive-file-size-limit](../reference/configuration_parameters/archive-file-size-limit.md)

-   [statistic-sample-rate](../reference/configuration_parameters/statistic-sample-rate.md)

To collect statement-level statistics and time-based, statement-level statistics for a specific connection (rather than globally or per-member), use these connection properties with a peer client connection:

-   [enable-stats](../reference/configuration_parameters/enable-stats.md)

-   [enable-timestats](../reference/configuration_parameters/enable-timestats.md)

-   [statistic-archive-file](../reference/configuration_parameters/statistic-archive-file.md)

These properties can only be used with a peer client connect.

!!! Note
	- Because of the overhead required for taking many timestamps, it is recommended that you enable time-based statistics only during testing and debugging. 

	- Use statement-level statistics only when the number of individual statements is small, such as when using prepared statements. SnappyData creates a separate statistics instance for each individual statement. With a large number of these statements, it can be difficult to load archives into VSD and navigate to those statistics of interest. 
