# Configuring High Availability for a Partitioned Table

Configure in-memory high availability for your partitioned table. Set other high-availability options, like redundancy zones and redundancy recovery strategies.

## Set the Number of Redundant Copies

Configure in-memory high availability for your partitioned table by specifying the number of secondary copies you want to maintain REDUNDANCY clause of [CREATE TABLE](../reference/sql_reference/create-table.md#redundancy).

## Configure Redundancy Zones for Members
Group members into redundancy zones so that TIBCO ComputeDB places redundant data copies in different zones.

Understand how to set a member's **gemfirexd.properties** settings. [See Configuration Properties](../reference/configuration_parameters/config_parameters.md).
Group the datastore members that host partitioned tables into redundancy zones by using the setting [redundancy-zone](../reference/configuration_parameters/redundancy-zone.md).

For example, if you had redundancy set to 1, so you have one primary and one secondary copy of each data entry, you could split primary and secondary data copies between two machine racks by defining one redundancy zone for each rack. To do this, you set this zone in the **gemfirexd.properties** file for all members that run on one rack:
```pre
redundancy-zone=rack1
```
You would set this zone in **gemfirexd.properties** for all members on the other rack:
```pre
redundancy-zone=rack2
```
Each secondary copy would be hosted on the rack opposite the rack where its primary copy is hosted.

## Set Enforce Unique Host

Configure TIBCO ComputeDB to use only unique physical machines for redundant copies of partitioned table data.

Understand how to set a member's **gemfirexd.properties** settings. See [Configuration Properties](../reference/configuration_parameters/config_parameters.md).

Configure your members so TIBCO ComputeDB always uses different physical machines for redundant copies of partitioned table data using the setting [enforce-unique-host](../reference/configuration_parameters/enforce-unique-host.md#enforce-unique-host). The default for this setting is **false**.

Example:
```pre
enforce-unique-host=true
```
