# jmx-manager-access-file

## Description

By default the JMX Manager allows full access to all MBeans by any client. If this property is set to the name of a file, then it can restrict clients to only reading MBeans; they cannot modify MBeans. The access level can be configured differently in this file for each user name defined in the password file. For more information about the format of this file see Oracle's documentation of the `com.sun.management.jmxremote.access.file` system property. Ignored if `jmx-manager` is false or if `jmx-manager-port` is zero. See <a href="../../manage_guide/jmx/management_system_jmx_authentication.md#topic_06B28974C3D34C019418C92B1FC189C8" class="xref" title="To force JMX clients to authenticate into the RowStore management system, you must configure authentication for the JMX Manager node.">Setting Up JMX Manager Authentication</a>.

## Default Value

*not set*

## Property Type

connection (boot)

## Prefix

n/a
