# jmx-manager-password-file

## Description

By default the JMX Manager allows clients without credentials to connect. If this property is set to the name of a file, only clients that connect with credentials that match an entry in this file will be allowed. Most JVMs require that the file is only readable by the owner. For more information about the format of this file see Oracle's documentation of the com.sun.management.jmxremote.password.file system property. Ignored if jmx-manager is false or if jmx-manager-port is zero. See <mark> [Setting Up JMX Manager Authentication](http://rowstore.docs.snappydata.io/docs/manage_guide/jmx/management_system_jmx_authentication.html#topic_06B28974C3D34C019418C92B1FC189C8)</mark>.

## Default Value

*not set*

## Property Type

connection (boot)

## Prefix

n/a
