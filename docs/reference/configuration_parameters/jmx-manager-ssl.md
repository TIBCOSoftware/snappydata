# jmx-manager-ssl

## Description

If true and `jmx-manager-port` is not zero, the JMX Manager accepts only SSL connections. The ssl-enabled property does not apply to the JMX Manager, but the other SSL properties do. This allows SSL to be configured for just the JMX Manager without needing to configure it for the other RowStore connections. Ignored if `jmx-manager` is false.

## Default Value

false

## Property Type

connection (boot)

## Prefix

n/a
