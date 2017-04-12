# jmx-manager-http-port

## Description

If non-zero, when started the JMX Manager will also start an embedded Web server and will listen on this port. The Web server is used to host the RowStore Pulse Web application. If you are hosting the Pulse web app in your own Web server, then disable this embedded server by setting this property to zero. Ignored if jmx-manager is false.

## Default Value

7070

## Property Type

connection (boot)

## Prefix

n/a
