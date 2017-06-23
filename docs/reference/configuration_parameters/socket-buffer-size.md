# socket-buffer-size

## Description

Receive buffer sizes in bytes of the TCP/IP connections used for data transmission. To minimize the buffer size allocation needed for distributing large, serializable messages, the messages are sent in chunks. This setting determines the size of the chunks. Larger buffers can handle large messages more quickly, but take up more memory.

## Default Value

32768

## Property Type

connection (boot)

## Prefix

gemfire.
