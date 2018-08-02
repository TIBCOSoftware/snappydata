# exit
Completes the `snappy` application and halts processing.

## Syntax

``` pre
EXIT
```

## Description

Causes the `snappy` application to complete and processing to halt. Issuing this command from within a file started with the [Run](run.md) command or on the command line causes the outermost input loop to halt.

`snappy` exits when the Exit command is entered or if given a command file on the Java invocation line, when the end of the command file is reached.

## Example


``` pre
snappy> DISCONNECT CONNECTION1;
snappy> EXIT;
```
