# Collecting logs, stats and dumps using the collect-debug-artifacts script

This section uses the term 'node' frequently. A node denotes a server or a locator member when a purely SnappyData system is there. In a SnappyData distributed system a node can mean server, locator or lead member.

The script **collect-debug-artifacts** enables you to collect the debug information like logs and stats. It also has an option to dump stacks of the running system. Details of all the options and capabilities of the script can be found below. The main purpose of this is to ease the collection of these information. The script collects all the artifacts node wise and outputs a tar file which contains member wise information.

Pre-requisites for running the script:

The script assumes certain conditions to be fulfilled before it is invoked. Please ensure that these requirements are fulfilled because the script does not validate these.
The conditions are:

1. This script is expected to be run by a user who has read and write permissions on the output directories of all the SnappyData nodes.

2. The user should have one way passwordless ssh setup from one machine to the other machines where the SnappyData nodes are running.

Below is the usage of the script

```pre

      <linux-shell> ./sbin/collect-debug-artifacts.sh -h

Usage: collect-debug-artifacts
       [ -c conffile|--conf=conffile|--config=conffile ]
       [ -o resultdir|--out=resultdir|--outdir=resultdir ]
       [ -h|--help ]
       [ -a|--all ]
       [ -d|--dump ]
       [ -v|--verbose ]
       [ -s starttimestamp|--start=starttimestamp ]
       [ -e endtimestamp|--end=endtimestamp ]
       [ -x debugtarfile|--extract=debugtarfile ]

       Timestamp format: YYYY-MM-DD HH:MM[:SS]
```

Options:

  All the options of the script are optional. By default the script tries to get the current logs. All the logs starting from the last restart and the last file before that. It also brings all the stat file in the output directory. However if you want to change this behavior of the script you can use the following options to collect the debug information as per your requirements. Please note that no stack dumps are collected by default. You need to use the '-d, --dump' option to get the stack dumps.

  -h, --help
  Prints a usage message summary briefly summarizing the command line options

  -c, --conf 
  The script uses a configration file which has three configuration elements.
  1. MEMBERS_FILE -- This is a text file which has member information. Each line has the host machine name followed by the full path to the run directory of the member. This file is generated automatically when the sbin/start-all-scripts.sh is used.
  2. NO_OF_STACK_DUMPS -- This parameter tells the script that how many stack dumps will be attempted per member/node of the running system.
  3. INTERVAL_BETWEEN_DUMPS -- The amount of time in seconds the script waits between registering stack dumps.

  -o, --out, --outdir
  The directory where the output file in the form of tar, will be created.

  -a, --all
  With '-a or --all' option all the logs and stats file are collected from each members output directory.

  -d, --dump
  Stack dumps are not collected by default or with -a, --all option. The user need to explicitly provide this argument if the stack dumps need to be collected.

  -v, --verbose
  verbose mode is on.

  -s, --start
  The script can also be asked to collect log files for specified time interval. The time interval can be specified using the start time and an end time parameter. Both the parameter needs to be specified. The format in which the time stamp can be specified is 'YYYY-MM-DD HH:MM[:SS]'

  -x, --extract=debugtarfile 
  To extract the contents of the tar file.

           Timestamp format: YYYY-MM-DD HH:MM[:SS]

