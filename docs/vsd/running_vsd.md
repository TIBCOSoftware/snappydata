# Installing and Running VSD

Start the VSD tool, load statistics files, and maintain the view you want on your statistics.

<a id="install-vsd"></a>
## Install VSD

VSD is a free analysis tool and is provided as-is. VSD is distributed with SnappyData. To install VSD, install SnappyData. See [Installing SnappyData](running_vsd.md) for instructions.

After you install SnappyData, you can find VSD in the following location of your installation:

```pre
<snappydata-installdir>/vsd
```

Where *snappydata-installdir* corresponds to the location where SnappyData is installed. 

The VSD tool installation has the following subdirectories:

-   **bin**. The scripts and binaries that can be used to run VSD on a variety of operating systems. The following scripts are included with this release:
    -   vsd

    -   vsd.bat

    The following binaries are included with this release:
    -   vsdwishLinux - for Linux

-   **lib**. The jars and binary libraries needed to run VSD

<a id="start-vsd"></a>

## Start VSD

-   **Linux/Unix, MacOS or Other OS:**

    ```pre 
    $ ./vsd
    ```

<a id="statistics-vsd"></a>

## Load a Statistics File into VSD

You have several options for loading a statistics file into VSD:

-   Include the name of one or more statistics files on the VSD command line. Example:

    ```pre
    vsd <filename.gfs> ...
    ```

-   Browse for an existing statistics file through **Main** > **Load Data** File.
-   Type the full path in the **Directories** field, in the **Files** field select the file name, and then press **OK** to load the data file.
-   Switch to a statistics file that you’ve already loaded by clicking the down-arrow next to the Files field.

After you load the data file, the VSD main window displays a list of entities for which statistics are available. VSD uses color to distinguish between entities that are still running (shown in green) and those that have stopped (shown in black).

<a id="current-view-datafile"></a>
## Maintain a Current View of the Data File

If you select the menu item **File** > **Auto Update**, VSD automatically updates your display, and any associated charts, whenever the data file changes. Alternatively, you can choose **File** > **Update** periodically to update the display manually.

<a id="about-statistics"></a>

## About Statistics

The statistics values (Y-axis) are plotted over time (X-axis). This makes it easy to see how statistics are trending, and to correlate different statistics.

Some statistics are cumulative from when the SnappyData system was started. Other statistics are instantaneous values that may change in any way between sample collection.

Cumulative statistics are best charted per second or per sample, so that the VSD chart is readable. Absolute values are best charted as No Filter.
