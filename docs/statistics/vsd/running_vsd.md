# Installing and Running VSD

Start the VSD tool, load statistics files, and maintain the view you want on your statistics.

<a id="install-vsd"></a>
## Install VSD

!!! Note

	Visual Statistics Display (VSD) is a third-party tool and is not shipped with SnappyData.
    It is available from [GemTalk Systems](https://gemtalksystems.com/products/vsd/) or
    [Pivotal GemFire](https://network.pivotal.io/products/pivotal-gemfire) under their own respective licenses.

You can open the VSD tool by running `bin/vsd` in the installation packages from above.

The VSD tool installation has the following subdirectories:

-   **bin**. The scripts/binaries that can be used to run VSD

-   **lib**. The jars and binary libraries needed to run VSD

<a id="start-vsd"></a>

## Start VSD

-   **Linux/Unix, MacOS:**

    ```pre
    $ ./bin/vsd
    ```

-   **Windows OS:**

    ```pre
    Run the vsd executable from command prompt or otherwise.

    > .\bin\vsd
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
