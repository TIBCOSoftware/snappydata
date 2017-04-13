#Overview

#ODBC Driver Supported Configurations
This topic lists ODBC driver configurations supported by SnappyData.

SnappyData provides support for the Progress DataDirect ODBC driver version 1.7. The following table lists the platforms, architectures and driver managers that the driver supports.

| Operating System | Driver Manager | Driver Download| Character Types
|--------|--------|--------|--------|
|RHEL 4, 5, 6 or 7 </br> CentOS 4, 5, 6, or 7</br> SUSE Linux Enterprise Server 10.x, and 11 </br>(32-bit and 64-bit)|Progress DataDirect v1.7|        |Unicode, ANSII|
|Windows Server 2003 Service Pack 2 and higher</br> Windows Server 2008</br> Windows Server 2012</br> Windows 7</br> Windows 8.x</br> Windows Vista </br>Windows XP Service Pack 2 and higher </br>(32-bit and 64-bit)|Progress DataDirect v1.7|        |Unicode, ANSII|

##Installing and Configuring the Progress DataDirect ODBC Driver
This topic describes how to install and configure the Progress DataDirect ODBC driver on Linux and Windows platforms.

The Progress DataDirect ODBC driver binaries are distributed for Linux- and Windows-based platforms. Binary installers are available for both 32-bit and 64-bit architectures.

Installing the Progress DataDirect ODBC Driver for Linux
Installing the Progress DataDirect ODBC Driver for Windows
Configuring the Progress DataDirect ODBC Driver


###Installing the Progress DataDirect ODBC Driver for Linux
1. Download and unzip the SnappyData ODBC Distribution 1.4.1. For example:
 
 ```
 $ tar -zxvf SnappyData_RowStore_XX_bNNNNN_ODBC.tar.gz -C path_to_install_location <mark>Verify File Name </mark>
 ```
 where path_to_install_location is an already existing directory.
2. Change to the /linux subdirectory of the ODBC distribution:
 
 ```
 $ cd path_to_install_location/SnappyData_RowStore_XX_bNNNNN_ODBC/linux <mark>Verify Location </mark>
 ```
3. Display the contents of the readme.txt file, and note the IPE key of the architecture (32-bit or 64-bit) of your linux system. You will need to enter the correct IPE key during the installation. For example:
 
 ```
 $ cat readme.txt
 [...]
 For OEM installations you may be asked for an IPE key. Please use these keys
 for the appropriate platform. 

 Linux:
 1076719616 SnappyData All 32-bit Platforms
 1076719872 SnappyData All 64-bit Platforms
 [...]
```

4. Change to the /32 or /64 subdirectory based on your architecture, and execute the ODBC installer application. For example:
 
 ```
 $ cd 64
 $ ./PROGRESS_DATADIRECT_ODBC_7.1_LINUX_64_INSTALL.bin
 ```
 The installer loads and displays the Introduction screen:

5. Click **Next** to display the license agreement.

6. Read and accept the license, then click **Next** to display the installation directory.

7. Choose a directory in which to install the driver, or accept the default, then click **Next**.

8. On the Install Type screen, select OEM/Licensed **Installation**, then click **Next**.

9. Enter the IPE key that you recorded in Step 3, then click Verify. The installer then shows the products available for installation.

10. Select **Drivers**, then click **Next** to show the Pre-Installation Summary.

11. Click **Install** to install the drivers into the selected directory.

12. After the installation completes, click **Done** to exit the installer.

###Installing the Progress DataDirect ODBC Driver for Windows
1. Download and unzip the SnappyData ODBC Distribution 1.4.1.<mark>Verify Version Number</mark>
2. Change to the \windows\32 or \windows\64 subdirectory of the ODBC distribution, depending on the architecture of your operating system (32-bit or 64-bit). For example:
 ```
 c:\> cd path_to_install_location\SnappyData_RowStore_XX_bNNNNN_ODBC\windows<mark>Verify File Name </mark>
 ```
3. Execute the ODBC Installer executable in the directory. For example:
 ```
 c:\SnappyData_RowStore_XX_bNNNNN_ODBC\windows\64> PROGRESS_DATADIRECT_ODBC_7.1_WIN_64_INSTALL.EXE<mark>Verify File Name </mark>
 ```
 The installer loads and displays the Introduction screen:

4. Click Next to display the license agreement.

5. Read and accept the license, then click **Next** to display the installation directory:

6. Choose a directory in which to install the driver, or accept the default, then click **Next**.

7. On the Install Type screen, select **OEM/Licensed Installation**, then click **Next**.

8. Enter the IPE key that you recorded in Step 3, then click Verify. The installer then shows the products available for installation:

9. Select **Drivers**, then click **Next**.
 The installer displays the Create Default Data Source screen:

10. Select **Create Default Data Source** if you want to update the registry entry and set the Progress DataDirect ODBC driver for Gemfire XD as your default. Click **Next** to show the Pre-Installation Summary.

11. Click **Install** to install the drivers into the selected directory.

12. After the installation completes, click **Done** to exit the installer.

###Configuring the Progress DataDirect ODBC Driver
Refer to the installed ODBC driver documentation for detailed instructions about configuring and using the driver:

 * The primary documentation for the ODBC driver is installed in path_to_install_location/help/RowStoreHelp/index.html<mark>To Be Confirmed </mark>
* The Quick Start Connect section (path_to_install_location/help/RowStoreHelp/RowStoreHelp/snappystore/quick-start-connect.html) explains how to configure and test the newly-installed driver.<mark>To Be Confirmed </mark>
