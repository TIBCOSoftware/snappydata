# Specifying Encrypted Passwords in Conf Files or in Client Connections

<ent>This feature is available only in the Enterprise version of SnappyData </br></ent>

SnappyData allows you to specify encrypted passwords, if you do not want to specify plain text passwords, in conf files or in JDBC/ODBC client connections URLs, while launching the SnappyData cluster in a secure mode. SnappyData provides an utility script called **snappy-encrypt-password.sh** to generate encrypted passwords of system users who launch the cluster.  This script is located under **sbin** directory of SnappyData product installation. 

You can generate encrypted passwords before starting the SnappyData cluster and use it in **conf** files of the SnappyData servers, locators and leads. 

You can also generate encrypted passwords for JDBC/ODBC client connections when SnappyData cluster is already started. For that case, using the **SYS.ENCRYPT_PASSWORD** system procedure is a preferable approach as mentioned in the following note.

This script accepts list of users as input, prompts for each user’s password and outputs the encrypted password on the console. A special user AUTH_LDAP_SEARCH_PW can be used to generate encrypted password for the LDAP search user, used for looking up the DN indicated by configuration parameter **-J-Dgemfirexd.auth-ldap-search-pw **in SnappyData **conf** files.

!!!Note

	*	Make sure that SnappyData system is not running when this script is run, because this script launches a locator using the parameters specified in user’s conf/locators file. The script connects to the existing cluster to generate the password if locators could not be started. However, this may not work if the user is a LDAP search user who does not have permissions to access Snappydata cluster. It is recommended to directly connect to cluster and then run the SQL `CALL SYS.ENCRYPT_PASSWORD('<user>', '<password>', 'AES', 0)`,  if encrypted password is to be generated after you start the cluster.
	*	The encrypted secret that is returned is specific to this particular SnappyData distributed system, because the system uses a unique private key to generate the secret. An obfuscated version of the private key is stored in the persistent data dictionary (SnappyData catalog). If  the existing data dictionary is ever deleted and recreated, then you must generate and use a new encrypted secret for use with the new distributed system. Also the encrypted password cannot be used in any other SnappyData installation, even if the user and password is the same. You need to generate the encrypted password separately for every other SnappyData installation.

## Script Usage 

	snappy-encrypt-password.sh <user1> <user2> ...
  
## Example Output

In the example output shown, **snappy-encrypt-password.sh** script is invoked for users **user1** and **AUTH_LDAP_SEARCH_PW** (special user used to indicate LDAP search user, used for looking up the DN). The script outputs encrypted password for **user1** and **AUTH_LDAP_SEARCH_PW**.


```pre
$ ./sbin/snappy-encrypt-password.sh user1 AUTH_LDAP_SEARCH_PW
Enter password for user1: user123 (atual password is not shown on the console)
Re-enter password for user1: user123 (atual password is not shown on the console)
Enter password for AUTH_LDAP_SEARCH_PW: admin123 (atual password is not shown on the console)
Re-enter password for AUTH_LDAP_SEARCH_PW: admin123 (atual password is not shown on the console)
Logs generated in /home/xyz/<snappydata_install_dir>/work/localhost-locator-1/snappylocator.log
SnappyData Locator pid: 2379 status: running
  Distributed system now has 1 members.
  Started DRDA server on: localhost/127.0.0.1[1527]
SnappyData version 1.2.0 
snappy> Using CONNECTION0
snappy> ENCRYPTED_PASSWORD                                                                                                              
--------------------------------------------------------------------------------------------------------------------------------
user1 = v13b607k2j611b8584423b2ea584c970fefd041f77f                                                                             

1 row selected
snappy> ENCRYPTED_PASSWORD                                                                                                              
--------------------------------------------------------------------------------------------------------------------------------
AUTH_LDAP_SEARCH_PW = v13b607k2j65384028e5090a8990e2ce17d43da3de9                                                               

1 row selected
snappy> 
The SnappyData Locator on 172.16.62.1(localhost-locator-1) has stopped.

```
## Startup Configuration Examples
You can then use the above encrypted password in the configuration of servers, locators and leads.  You can either edit the **conf** files or use the environment variables for startup options of locators, servers, and leads.

### Configuring by Editing conf Files
Following is an example of the **conf/locators** file where instead of plain text, encrypted passwords are used.  Similar change must be done to **conf/servers** and **conf/leads** files.

```
$ cat conf/locators
localhost -auth-provider=LDAP -J-Dgemfirexd.auth-ldap-server=ldap://192.168.1.162:389/ -user=user1 -password=v13b607k2j611b8584423b2ea584c970fefd041f77f -J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-pw=v13b607k2j65384028e5090a8990e2ce17d43da3de9

```
You can then start the SnappyData system.

### Configuring with Environment Variables for Startup Options 

An alternate way to specify the security options is by using the environment variable for  **LOCATOR_STARTUP_OPTIONS**, **SERVER_STARTUP_OPTIONS** and **LEAD_STARTUP_OPTIONS** for locators, servers, and leads respectively. These startup variables can be specified in **conf/spark-env.sh** file. This file is sourced when starting the SnappyData system. A template file (**conf/spark-env.sh.template**) is provided in the **conf** directory for reference. You can copy this file and use it to configure security options. 

For example:

```
# create a spark-env.sh from the template file
$cp conf/spark-env.sh.template conf/spark-env.sh 

# edit the conf/spark-env.sh file to add security configuration as shown below

SECURITY_ARGS="-auth-provider=LDAP -J-Dgemfirexd.auth-ldap-server=ldap://192.168.1.162:389/ -user=user1 -password=v13b607k2j637b2ae24e60be46613391117b7f234d0 -J-Dgemfirexd.auth-ldap-search-base=cn=sales-group,ou=sales,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-dn=cn=admin,dc=example,dc=com -J-Dgemfirexd.auth-ldap-search-pw=v13b607k2j6174404428eee3374411d97d1d497d3b8"

LOCATOR_STARTUP_OPTIONS=”$SECURITY_ARGS”
SERVER_STARTUP_OPTIONS=”$SECURITY_ARGS”
LEAD_STARTUP_OPTIONS=”$SECURITY_ARGS”

```

## Using Encrypted Password in Client Connections

You can generate encrypted password for use in JDBC/ODBC client connections by executing a system procedure with a statement such as `CALL SYS.ENCRYPT_PASSWORD('<user>', '<password>', 'AES', 0)`. 

For example: 

```
snappy> call sys.encrypt_password('user2', 'user2123', 'AES', 0);
ENCRYPTED_PASSWORD                                                                                                              
--------------------------------------------------------------------------------------------------------------------------------
user2 = v13b607k2j6c519cc88605e5e8fa778f46fb8b2b610                                                                             

1 row selected

```
   
This procedure accepts user id and plain text password as input arguments and outputs the encrypted password. 

!!!Note
	In the current release of SnappyData, the last two parameters should be “AES” and “0”. 

The encrypted password can be used in JDBC/ODBC client connections.  

For example using snappy shell:

```
snappy> connect client 'localhost:1527;user=user2;password=v13b607k2j6c519cc88605e5e8fa778f46fb8b2b610';
Using CONNECTION1

```
