# Experimental Features

SnappyData 1.3.1 provides the following features on an experimental basis. These features are included only for testing purposes and are not yet supported officially:

## Authorization for External Tables
You can enable authorization of external tables by setting the system property **CHECK_EXTERNAL_TABLE_AUTHZ** to true when the cluster's security is enabled.
System admin or the schema owner can grant or revoke the permissions on external tables to other users. 
For example: `GRANT ALL ON <external-table> to <user>;`
