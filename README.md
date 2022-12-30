# IbmiOdbcDataAccess -IBM i/AS400 ODBC Data Access Class
This C# project contains a sample class library that can be generated for using the IBM i Access ODBC Driver in a .Net or .Net Core project. 

## Obtaining the IBM i Access Client Solutions ODBC Driver
The IBM i Access ODBC drivers can be downloaded from the following IBM site as long as you have an account to log in to the IBM download site.  

https://www.ibm.com/support/pages/ibm-i-access-client-solutions  

There is an ODBC driver available that runs on Windows, Linux, MacOS and there is also a native IBM i ODBC driver available.  

## Sample IBM i ODBC Connection String
In order to use this connection string you must already have the IBM i Access ODBC Driver installed on your Windows, Linux or Mac computer.

The sample C# connection string variable below connects to a system with IP address: ```1.1.1.1``` User: ```user1``` Password: ```pass1```
```
String _conn = "Driver={IBM i Access ODBC Driver};System=1.1.1.1;Uid=user1;Pwd=pass1;CommitMode=0;EXTCOLINFO=1;CommitMode=0;EXTCOLINFO=1";
```

## Sample IBM i Connection Strings
https://www.connectionstrings.com/ibm-i-access-odbc-driver/
