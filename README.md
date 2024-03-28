# IbmiOdbcDataAccess -IBM i/AS400 ODBC Data Access Class
This C# project contains a sample class library that can be generated for using the IBM i Access ODBC Driver in a .Net or .Net Core project. The project compiles to .Net Standard 2.0 which is compatible back to .Net and .Net Core 2.1.

You could also just lift the classes and put into your own source code rather than creating a separate class library. It's up to you.

Rather than worrying about writing ODBC data access code, you can focus on your business logic.

## Obtaining the IBM i Access Client Solutions ODBC Driver
The IBM i Access ODBC drivers can be downloaded from the following IBM site as long as you have an account to log in to the IBM download site.  

https://www.ibm.com/support/pages/ibm-i-access-client-solutions  

There is an ODBC driver available that runs on Windows, Linux, MacOS and there is also a native IBM i ODBC driver available.  

## Sample IBM i ODBC Connection String
In order to use this connection string you must already have the IBM i Access ODBC Driver installed on your Windows, Linux or Mac computer.

The sample C# connection string variable below connects to a system with IP address: ```1.1.1.1``` User: ```user1``` Password: ```pass1```
```
String _conn = "Driver={IBM i Access ODBC Driver};System=1.1.1.1;Uid=user1;Pwd=pass1;CommitMode=0;EXTCOLINFO=1;";
```
## Sample C# Console Code to Run Query and Do Simple Iteration of Results
```
using IbmiOdbcDataAccess;
using System.Data;
using System.Data.Common;

// Connect to system using only syste, user and password instead of connection string
var rtnconn = _ibmi.OpenConnection("1.1.1.1", "user1", "pass1");

// Execute SQL query to results DataTable
var _dtable1 = _ibmi.ExecuteQueryToDataTable ("SELECT * FROM QIWS.QCUSTCDT");

// Iterate and output desired columns to console
foreach(DataRow _row in _dtable1.Rows)
{
    Console.WriteLine($"Cusnum:{_row["CUSNUM"]} Lastname:{_row["LSTNAM"]} Init:{_row["INIT"]}");
}

```
## Sample IBM i Connection Strings
https://www.connectionstrings.com/ibm-i-access-odbc-driver/

