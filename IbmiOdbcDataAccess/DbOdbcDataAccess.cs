using System;
using System.Text;
using System.Data;
using System.IO;
using System.Data.Odbc;
using System.Data.Common;


namespace IbmiOdbcDataAccess
{
    /// <summary>
    /// IBM i Access ODBC Data Access
    /// This class contains a general ODBC data class wrapper and is meant to simplify ODBC work with IBM i data.
    /// The class can also be inherited and extended from a business object.
    /// Extending and inheriting is a better strategy than modifying the core IbmiOdbcDataAccess class object.
    /// </summary>
    /// <remarks></remarks>
    public class DbOdbcDataAccess
    {
        // Made these class variables public so class
        // that is using this as a base class can use these variables too
        private string _lastError;
        private string _connectionString = "";
        private DataTable _dtTable;
        private int _iDtRows;
        private int _iDtColumns;
        private OdbcDataReader _dtReader;
        private OdbcConnection _conn;
        private OdbcCommand _cmd;
        private bool _bConnectionOpen = false;
        private int _iLastExportCount;
        private string _lastSql;
        private string _ibmiaccessconntemplate="Driver={IBM i Access ODBC Driver};System=@@SYSTEM;Uid=@@USERID;Pwd=@@PASS;CommitMode=0;EXTCOLINFO=1";


        /// <summary>
        /// Get internal OdbcConnection object.
        /// </summary>
        /// <returns>Return OdbcConnection object</returns>
        public OdbcConnection GetOdbcConnection()
        {
            return _conn;
        }

        /// <summary>
        /// Get internal DataReader object.
        /// </summary>
        /// <returns>Return DataReader object</returns>
        public OdbcDataReader GetInternalDataReader()
        {
            return _dtReader;
        }

        /// <summary>
        /// Get internal DataTable object.
        /// </summary>
        /// <returns>Return DataTable object</returns>
        public DataTable GetInternalDataTable()
        {
            return _dtTable;
        }

        /// <summary>
        /// Get last error.
        /// </summary>
        /// <returns>Error info from last call if set</returns>
        public string GetLastError()
        {
            return _lastError;
        }

        /// <summary>
        /// Get last SQL query.
        /// </summary>
        /// <returns>Return last SQL statement executed if set</returns>
        public string GetLastSql()
        {
            return _lastSql;
        }

        /// <summary>
        /// Set IBM i access connection string default template if you want to override the default.
        /// Keywords that can be passed as part of the template:
        /// @@SYSTEM - IBMi system host name or IP address
        /// @@USER - IBMi user id
        /// @@PASS - IBMi password
        /// You don't need to use this method unless you want to use the OpenConnection method and pass just
        /// the system/host, user id and password instead of the entire string and want to overrider the default 
        /// IBM i connection string.
        /// Default connection string template which is pre-set:
        /// "Driver={IBM i Access ODBC Driver};System=@@SYSTEM;Uid=@@USERID;Pwd=@@PASS;CommitMode=0;EXTCOLINFO=1"
        /// </summary>
        /// <param name="strConnStringTemplate"></param>
        public void SetIbmiConnectionStringTemplate(string strConnStringTemplate)
        {
            try
            {
                _lastError = "";
                _ibmiaccessconntemplate = strConnStringTemplate;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
            }
        }

        /// <summary>
        /// Set general ODBC connection string.
        /// </summary>
        /// <param name="strConnString"></param>
        public void SetConnectionString(string strConnString)
        {
            try
            {
                _lastError = "";
                _connectionString = strConnString;
            }
            catch (Exception ex)
            {
                _connectionString = "";
                _lastError = ex.Message;
            }
        }

        /// <summary>
        /// Open database connection without passing explicit connection string.
        /// If no connection string passed, SetConnectionString must be called beforehand.
        /// to set connection string info.
        /// </summary>
        /// <returns>True=Connection opened successfully. False=Error occurred opening connection.</returns>
        public bool OpenConnection()
        {
            // Call open connection with no connection string
            return OpenConnection("");
        }

        /// <summary>
        /// Return connection status.
        /// </summary>
        /// <returns>True=Connection is open. False=Connection is not open.</returns>
        public bool IsConnected()
        {
            return _bConnectionOpen;
        }

        /// <summary>
        /// Open database connection with set connection string.
        /// </summary>
        /// <returns>True=Connection opened successfully. False=Error occurred opening connection.</returns>
        public bool OpenConnection(string strConnString)
        {
            try
            {
                _lastError = "";

                // If connection string passed, set it.
                // Otherwise use what is set already via
                // SetConnectionString
                if (strConnString.Trim() != "")
                    _connectionString = strConnString;

                // Bail if no connection string was pre-set with SetConnectionString method.
                if (_connectionString.Trim() == "")
                    throw new Exception("No database connection string has been set.");

                // Create the connection
                _conn = new OdbcConnection();

                // Set the connection string now
                _conn.ConnectionString = _connectionString;

                // Now open the connection
                _conn.Open();

                _bConnectionOpen = true;

                _lastError = "Connection opened successfully.";

                return true;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                _bConnectionOpen = false;
                return false;
            }
        }

        /// <summary>
        /// Open database connection with system name/ip, user id, password using 
        /// IBM i Access connection string template instead of passing entire connection string.
        /// You can override the default IBM i connection string template via the SetIbmiConnectionStringTemplate()
        /// method. 
        /// </summary>
        /// <returns>True=Connection opened successfully. False=Error occurred opening connection.</returns>
        public bool OpenConnection(string systemHost,string userId, string password)
        {

            // Set temp connection string
            string _tempConn = _ibmiaccessconntemplate;

            try
            {
                _lastError = "";

                // If host, user, password passed in, 
                // SetConnectionString
                if (systemHost.Trim() == "")
                    throw new Exception("System name/host ip address is required.");

                if (userId.Trim() == "")
                    throw new Exception("User id is required.");

                if (password.Trim() == "")
                    throw new Exception("Password is required.");

                // Build connection string
                _tempConn = _tempConn.Replace("@@SYSTEM", systemHost.Trim());
                _tempConn = _tempConn.Replace("@@USERID", userId.Trim());
                _tempConn = _tempConn.Replace("@@PASS", password.Trim());

                // Set connection string based on IBM i connection string teamplte
                _connectionString = _tempConn;

                // Open the connection now
                return OpenConnection(_connectionString);

            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                _bConnectionOpen = false;
                return false;
            }
        }

        /// <summary>
        /// Close database connection
        /// </summary>
        /// <returns>True=Connection closed successfully. False=Error occurred closing connection.</returns>
        public bool CloseConnection()
        {
            try
            {
                _lastError = "";

                if (_conn == null == false)
                {
                    _conn.Close();
                    _conn = null;
                }

                _lastError = "Connection closed successfully.";

                _bConnectionOpen = false;

                return true;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                _bConnectionOpen = false;
                return false;
            }
        }
        /// <summary>
        /// Run SQL query and return as DataTable object.
        /// This function takes an SQL SELECT statement and connection string and 
        /// runs the query to get the data we want to work with.
        /// </summary>
        /// <param name="sqlselect">SQL query</param>
        /// <param name="iStartRecord">Starting record. Default=0. If start and max are 0, all records will be exported to DataTable.</param>
        /// <param name="iMaxRecords">Ending record. Default = 0. If start and max are 0, all records will be exported to DataTable.</param>
        /// <param name="tablename">DataTable name</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <returns>DataTable or null</returns>
        public DataTable ExecuteQueryToDataTable(string sqlselect, int iStartRecord = 0, int iMaxRecords = 0, string tableName = "Table1", int queryTimeout = -1)
        {
            try
            {
                _lastError = "";
                _dtTable = null;
                _iDtRows = 0;
                _iDtColumns = 0;

                // Check for active connection
                if (IsConnected() == false)
                    throw new Exception("Database connection not open.");

                // Bail if not a SELECT
                if (!sqlselect.ToUpper().StartsWith("SELECT"))
                    throw new Exception("Only SELECT queries can be run.");

                // Save last SQL property
                _lastSql = sqlselect;

                // Create temporary SQL Server DataAdapter using SQL Server connection string and SQL statement
                using (OdbcDataAdapter adapter = new OdbcDataAdapter(sqlselect, _conn))
                {

                    // Set query timeout if specified. 0=no timeout
                    if (queryTimeout >= 0)
                    {
                        adapter.SelectCommand.CommandTimeout = queryTimeout;
                    }

                    // Fill a DataTable using the DataAdapter
                    DataTable dtWork = new DataTable();

                    // If limits passed, limit records returned
                    if (iStartRecord == 0 & iMaxRecords == 0)
                    {
                        adapter.Fill(dtWork);
                    }
                    else
                    {
                        adapter.Fill(iStartRecord, iMaxRecords, dtWork);
                    }

                    // Dispose of DataAdapter when we're done
                    adapter.Dispose();

                    // Return the recordset to class level DataTable so we can access indefinitely
                    _dtTable = dtWork;
                    _dtTable.TableName = tableName;

                    // Set row/col info
                    _iDtRows = _dtTable.Rows.Count;
                    _iDtColumns = _dtTable.Columns.Count;

                    return _dtTable; // Return DataTable
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return nothing.
                return null;
            }
        }

        /// <summary>
        /// Run SQL query and return as internal DataTable.
        /// This function takes an SQL SELECT statement and connection string and 
        /// runs the query to get the data we want to work with.
        /// </summary>
        /// <param name="sqlselect">SQL query</param>
        /// <param name="iStartRecord">Starting record. Default=0. If start and max are 0, all records will be exported to DataTable.</param>
        /// <param name="iMaxRecords">Ending record. Default = 0. If start and max are 0, all records will be exported to DataTable.</param>
        /// <param name="tablename">DataTable name</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <returns>Boolean for query completion</returns>
        public bool ExecuteQueryToDataTableInternal(string sqlselect, int iStartRecord = 0, int iMaxRecords = 0, string tableName = "Table1", int queryTimeout = -1)
        {
            try
            {
                _lastError = "";

                _dtTable = null;
                _iDtRows = 0;
                _iDtColumns = 0;

                // Check for active connection
                if (IsConnected() == false)
                    throw new Exception("Database connection not open.");

                // Bail if not a SELECT
                if (!sqlselect.ToUpper().StartsWith("SELECT"))
                    throw new Exception("Only SELECT queries can be run.");

                // Save last SQL property
                _lastSql = sqlselect;

                // Create temporary SQL Server data adapter using SQL Server connection string and SQL statement
                using (OdbcDataAdapter adapter = new OdbcDataAdapter(sqlselect, _conn))
                {

                    // Set query timeout if specified. 0=no timeout
                    if (queryTimeout >= 0)
                    {
                        adapter.SelectCommand.CommandTimeout = queryTimeout;
                    }

                    // Fill a DataTable using the DataAdapter
                    DataTable dtWork = new DataTable();

                    // If limits passed, limit records returned
                    if (iStartRecord == 0 & iMaxRecords == 0)
                    {
                        adapter.Fill(dtWork);
                    }
                    else
                    {
                        adapter.Fill(iStartRecord, iMaxRecords, dtWork);
                    }

                    // Dispose of DataAdapter when we're done
                    adapter.Dispose();

                    // Return the recordset to class level DataTable so we can access indefinitely
                    _dtTable = dtWork;
                    _dtTable.TableName = tableName;

                    // Set row/col info
                    _iDtRows = _dtTable.Rows.Count;
                    _iDtColumns = _dtTable.Columns.Count;

                    return true; // Return DataTable
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return nothing.
                return false;
            }
        }

        /// <summary>
        /// Get internal DataTable contents to delimited string.
        /// </summary>
        /// <param name="delim">Field delimiter. Default=|</param>
        /// <param name="replace">True=replace output file is it exists. False=Dont replace. Default=False</param>
        /// <param name="removeLineFeeds">True=replace CRLF, LF and CR with placeholders values of <CRLF>, <LF> or <CR>. False=Don't replace linefeeds in data.</param>
        /// <param name="doubleQuotes">Output double quotes. True - output quotes, False-No quotes. Default=False</param>
        /// <param name="outputHeadings">Output column headings. True - output headings, False-No headings. Default=True</param>
        /// <returns>True=Success,False=Errors</returns>
        public string GetRecordsToDelimStringInternal(string delim = ",", bool replace = false, bool removeLineFeeds = true, bool doubleQuotes = true, bool spaceBeforeDelim = true, bool outputHeadings = true)
        {
            StringBuilder sb = new StringBuilder();
            StringBuilder sbHdr = new StringBuilder();
            StringBuilder sbDtl = new StringBuilder();

            //string sql = "";
            string sWorkSpace = "";
            bool bOutputFileExists = false;
            string dblqt = "";

            try
            {
                _lastError = "";
                _iLastExportCount = 0;

                // If double quotes, set char
                if (doubleQuotes)
                {
                    dblqt = "\"";
                }

                // Set space before delim
                if (spaceBeforeDelim)
                    sWorkSpace = " ";
                else
                    sWorkSpace = "";

                // Verify that DataTable has data
                if (_dtTable == null)
                    throw new Exception("DataTable has no data. Export cancelled.");

                // Get first record so we can extract field names in query result
                int count = 0;

                // Output headings only if enabled and output file not found already
                if (outputHeadings & bOutputFileExists == false)
                {

                    // Extract all the local filed names
                    for (int j = 0; j <= _dtTable.Columns.Count - 1; j++)
                    {
                        if (count == _dtTable.Columns.Count - 1)
                        {
                            if (removeLineFeeds)
                                sbHdr.Append(_dtTable.Columns[j].ColumnName.Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>"));
                            else
                                sbHdr.Append(_dtTable.Columns[j].ColumnName.Trim());
                        }
                        else if (removeLineFeeds)
                            sbHdr.Append(_dtTable.Columns[j].ColumnName.Trim().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + sWorkSpace + delim);
                        else
                            sbHdr.Append(_dtTable.Columns[j].ColumnName.Trim() + sWorkSpace + delim);
                        count += 1;
                    }
                    // Output new line after record is output
                    sbHdr.AppendLine("");
                }

                // Process all the records to delimited string buffer
                // Replace CRLF, CR and LF values with placeholders.
                foreach (DataRow dr in _dtTable.Rows)
                {
                    // Extract all field data
                    count = 0;
                    for (int j = 0; j <= _dtTable.Columns.Count - 1; j++)
                    {
                        if (count == _dtTable.Columns.Count - 1)
                        {
                            if (removeLineFeeds)
                                sbDtl.Append(dblqt + dr[j].ToString().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + dblqt);
                            else
                                sbDtl.Append(dblqt + dr[j].ToString() + dblqt);
                        }
                        else if (removeLineFeeds)
                            sbDtl.Append(dblqt + dr[j].ToString().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + dblqt + sWorkSpace + delim);
                        else
                            sbDtl.Append(dblqt + dr[j].ToString() + dblqt + " " + delim);
                        count += 1;
                    }
                    // Output new line after record is output
                    sbDtl.AppendLine("");
                }

                // Return all text
                return sbHdr.ToString() + sbDtl.ToString();
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "error";
            }
        }
        /// <summary>
        /// Export internal DataTable contents to delimited file.
        /// If file exists and replace not selected, data will be appended 
        /// to existing file without any additional column headings.
        /// </summary>
        /// <param name="outputfile">Output file</param>
        /// <param name="delim">Field delimiter. Default=|</param>
        /// <param name="replace">True=replace output file is it exists. False=Dont replace. Default=False</param>
        /// <param name="removeLineFeeds">True=replace CRLF, LF and CR with placeholders values of <CRLF>, <LF> or <CR>. False=Don't replace linefeeds in data.</param>
        /// <param name="doubleQuotes">Output double quotes. True - output quotes, False-No quotes. Default=False</param>
        /// <param name="outputHeadings">Output column headings. True - output headings, False-No headings. Default=True</param>
        /// <returns>True=Success,False=Errors</returns>
        public bool ExportRecordsToDelimFileDt(string outputFile, string delim = ",", bool replace = false, bool removeLineFeeds = true, bool doubleQuotes = true, bool spaceBeforeDelim = true, bool outputHeadings = true)
        {
            StringBuilder sb = new StringBuilder();
            StringBuilder sbHdr = new StringBuilder();
            StringBuilder sbDtl = new StringBuilder();

            //string sql = "";
            string sWorkSpace = "";
            bool bOutputFileExists = false;
            string dblqt = "";

            try
            {
                _lastError = "";
                _iLastExportCount = 0;

                // If double quotes, set char
                if (doubleQuotes)
                {
                    dblqt = "\"";
                }

                // Set space before delim
                if (spaceBeforeDelim)
                    sWorkSpace = " ";
                else
                    sWorkSpace = "";

                // Make sure output file specified
                if (outputFile.Trim() == "")
                    throw new Exception("Output file must be specified.");

                // Verify that DataTable has data
                if (_dtTable == null)
                    throw new Exception("DataTable has no data. Export cancelled.");

                // If file exists and replace not selected bail
                if (System.IO.File.Exists(outputFile))
                {
                    bOutputFileExists = true;
                    if (replace == true)
                    {
                        System.IO.File.Delete(outputFile);
                        bOutputFileExists = false;
                    }
                    else
                    {
                    }
                }

                // Get first record so we can extract field names in query result
                int count = 0;

                // Output headings only if enabled and output file not found already
                if (outputHeadings & bOutputFileExists == false)
                {

                    // Extract all the local filed names
                    for (int j = 0; j <= _dtTable.Columns.Count - 1; j++)
                    {
                        if (count == _dtTable.Columns.Count - 1)
                        {
                            if (removeLineFeeds)
                                sbHdr.Append(_dtTable.Columns[j].ColumnName.Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>"));
                            else
                                sbHdr.Append(_dtTable.Columns[j].ColumnName.Trim());
                        }
                        else if (removeLineFeeds)
                            sbHdr.Append(_dtTable.Columns[j].ColumnName.Trim().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + sWorkSpace + delim);
                        else
                            sbHdr.Append(_dtTable.Columns[j].ColumnName.Trim() + sWorkSpace + delim);
                        count += 1;
                    }
                    // Output new line after record is output
                    sbHdr.AppendLine("");
                }

                // Process all the records to delimited string buffer
                // Replace CRLF, CR and LF values with placeholders.
                foreach (DataRow dr in _dtTable.Rows)
                {
                    // Extract all field data
                    count = 0;
                    for (int j = 0; j <= _dtTable.Columns.Count - 1; j++)
                    {
                        if (count == _dtTable.Columns.Count - 1)
                        {
                            if (removeLineFeeds)
                                sbDtl.Append(dblqt + dr[j].ToString().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + dblqt);
                            else
                                sbDtl.Append(dblqt + dr[j].ToString() + dblqt);
                        }
                        else if (removeLineFeeds)
                            sbDtl.Append(dblqt + dr[j].ToString().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + dblqt + sWorkSpace + delim);
                        else
                            sbDtl.Append(dblqt + dr[j].ToString() + dblqt + " " + delim);
                        count += 1;
                    }
                    // Output new line after record is output
                    sbDtl.AppendLine("");
                }

                // Append all text to file. That way if already exists, we can append new data if selected
                System.IO.File.AppendAllText(outputFile, sbHdr.ToString() + sbDtl.ToString(), Encoding.UTF8);

                // Set completion
                _iLastExportCount = _dtTable.Rows.Count;
                _lastError = _dtTable.Rows.Count + " rows were exported to delimited file " + outputFile;

                return true;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }
        /// <summary>
        /// Export internal DataReader contents to delimited file. 
        /// If file exists and replace not selected, data will be appended 
        /// to existing file without any additional column headings.
        /// </summary>
        /// <param name="outputfile">Output file</param>
        /// <param name="delim">Field delimiter. Default=|</param>
        /// <param name="replace">True=replace output file is it exists. False=Dont replace. Default=False</param>
        /// <param name="removeLineFeeds">True=replace CRLF, LF and CR with placeholders values of <CRLF>, <LF> or <CR>. False=Don't replace linefeeds in data.</param>
        /// <param name="doubleQuotes">Output double quotes. True - output quotes, False-No quotes. Default=False</param>
        /// <param name="outputHeadings">Output column headings. True - output headings, False-No headings. Default=True</param>
        /// <returns>True=Success,False=Errors</returns>
        public bool ExportRecordsToDelimFileDr(string outputFile, string delim = ",", bool replace = false, bool removeLineFeeds = true, bool doubleQuotes = true, bool spaceBeforeDelim = true, bool outputHeadings = true)
        {
            StringBuilder sb = new StringBuilder();
            StringBuilder sbHdr = new StringBuilder();
            StringBuilder sbDtl = new StringBuilder();

            //string sql = "";
            string sWorkSpace = "";
            int rowcount = 0;
            bool bOutputFileExists = false;

            try
            {
                _lastError = "";
                _iLastExportCount = 0;

                // Set space before delim
                if (spaceBeforeDelim)
                    sWorkSpace = " ";
                else
                    sWorkSpace = "";

                // Make sure output file specified
                if (outputFile.Trim() == "")
                    throw new Exception("Output file must be specified.");

                // Verify that DataTable has data
                if (_dtReader == null)
                    throw new Exception("Data Reader has no data. Export cancelled.");

                // If file exists and replace not selected bail
                if (System.IO.File.Exists(outputFile))
                {
                    bOutputFileExists = true;
                    if (replace == true)
                    {
                        System.IO.File.Delete(outputFile);
                        bOutputFileExists = false;
                    }
                    else
                    {
                    }
                }

                // Get first record so we can extract field names in query result
                int count = 0;

                // Output headings only if enabled and output file not found already
                if (outputHeadings & bOutputFileExists == false)
                {

                    // Extract all the local field names
                    for (int j = 0; j <= _dtReader.FieldCount - 1; j++)
                    {
                        if (count == _dtReader.FieldCount - 1)
                        {
                            if (removeLineFeeds)
                                sbHdr.Append(_dtReader.GetName(j).Trim().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>"));
                            else
                                sbHdr.Append(_dtReader.GetName(j).Trim());
                        }
                        else if (removeLineFeeds)
                            sbHdr.Append(_dtReader.GetName(j).Trim().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + sWorkSpace + delim);
                        else
                            sbHdr.Append(_dtReader.GetName(j).Trim() + sWorkSpace + delim);
                        count += 1;
                    }
                    // Output new line after record is output
                    sbHdr.AppendLine("");
                }

                // Process all the records to delimited string buffer
                // Replace CRLF, CR and LF values with placeholders.
                while (_dtReader.Read())
                {
                    // Extract all field data
                    count = 0;
                    for (int j = 0; j <= _dtReader.FieldCount - 1; j++)
                    {
                        if (count == _dtReader.FieldCount - 1)
                        {
                            if (removeLineFeeds)
                                sbDtl.Append(_dtReader.GetValue(j).ToString().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>"));
                            else
                                sbDtl.Append(_dtReader.GetValue(j).ToString());
                        }
                        else if (removeLineFeeds)
                            sbDtl.Append(_dtReader.GetValue(j).ToString().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + sWorkSpace + delim);
                        else
                            sbDtl.Append(_dtReader.GetValue(j).ToString() + sWorkSpace + delim);
                        count += 1;
                    }
                    // Output new line after record is output
                    sbDtl.AppendLine("");
                    // Increment row counter
                    rowcount += 1;
                }

                // Append all text to file. That way if already exists, we can append new data if selected
                System.IO.File.AppendAllText(outputFile, sbHdr.ToString() + sbDtl.ToString(), Encoding.UTF8);

                // Set completion
                _iLastExportCount = rowcount;
                _lastError = rowcount + " rows were exported to delimited file " + outputFile;

                return true;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Query Table and Export Internal DataReader contents to delimited file.
        /// If file exists and replace not selected, data will be appended 
        /// to existing file without any additional column headings.
        /// </summary>
        /// <param name="sqlselect">SQL select</param>
        /// <param name="outputfile">Output file</param>
        /// <param name="delim">Field delimiter. Default=|</param>
        /// <param name="replace">True=replace output file is it exists. False=Dont replace. Default=False</param>
        /// <param name="removeLineFeeds">True=replace CRLF, LF and CR with placeholders values of <CRLF>, <LF> or <CR>. False=Don't replace linefeeds in data.</param>
        /// <param name="doubleQuotes">Output double quotes. True - output quotes, False-No quotes. Default=False</param>
        /// <param name="outputHeadings">Output column headings. True - output headings, False-No headings. Default=True</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <returns>True=Success,False=Errors</returns>
        public bool QueryRecordsToDelimFileDr(string sqlselect, string outputFile, string delim = ",", bool replace = false, bool removeLineFeeds = true, bool doubleQuotes = true, bool spaceBeforeDelim = true, bool outputHeadings = true, int queryTimeout = -1)
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataReader. 
                rtnquery = ExecuteQueryToDataReaderInternal(sqlselect, queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Now export the DataReader results to delimited file
                return ExportRecordsToDelimFileDr(outputFile, delim, replace, removeLineFeeds, doubleQuotes, spaceBeforeDelim, outputHeadings);
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Query Table and Export Internal DataTable contents to delimited file.
        /// If file exists and replace not selected, data will be appended 
        /// to existing file without any additional column headings.
        /// </summary>
        /// <param name="sqlselect">SQL select</param>
        /// <param name="outputfile">Output file</param>
        /// <param name="delim">Field delimiter. Default=|</param>
        /// <param name="replace">True=replace output file is it exists. False=Dont replace. Default=False</param>
        /// <param name="removeLineFeeds">True=replace CRLF, LF and CR with placeholders values of <CRLF>, <LF> or <CR>. False=Don't replace linefeeds in data.</param>
        /// <param name="doubleQuotes">Output double quotes. True - output quotes, False-No quotes. Default=False</param>
        /// <param name="outputHeadings">Output column headings. True - output headings, False-No headings. Default=True</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <returns>True=Success,False=Errors</returns>
        public bool QueryRecordsToDelimFileDt(string sqlselect, string outputFile, string delim = ",", bool replace = false, bool removeLineFeeds = true, bool doubleQuotes = true, bool spaceBeforeDelim = true, bool outputHeadings = true, int queryTimeout = -1)
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataTable. 
                rtnquery = ExecuteQueryToDataTableInternal(sqlselect, 0, 0, "Table1", queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Now export the DataTable results to delimited file
                return ExportRecordsToDelimFileDt(outputFile, delim, replace, removeLineFeeds, doubleQuotes, spaceBeforeDelim, outputHeadings);
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Query Table and Export Internal DataTable contents to delimited string.
        /// </summary>
        /// <param name="sqlselect">SQL select</param>
        /// <param name="delim">Field delimiter. Default=|</param>
        /// <param name="removeLineFeeds">True=replace CRLF, LF and CR with placeholders values of <CRLF>, <LF> or <CR>. False=Don't replace linefeeds in data.</param>
        /// <param name="doubleQuotes">Output double quotes. True - output quotes, False-No quotes. Default=False</param>
        /// <param name="outputHeadings">Output column headings. True - output headings, False-No headings. Default=True</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <returns>Query results string or blanks</returns>
        public string QueryRecordsToDelimStringDt(string sqlselect, string delim = ",", bool removeLineFeeds = true, bool doubleQuotes = true, bool spaceBeforeDelim = true, bool outputHeadings = true, int queryTimeout = -1)
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataTable. 
                rtnquery = ExecuteQueryToDataTableInternal(sqlselect, 0, 0, "Table1", queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Now export the DataTable results to delimited string
                return GetQueryResultsDataTableToCsvString(delim);
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "";
            }
        }

        /// <summary>
        /// Query Table and Export Internal DataTable contents to JSON file.
        /// If file exists and replace not selected, data will be appended 
        /// to existing file without any additional column headings.
        /// </summary>
        /// <param name="sqlselect">SQL select</param>
        /// <param name="outputfile">Output file</param>
        /// <param name="replace">True=replace output file is it exists. False=Dont replace. Default=False</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <param name="tableName">DataTable name to use. Default = "Table1"</param>
        /// <returns>True=Success,False=Errors</returns>
        public bool QueryRecordsToJsonFileDt(string sqlselect, string outputfile, bool replace = false, int queryTimeout = -1,string tableName="Table1")
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataTable. 
                rtnquery = ExecuteQueryToDataTableInternal(sqlselect, 0, 0, tableName, queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Now export the DataTable results to JSON file
                return GetQueryResultsDataTableToJsonFile(outputfile,replace);

            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Query Table and Export Internal DataTable contents to JSON string.
        /// </summary>
        /// <param name="sqlselect">SQL select</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <param name="tableName">DataTable name to use. Default = "Table1"</param>
        /// <returns>Query result as JSON string or blanks</returns>
        public string QueryRecordsToJsonStringDt(string sqlselect,int queryTimeout = -1, string tableName = "Table1")
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataTable. 
                rtnquery = ExecuteQueryToDataTableInternal(sqlselect, 0, 0, tableName, queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Now export the DataTable results to JSON string
                return GetQueryResultsDataTableToJsonString();

            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "";
            }
        }

        /// <summary>
        /// Query Table and Export Internal DataReader contents to JSON file.
        /// If file exists and replace not selected, data will be appended 
        /// to existing file without any additional column headings.
        /// </summary>
        /// <param name="sqlselect">SQL select</param>
        /// <param name="outputfile">Output file</param>
        /// <param name="replace">True=replace output file is it exists. False=Dont replace. Default=False</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <param name="tableName">DataTable name to use. Default = "Table1"</param>
        /// <returns>True=Success,False=Errors</returns>
        public bool QueryRecordsToJsonFileDr(string sqlselect, string outputfile, bool replace = false, int queryTimeout = -1,string tableName="Table1")
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataReader. 
                rtnquery = ExecuteQueryToDataReaderInternal(sqlselect, queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Convert the internal DataReader to internal DataTable
                _dtTable = ConvertDataReaderToDataTable(_dtReader);

                // Now export the internal DataTable results to JSON file
                return GetQueryResultsDataTableToJsonFile(outputfile, replace);
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Query Table and Export Internal DataTable contents to XML file.
        /// If file exists and replace not selected, data will be appended 
        /// to existing file without any additional column headings.
        /// </summary>
        /// <param name="sqlselect">SQL select</param>
        /// <param name="outputfile">Output file</param>
        /// <param name="replace">True=replace output file is it exists. False=Dont replace. Default=False</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        ///  <param name="writeSchema">Write XML schema in return data</param>
        ///  <param name="tableName">DataTable name to use. Default = "Table1"</param>
        /// <returns>True=Success,False=Errors</returns>
        public bool QueryRecordsToXmlFileDt(string sqlselect, string outputfile, bool replace = false, int queryTimeout = -1,bool writeSchema = false,string tableName="Table1")
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataTable. 
                rtnquery = ExecuteQueryToDataTableInternal(sqlselect, 0, 0, tableName, queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Now export the internal DataTable results to XML file
                return GetQueryResultsDataTableToXmlFile(outputfile, tableName, writeSchema, replace);


            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Query Table and Export Internal DataTable contents to XML string.
        /// </summary>
        /// <param name="sqlselect">SQL select</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        ///  <param name="writeSchema">Write XML schema in return data</param>
        ///  <param name="tableName">DataTable name to use. Default = "Table1"</param>
        /// <returns>True=Success,False=Errors</returns>
        public string QueryRecordsToXmlStringDt(string sqlselect, int queryTimeout = -1, bool writeSchema = false, string tableName = "Table1")
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataTable. 
                rtnquery = ExecuteQueryToDataTableInternal(sqlselect, 0, 0, tableName, queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Now export the internal DataTable results to XML string
                return GetQueryResultsDataTableToXmlString(tableName, writeSchema);

            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "";
            }
        }

        /// <summary>
        /// Query Table and Export Internal DataReader contents to XML file.
        /// If file exists and replace not selected, data will be appended 
        /// to existing file without any additional column headings.
        /// </summary>
        /// <param name="sqlselect">SQL select</param>
        /// <param name="outputfile">Output file</param>
        /// <param name="replace">True=replace output file is it exists. False=Dont replace. Default=False</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        ///  <param name="writeSchema">Write XML schema in return data</param>
        ///  <param name="tableName">DataTable name to use. Default = "Table1"</param>
        /// <returns>True=Success,False=Errors</returns>
        public bool QueryRecordsToXmlFileDr(string sqlselect, string outputfile, bool replace = false, int queryTimeout = -1,bool writeSchema=false, string tableName = "Table1")
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataReader. 
                rtnquery = ExecuteQueryToDataReaderInternal(sqlselect, queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Convert the internal DataReader to internal DataTable
                _dtTable = ConvertDataReaderToDataTable(_dtReader);

                // Now export the internal DataTable results to XML file
                return GetQueryResultsDataTableToXmlFile(outputfile,tableName,writeSchema, replace);

            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }

        }

        /// <summary>
        /// Get last export record count
        /// </summary>
        /// <returns></returns>
        public int GetLastExportCount()
        {
            return _iLastExportCount;
        }

        /// <summary>
        /// Run SQL query and return as an internal DataReader object.
        /// This allows us to iterate the Data Reader from a VB or VB Scripting environment.
        /// This function takes an SQL SELECT statement and connection string and 
        /// runs the query to get the data we want to work with.
        /// </summary>
        /// <param name="sqlselect"></param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <returns>Boolean for query completion</returns>
        public bool ExecuteQueryToDataReaderInternal(string sqlselect, int queryTimeout = -1)
        {
            try
            {
                _lastError = "";

                _dtTable = null;
                _dtReader = null;
                _iDtRows = 0;
                _iDtColumns = 0;

                // Check for active connection
                if (IsConnected() == false)
                    throw new Exception("Database connection not open.");

                // Bail if not a SELECT
                if (!sqlselect.ToUpper().StartsWith("SELECT"))
                    throw new Exception("Only SELECT queries can be run.");

                // Create command and run query to data reader
                _cmd = _conn.CreateCommand();

                // Set query timeout if specified. 0=no timeout
                if (queryTimeout >= 0)
                {
                    _cmd.CommandTimeout = queryTimeout;
                }

                // Save last SQL property
                _lastSql = sqlselect;

                // Set SQL
                _cmd.CommandText = sqlselect;
                // Get the data reader so we can process one record at a time
                _dtReader = _cmd.ExecuteReader();

                return true;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return nothing.
                return false;
            }
        }

        /// <summary>
        /// Run SQL query and return as an internal DataReader object.
        /// This allows us to iterate the Data Reader from a VB or VB Scripting environment.
        /// This function takes an SQL SELECT statement and connection string and 
        /// runs the query to get the data we want to work with.
        /// </summary>
        /// <param name="sqlselect"></param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <returns>DataReader or null if errors</returns>
        public OdbcDataReader ExecuteQueryToDataReader(string sqlselect, int queryTimeout = -1)
        {
            try
            {
                _lastError = "";

                _dtTable = null;
                _dtReader = null;
                _iDtRows = 0;
                _iDtColumns = 0;

                // Check for active connection
                if (IsConnected() == false)
                    throw new Exception("Database connection not open.");

                // Bail if not a SELECT
                if (!sqlselect.ToUpper().StartsWith("SELECT"))
                    throw new Exception("Only SELECT queries can be run.");

                // Create command and run query to data reader
                _cmd = _conn.CreateCommand();

                // Set query timeout if specified. 0=no timeout
                if (queryTimeout >= 0)
                {
                    _cmd.CommandTimeout = queryTimeout;
                }

                // Save last SQL property
                _lastSql = sqlselect;

                // Set SQL
                _cmd.CommandText = sqlselect;

                // Get the data reader so we can process one record at a time
                return _cmd.ExecuteReader();
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return nothing.
                return null;
            }
        }

        /// <summary>
        /// Get Internal DataTable object reference. Must be populated using ExecuteQueryDataTableInternal.
        /// </summary>
        /// <returns>DataTable object or null on errors</returns>
        public DataTable GetDataTableInternal()
        {
            try
            {
                _lastError = "";

                return _dtTable;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return null.
                return null;
            }
        }
        /// <summary>
        /// Get Internal DataReader object reference. Must be populated using ExecuteQueryDataReaderInternal.
        /// </summary>
        /// <returns>DataReader object or null on errors</returns>
        public OdbcDataReader GetDataReaderInternal()
        {
            try
            {
                _lastError = "";

                return _dtReader;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return null.
                return null;
            }
        }
        /// <summary>
        /// Get Internal Data Connection reference. 
        /// </summary>
        /// <returns>DataReader object or null on errors</returns>
        public OdbcConnection GetDataConnection()
        {
            try
            {
                _lastError = "";

                return _conn;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return null.
                return null;
            }
        }
        /// <summary>
        /// Get next row from internal DataReader. 
        /// </summary>
        /// <returns>True-Next record read to internal reader, False-No more records read or error. </returns>
        public bool GetNextRowDrInternal()
        {
            try
            {
                _lastError = "";

                return _dtReader.Read();
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return nothing.
                return false;
            }
        }
        /// <summary>
        /// Close DataReader
        /// </summary>
        /// <returns>True-Internal DataReader closed. False-Internal DataReader did not close or error occurred.</returns>
        public bool CloseDataReaderInternal()
        {
            try
            {
                _lastError = "";

                if (_dtReader != null)
                {
                    _dtReader.Close();
                }
                _dtReader = null;

                return true;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return fale
                return false;
            }
        }
        /// <summary>
        /// Get Field from DataReader based on ordinal column position
        /// </summary>
        /// <returns></returns>
        public string GetColValueByPosDr(int iCol)
        {
            try
            {
                _lastError = "";

                // get selected column number as tring
                return _dtReader.GetValue(iCol).ToString();
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return blanks
                return "";
            }
        }
        /// <summary>
        /// Return current row from DataReader as delimited record.
        /// </summary>
        /// <param name="sDelim">Field delimiter. Default = ,</param>
        /// <returns></returns>
        public string GetRowDelimDr(string sDelim = ",")
        {
            try
            {
                string swork = "";
                int iCurCol = 0;

                _lastError = "";

                // Build delimited data from current DataReader row
                for (iCurCol = 0; iCurCol <= _dtReader.FieldCount - 1; iCurCol++)
                    swork = swork + _dtReader.GetValue(iCurCol).ToString() + sDelim;

                // Trim last delimiter at tail end of string
                if (swork.Length > 0)
                    swork = swork.Remove(swork.Length - 1);

                return swork;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "";
            }
        }
        /// <summary>
        /// Return internal DataReader field/column count.
        /// Same as GetFieldCountDR()
        /// </summary>
        /// <returns>Field/column count from internal DataReader results.</returns>
        public int GetColCountDr()
        {
            return _dtReader.FieldCount;
        }

        /// <summary>
        /// Return internal DataReader field/column count.
        /// Same as GetColCountDR()
        /// </summary>
        /// <returns>Field/column count from internal DataReader results.</returns>
        public int GetFieldCountDr()
        {
            return _dtReader.FieldCount;
        }

        /// <summary>
        /// Return internal DataTable row count.
        /// </summary>
        /// <returns>Row count from internal DataTable</returns>
        public int GetRowCountDt()
        {
            return _iDtRows;
        }

        /// <summary>
        /// Return internal DataTable column count.
        /// </summary>
        /// <returns>Column count from internal DataTable</returns>
        public int GetColCountDt()
        {
            return _iDtColumns;
        }
        /// <summary>
        /// Return internal DataTable column value for specified row/column.
        /// </summary>
        /// <param name="iRow">DataTable row number</param>
        /// <param name="iCol">DataTable column number</param>
        /// <returns>Return row/col value from internal DataTable as string</returns>
        public string GetRowValueByPosDt(int iRow, int iCol)
        {
            try
            {
                string swork = "";
                _lastError = "";
                //swork = _dTable.Rows[iRow].Item[iCol].ToString;
                // TODO - Test this after conv to C#
                swork = _dtTable.Rows[iRow][iCol].ToString();
                return swork;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "**ERROR";
            }
        }

        /// <summary>
        /// Return internal DataTable column for specified row based on column field name.
        /// </summary>
        /// <param name="iRow">DataTable row number</param>
        /// <param name="sColName">DataTable column name</param>
        /// <returns>Return row/col value from internal DataTable as string or **ERROR if any errors occurred</returns>
        public string GetRowValueByNameDt(int iRow, string sColName)
        {
            try
            {
                string swork = "";
                _lastError = "";
                // TODO - test this after conversion to C#
                swork = _dtTable.Rows[iRow][sColName].ToString();
                return swork;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "**ERROR";
            }
        }
        /// <summary>
        /// Return internal DataReader column for current row based on column field name.
        /// This is a convenience function because the Data Reader needs ordinal positions
        /// when returning field data.
        /// </summary>
        /// <param name="sColName">Column field name</param>
        /// <returns>Return col value from current data row as string or **ERROR if any errors occurred</returns>
        public string GetColValueByNameDr(string sColName)
        {
            try
            {
                string swork = "";
                int iCol;

                _lastError = "";

                // Get ordinal if field exists
                iCol = GetColPosByNameDr(sColName.Trim());
                // Return field value for ordinal
                swork = _dtReader.GetValue(iCol).ToString();

                return swork;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "**ERROR";
            }
        }
        /// <summary>
        /// Return column names for current internal DataTable in delimited record.
        /// </summary>
        /// <param name="sDelim">Field delimiter. Default = ,</param>
        /// <returns>Delimited string of field names.</returns>
        public string GetColNamesDt(string sDelim = ",")
        {
            try
            {
                string swork = "";

                _lastError = "";

                // Build delimited column name list
                foreach (DataColumn col in _dtTable.Columns)
                    swork = swork + col.ColumnName + sDelim;

                // Trim last delimiter at tail end of string
                if (swork.Length > 0)
                    swork = swork.Remove(swork.Length - 1);

                return swork;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "";
            }
        }
        /// <summary>
        /// Return column names for current internal DataReader in delimited record.
        /// </summary>
        /// <param name="sDelim">Field delimiter. Default = ,</param>
        /// <returns>Delimited string of field names.</returns>
        public string GetColNamesDr(string sDelim = ",")
        {
            try
            {
                string swork = "";
                int iCount = 0;

                _lastError = "";

                // Build delimited column name list
                for (iCount = 0; iCount <= _dtReader.FieldCount - 1; iCount++)
                    swork = swork + _dtReader.GetName(iCount) + sDelim;

                // Trim last delimiter at tail end of string
                if (swork.Length > 0)
                    swork = swork.Remove(swork.Length - 1);

                return swork;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "";
            }
        }
        /// <summary>
        /// Return internal DataReader column ordinal position based on name.
        /// </summary>
        /// <param name="sFieldName">Column field name</param>
        /// <returns>Column position or -2 if errors or not found</returns>
        public int GetColPosByNameDr(string sFieldName)
        {
            try
            {
                //string swork = "";
                int iCount = 0;

                _lastError = "";

                // See if field is found and return ordinal 
                for (iCount = 0; iCount <= _dtReader.FieldCount - 1; iCount++)
                {
                    if (sFieldName.ToLower().Trim() == _dtReader.GetName(iCount).ToLower().Trim())
                        return iCount;
                }

                // No fields found, throw an error so we can return 
                throw new Exception(sFieldName + " was not found in the field list.");
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return -2;
            }
        }
        /// <summary>
        /// Return internal DataTable current row as delimited record.
        /// </summary>
        /// <param name="iRow">DataTable row</param>
        /// <param name="sDelim">Field delimiter. Default = ,</param>
        /// <returns>Delimited string of data</returns>
        public string GetRowDelimDt(int iRow, string sDelim = ",")
        {
            try
            {
                string swork = "";

                _lastError = "";

                // Build delimited data from current row
                foreach (DataColumn col in _dtTable.Columns)
                    swork = swork + _dtTable.Rows[iRow][col.ColumnName] + sDelim;

                // Trim last delimiter at tail end of string
                if (swork.Length > 0)
                    swork = swork.Remove(swork.Length - 1);

                return swork;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "";
            }
        }

        /// <summary>
        /// Run SQL Insert, Update, Delete or Other Command With no Resultset.
        /// This function takes an SQL INSERT, UPDATE or DELETE statement and 
        /// connection string and runs the SQL command to update or 
        /// delete the data we want to work with.
        /// </summary>
        /// <param name="sqlCommand">SQL action command</param>
        /// <param name="commandTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds, Otherwise set specific timeout in seconds.</param>
        /// <param name="allowSelectQueries">True-Allow SELECT queries. False-Do not allow select queries. Default=False</param>
        /// <returns>Records affected or -2 on errors.</returns>
        public int ExecuteCommandNonQuery(string sqlCommand, int commandTimeout = -1,bool allowSelectQueries=false)
        {
            try
            {
                _lastError = "";

                // Check for active connection
                if (IsConnected() == false)
                    throw new Exception("Database connection not open.");

                // Bail if a SELECT and select not allowed
                if (!allowSelectQueries) { 
                    if (sqlCommand.ToUpper().StartsWith("SELECT"))
                        throw new Exception("SELECT queries are not allowed here.");
                }

                // Save last SQL property
                _lastSql = sqlCommand;

                // Run SQL command
                // create connection and command
                using (OdbcCommand cmd = new OdbcCommand(sqlCommand, _conn))
                {

                    // Set query timeout if specified. 0=no timeout
                    if (commandTimeout >= 0)
                    {
                        cmd.CommandTimeout = commandTimeout;
                    }

                    // Run the command now
                    int iRtnCmd;
                    iRtnCmd = cmd.ExecuteNonQuery();

                    return iRtnCmd;
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;

                // If errors occur, return -2
                return -2;
            }
        }

        /// <summary>
        /// Drop selected table based on SCHEMALIB.TABLENAME.
        /// </summary>
        /// <param name="tableschema">Table library/schema for table to drop.</param>
        /// <param name="tablename">Table name to drop.</param>
        /// <returns>True-Table dropped. False-Table not dropped or error occured.</returns>
        public bool DropTable(string tableschema, string tablename)
        {

            int iRtnCmd = 0;
            string query = "";

            try
            {

                _lastError = "";

                // Check for active connection
                if (IsConnected() == false)
                    throw new Exception("Database connection not open.");

                // Build drop statement
                query = String.Format("DROP TABLE {0}.{1}",tableschema,tablename);

                // create connection and command
                using (OdbcCommand cmd = new OdbcCommand(query, _conn))
                {
                    // Define SQL command to run
                    cmd.CommandText = query;

                    // open connection, execute create command 
                    iRtnCmd = cmd.ExecuteNonQuery();
                }
                // Return results
                if (iRtnCmd == 0)
                {
                    _lastError = String.Format("Table {1}.{0} was dropped/deleted.", tablename,tableschema);
                    return true;
                }
                else
                {
                    _lastError = String.Format("Table {1}.{0} was not dropped/deleted.", tablename,tableschema);
                    return false;
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;

                // If errors occur, return false
                return false;
            }

        }

        /// <summary>
        ///  Check for IBM i table existence based on SCHEMALIB.TABLENAME.
        /// </summary>
        /// <param name="tableschema">Table library/schema to check for.</param>
        /// <param name="tablename">Table name to check for.</param>
        /// <returns>True-Table exists. False-Table does not exist.</returns>
        public bool TableExists(string tableschema,string tablename)
        {
            try
            {
                _lastError = "";

                _dtTable = null;
                _iDtRows = 0;
                _iDtColumns = 0;

                // Check for active connection
                if (IsConnected() == false)
                    throw new Exception("Database connection not open.");

                //string query = String.Format("SELECT TABLE_PARTITION FROM QSYS2.SYSPARTITIONSTAT WHERE TABLE_NAME = '{0}' and TABLE_SCHEMA = '{1}' ", tablename, tableschema);
                
                // Query table but only return first result row
                string query = String.Format("SELECT * from {1}.{0} FETCH FIRST 1 ROWS ONLY", tablename, tableschema);

                // Create temporary SQL Server data adapter using SQL Server connection string and SQL statement
                using (OdbcDataAdapter adapter = new OdbcDataAdapter(query, _conn))
                {

                    // Fill a DataTable using the DataAdapter
                    DataTable dtWork = new DataTable();

                    adapter.Fill(dtWork);

                    // Dispose of Adapter when we're done
                    adapter.Dispose();

                    if (dtWork == null)
                    {
                        _lastError = "SQL query returned no DataTable.";
                        return false;

                    }
                    else if (dtWork.Rows.Count > 0) // AT least 1 row returned
                    {
                        _lastError = String.Format("{0} rows were returned. Table {2}.{1} exists.", dtWork.Rows.Count, tablename,tableschema);
                        return true;
                    }
                    else // No rows returned but successful query so table must exist
                    {
                        _lastError = String.Format("No rows were returned, but it appears table {1}.{0} exists.",tablename,tableschema);
                        return true;
                    }

                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return nothing.
                return false;
            }
        }

        /// <summary>
        ///  This function gets the internal DataTable results and returns as a CSV string.
        ///  </summary>
        ///  <param name="sFieldSepchar">Field delimiter/separator. Default = Comma</param>
        ///  <param name="sFieldDataDelimChar">Field data delimiter character. Default = double quotes.</param>
        ///  <returns>CSV string from DataTable</returns>
        public string GetQueryResultsDataTableToCsvString(string sFieldSepchar = ",", string sFieldDataDelimChar = "\"")
        {
            try
            {
                _lastError = "";

                //string sHeadings = "";
                //string sBody = "";
                StringBuilder sCsvData = new StringBuilder();

                // first write a line with the columns name
                string sep = "";
                System.Text.StringBuilder builder = new System.Text.StringBuilder();
                foreach (DataColumn col in _dtTable.Columns)
                {
                    builder.Append(sep).Append(col.ColumnName);
                    sep = sFieldSepchar;
                }
                sCsvData.AppendLine(builder.ToString());

                // then write all the rows
                foreach (DataRow row in _dtTable.Rows)
                {
                    sep = "";
                    builder = new System.Text.StringBuilder();

                    foreach (DataColumn col in _dtTable.Columns)
                    {
                        builder.Append(sep);
                        builder.Append(sFieldDataDelimChar).Append(row[col.ColumnName]).Append(sFieldDataDelimChar);
                        sep = sFieldSepchar;
                    }
                    sCsvData.AppendLine(builder.ToString());
                }

                // Return CSV output
                return sCsvData.ToString();
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "";
            }
        }
        /// <summary>
        ///  This function gets the internal DataTable results and outputs the data as a CSV file.
        ///  </summary>
        ///  <param name="sOutputFile">Output CSV file</param>
        ///  <param name="sFieldSepchar">Field delimiter/separator. Default = Comma</param>
        ///  <param name="sFieldDataDelimChar">Field data delimiter character. Default = double quotes.</param>
        ///  <param name="replace">Replace output file True=Replace file,False=Do not replace</param>
        ///  <returns>True=CSV file written successfully, False=Failure writing CSV output file.</returns>
        public bool GetQueryResultsDataTableToCsvFile(string sOutputFile, string sFieldSepchar = ",", string sFieldDataDelimChar = "\"", bool replace = false)
        {
            string sCsvWork;

            try
            {
                _lastError = "";

                // Delete existing file if replacing
                if (File.Exists(sOutputFile))
                {
                    if (replace)
                        File.Delete(sOutputFile);
                    else
                        throw new Exception("Output file " + sOutputFile + " already exists and replace not selected.");
                }

                // Get data and output
                using (System.IO.StreamWriter writer = new System.IO.StreamWriter(sOutputFile))
                {

                    // Get CSV string
                    sCsvWork = GetQueryResultsDataTableToCsvString(sFieldSepchar, sFieldDataDelimChar);

                    // Write out CSV data
                    writer.Write(sCsvWork);

                    // Flush final output and close
                    writer.Flush();
                    writer.Close();

                    return true;
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        ///  This function gets the internal DataTable results and returns as a XML string.
        ///  </summary>
        ///  <param name="sTableName">DataTable name to use. Default="Table1"</param>
        ///  <param name="writeSchema">Write XML schema in return data. Default=False</param>
        ///  <returns>XML string from DataTable</returns>
        public string GetQueryResultsDataTableToXmlString(string sTableName = "Table1", bool writeSchema = false)
        {
            string sRtnXml = "";

            try
            {
                _lastError = "";

                // if table not set, default to Table1
                if (sTableName.Trim() == "")
                    sTableName = "Table1";

                // Export results to XML
                if (_dtTable == null == false)
                {
                    StringBuilder SB = new StringBuilder();
                    System.IO.StringWriter SW = new System.IO.StringWriter(SB);
                    _dtTable.TableName = sTableName;
                    // Write XMl with or without schema info
                    if (writeSchema)
                        _dtTable.WriteXml(SW, System.Data.XmlWriteMode.WriteSchema);
                    else
                        _dtTable.WriteXml(SW);
                    sRtnXml = SW.ToString();
                    SW.Close();
                    return sRtnXml;
                }
                else
                    throw new Exception("No data available. Error: " + GetLastError());
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "";
            }
        }
        /// <summary>
        ///  This function gets the internal DataTable results and outputs the data as a XML file.
        ///  </summary>
        ///  <param name="sOutputFile">Output XML result file</param>
        ///  <param name="sTableName">DataTable name to use. Default = "Table1"</param>
        ///  <param name="writeSchema">Write XML schema in return data</param>
        ///  <param name="replace">Replace output file True=Replace file,False=Do not replace</param>
        ///  <returns>True=XML file written successfully, False=Failure writing XML output file.</returns>
        public bool GetQueryResultsDataTableToXmlFile(string sOutputFile, string sTableName = "Table1", bool writeSchema = false, bool replace = false)
        {
            string sXmlWork;

            try
            {
                _lastError = "";

                // Delete existing file if replacing
                if (File.Exists(sOutputFile))
                {
                    if (replace)
                        File.Delete(sOutputFile);
                    else
                        throw new Exception("Output file " + sOutputFile + " already exists and replace not selected.");
                }

                // Get data and output 
                using (System.IO.StreamWriter writer = new System.IO.StreamWriter(sOutputFile))
                {

                    // Get XML string
                    sXmlWork = GetQueryResultsDataTableToXmlString(sTableName, writeSchema);

                    // Write out CSV data
                    writer.Write(sXmlWork);

                    // Flush final output and close
                    writer.Flush();
                    writer.Close();

                    return true;
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        ///  This function gets the internal DataTable results and returns as a JSON string.
        ///  </summary>
        /// <param name="debugInfo">Write debug info in to JSON result packet. Default = False</param>
        ///  <returns>JSON string from DataTable</returns>
        public string GetQueryResultsDataTableToJsonString(bool debugInfo = false)
        {

            // TODO - Use Newtonsoft JSON to convert to JSON

            string sJsonData = "";
            JsonHelper oJsonHelper = new JsonHelper();

            try
            {
                _lastError = "";

                // If DataTable is blank, bail
                if (_dtTable == null)
                    throw new Exception("DataTable is Nothing. No data available.");

                // Convert DataTable to JSON
                sJsonData = oJsonHelper.DataTableToJsonWithStringBuilder(_dtTable, debugInfo);

                // Return JSON output
                return sJsonData.ToString();
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "";
            }
        }

        /// <summary>
        ///  This function gets the internal DataTable results and outputs the data as a JSON file.
        ///  </summary>
        ///  <param name="sOutputFile">Output JSON file</param>
        ///  <param name="replace">Replace output file True=Replace file,False=Do not replace</param>
        ///  <returns>True=JSON file written successfully, False=Failure writing JSON output file.</returns>
        public bool GetQueryResultsDataTableToJsonFile(string sOutputFile, bool replace = false)
        {
            string sJsonWork;

            try
            {
                _lastError = "";

                // Delete existing file if replacing
                if (File.Exists(sOutputFile))
                {
                    if (replace)
                        File.Delete(sOutputFile);
                    else
                        throw new Exception("Output file " + sOutputFile + " already exists and replace not selected.");
                }

                // Get data and output 
                using (System.IO.StreamWriter writer = new System.IO.StreamWriter(sOutputFile))
                {

                    // Get JSON string
                    sJsonWork = GetQueryResultsDataTableToJsonString();

                    // Write out JSON data
                    writer.Write(sJsonWork);

                    // Flush final output and close
                    writer.Flush();
                    writer.Close();

                    return true;
                }
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Convert DataReader to DataTable
        /// </summary>
        /// <param name="_dataReader">DataReader object to convert</param>
        /// <returns>DataTable of results or null on error</returns>
        public DataTable ConvertDataReaderToDataTable(DbDataReader _dataReader)
        {

            try
            {
                _lastError = "";
                DataTable _dataTable = new DataTable();
                _dataTable.Load(_dataReader);
                return _dataTable;
            } catch (Exception ex)
            {
              _lastError= ex.Message;
              return null;
            }

        }

        /// <summary>
        /// Execute CL command via SQL call to QSYS.QCMDEXC
        /// </summary>
        /// <param name="clCommand">CL command line</param>
        /// <returns>0=success,-2=errors</returns>
        public int ExecClCommandQsys(string clCommand)
        {
         
            String strClCmd;

            try
            {

                _lastError = "";

                // Check for active connection
                if (IsConnected() == false)
                    throw new Exception("Database connection not open.");

                // Build CL SQL command
                strClCmd = "CALL QSYS.QCMDEXC('" + clCommand.Trim() + "', " + (clCommand.Trim().Length).ToString("0000000000.00000").Replace(",", ".") + ")";

                // Create command object to run CL command
                using (OdbcCommand _cmdcl = new OdbcCommand(strClCmd, _conn))
                {

                    // Execute the command. 
                    // 0 is returned for success.
                    int i = _cmdcl.ExecuteNonQuery();

                    return i;
                }

            }
            catch (Exception ex)
            {
                _lastError=ex.Message; 
                return -2;
            }

        }

        /// <summary>
        /// Execute CL command via SQL call to QSYS2.QCMDEXC
        /// </summary>
        /// <param name="clCommand">CL command line</param>
        /// <returns>0=success,-2=errors</returns>
        public int ExecClCommandQsys2(string clCommand)
        {

            String strClCmd;

            try
            {

                _lastError = "";

                // Check for active connection
                if (IsConnected() == false)
                    throw new Exception("Database connection not open.");

                // Build CL SQL command
                strClCmd = "CALL QSYS2.QCMDEXC('" + clCommand.Trim() + "', " + (clCommand.Trim().Length).ToString("0000000000.00000").Replace(",", ".") + ")";

                // Create command object to run CL command
                using (OdbcCommand _cmdcl = new OdbcCommand(strClCmd, _conn))
                {

                    // Execute the command. 
                    // 0 is returned for success.
                    int i = _cmdcl.ExecuteNonQuery();

                    return i;
                }

            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return -2;
            }

        }

    }
}