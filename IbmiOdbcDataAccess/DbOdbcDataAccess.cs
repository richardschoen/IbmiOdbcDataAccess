using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security;
using System.Text;
using System.Data;
using System.Data.Odbc;
using static System.Net.WebRequestMethods;

namespace IbmiOdbcDataAccess
{
    /// <summary>
    /// This class file contains a general ODBC data class wrapper
    /// to simplify ODBC work with IBM i data.
    /// This class can also be inherited and extended from a business object.
    /// </summary>
    /// <remarks></remarks>
    public class DbOdbcDataAccess
    {
        // Made these class variables public so class
        // that is using this as a base class can use these variables too
        private string _lastError;
        private string _connectionString = "";
        private DataTable _dTable;
        private int _iDtRows;
        private int _iDtColumns;
        private OdbcDataReader _dReader;
        private OdbcConnection _conn;
        private OdbcCommand _cmd;
        private bool _bConnectionOpen = false;
        private int _iLastExportCount;
        private string _lastSql;
        private string _ibmiaccessconntemplate="Driver={IBM i Access ODBC Driver};System=@@SYSTEM;Uid=@@USERID;Pwd=@@PASS;CommitMode=0;EXTCOLINFO=1";

        /// <summary>
        /// Get last error
        /// </summary>
        /// <returns>Error info from last call if set</returns>
        public string GetLastError()
        {
            return _lastError;
        }

        /// <summary>
        /// Get last SQL query
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
        /// Open database connection without passing explicit connection string
        /// If no connection string passed, SetConnectionString must be called beforehand 
        /// to set connection string info.
        /// </summary>
        /// <returns>True=Connection opened successfully. False=Error occurred opening connection.</returns>
        public bool OpenConnection()
        {
            // Call open connection with no connection string
            return OpenConnection("");
        }

        /// <summary>
        /// Return connection status
        /// </summary>
        /// <returns>True=Connection is open. False=Connection is not open.</returns>
        public bool IsConnected()
        {
            return _bConnectionOpen;
        }

        /// <summary>
        /// Open database connection with set connection string
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
        /// <param name="tablename">Data table name</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <returns>DataTable or null</returns>
        public DataTable ExecuteQueryToDataTable(string sqlselect, int iStartRecord = 0, int iMaxRecords = 0, string tableName = "Table1", int queryTimeout = -1)
        {
            try
            {
                _lastError = "";
                _dTable = null;
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

                    // Fill a Data Table using the data adapter
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

                    // Dispose of Adapter when we're done
                    adapter.Dispose();

                    // Return the recordset to class level datatable so we can access indefinitely
                    _dTable = dtWork;
                    _dTable.TableName = tableName;

                    // Set row/col info
                    _iDtRows = _dTable.Rows.Count;
                    _iDtColumns = _dTable.Columns.Count;

                    return _dTable; // Return data table
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
        /// Run SQL query and return as internal DataTable
        /// This function takes an SQL SELECT statement and connection string and 
        /// runs the query to get the data we want to work with.
        /// </summary>
        /// <param name="sqlselect">SQL query</param>
        /// <param name="iStartRecord">Starting record. Default=0. If start and max are 0, all records will be exported to DataTable.</param>
        /// <param name="iMaxRecords">Ending record. Default = 0. If start and max are 0, all records will be exported to DataTable.</param>
        /// <param name="tablename">Data table name</param>
        /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
        /// <returns>Boolean for query completion</returns>
        public bool ExecuteQueryToDataTableInternal(string sqlselect, int iStartRecord = 0, int iMaxRecords = 0, string tableName = "Table1", int queryTimeout = -1)
        {
            try
            {
                _lastError = "";

                _dTable = null;
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

                    // Fill a Data Table using the data adapter
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

                    // Dispose of Adapter when we're done
                    adapter.Dispose();

                    // Return the recordset to class level datatable so we can access indefinitely
                    _dTable = dtWork;
                    _dTable.TableName = tableName;

                    // Set row/col info
                    _iDtRows = _dTable.Rows.Count;
                    _iDtColumns = _dTable.Columns.Count;

                    return true; // Return data table
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
        /// Get internal Data Table contents to delimited string
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

                // Verify that Data Table has data
                if (_dTable == null)
                    throw new Exception("Data Table has no data. Export cancelled.");

                // Get first record so we can extract field names in query result
                int count = 0;

                // Output headings only if enabled and output file not found already
                if (outputHeadings & bOutputFileExists == false)
                {

                    // Extract all the local filed names
                    for (int j = 0; j <= _dTable.Columns.Count - 1; j++)
                    {
                        if (count == _dTable.Columns.Count - 1)
                        {
                            if (removeLineFeeds)
                                sbHdr.Append(_dTable.Columns[j].ColumnName.Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>"));
                            else
                                sbHdr.Append(_dTable.Columns[j].ColumnName.Trim());
                        }
                        else if (removeLineFeeds)
                            sbHdr.Append(_dTable.Columns[j].ColumnName.Trim().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + sWorkSpace + delim);
                        else
                            sbHdr.Append(_dTable.Columns[j].ColumnName.Trim() + sWorkSpace + delim);
                        count += 1;
                    }
                    // Output new line after record is output
                    sbHdr.AppendLine("");
                }

                // Process all the records to delimited string buffer
                // Replace CRLF, CR and LF values with placeholders.
                foreach (DataRow dr in _dTable.Rows)
                {
                    // Extract all field data
                    count = 0;
                    for (int j = 0; j <= _dTable.Columns.Count - 1; j++)
                    {
                        if (count == _dTable.Columns.Count - 1)
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
        /// Export internal Data Table contents to delimited file
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

                // Verify that Data Table has data
                if (_dTable == null)
                    throw new Exception("Data Table has no data. Export cancelled.");

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
                    for (int j = 0; j <= _dTable.Columns.Count - 1; j++)
                    {
                        if (count == _dTable.Columns.Count - 1)
                        {
                            if (removeLineFeeds)
                                sbHdr.Append(_dTable.Columns[j].ColumnName.Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>"));
                            else
                                sbHdr.Append(_dTable.Columns[j].ColumnName.Trim());
                        }
                        else if (removeLineFeeds)
                            sbHdr.Append(_dTable.Columns[j].ColumnName.Trim().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + sWorkSpace + delim);
                        else
                            sbHdr.Append(_dTable.Columns[j].ColumnName.Trim() + sWorkSpace + delim);
                        count += 1;
                    }
                    // Output new line after record is output
                    sbHdr.AppendLine("");
                }

                // Process all the records to delimited string buffer
                // Replace CRLF, CR and LF values with placeholders.
                foreach (DataRow dr in _dTable.Rows)
                {
                    // Extract all field data
                    count = 0;
                    for (int j = 0; j <= _dTable.Columns.Count - 1; j++)
                    {
                        if (count == _dTable.Columns.Count - 1)
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
                _iLastExportCount = _dTable.Rows.Count;
                _lastError = _dTable.Rows.Count + " rows were exported to delimited file " + outputFile;

                return true;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }
        /// <summary>
        /// Export internal Data Reader contents to delimited file. 
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

                // Verify that Data Table has data
                if (_dReader == null)
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
                    for (int j = 0; j <= _dReader.FieldCount - 1; j++)
                    {
                        if (count == _dReader.FieldCount - 1)
                        {
                            if (removeLineFeeds)
                                sbHdr.Append(_dReader.GetName(j).Trim().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>"));
                            else
                                sbHdr.Append(_dReader.GetName(j).Trim());
                        }
                        else if (removeLineFeeds)
                            sbHdr.Append(_dReader.GetName(j).Trim().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + sWorkSpace + delim);
                        else
                            sbHdr.Append(_dReader.GetName(j).Trim() + sWorkSpace + delim);
                        count += 1;
                    }
                    // Output new line after record is output
                    sbHdr.AppendLine("");
                }

                // Process all the records to delimited string buffer
                // Replace CRLF, CR and LF values with placeholders.
                while (_dReader.Read())
                {
                    // Extract all field data
                    count = 0;
                    for (int j = 0; j <= _dReader.FieldCount - 1; j++)
                    {
                        if (count == _dReader.FieldCount - 1)
                        {
                            if (removeLineFeeds)
                                sbDtl.Append(_dReader.GetValue(j).ToString().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>"));
                            else
                                sbDtl.Append(_dReader.GetValue(j).ToString());
                        }
                        else if (removeLineFeeds)
                            sbDtl.Append(_dReader.GetValue(j).ToString().Replace("\r\n", "<CRLF>").Replace("\r", "<CR>").Replace("\n", "<LF>") + sWorkSpace + delim);
                        else
                            sbDtl.Append(_dReader.GetValue(j).ToString() + sWorkSpace + delim);
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
        /// Query Table and Export Internal Data Reader contents to delimited file
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
        public bool QueryAndExportRecordsToDelimFileDr(string sqlselect, string outputFile, string delim = ",", bool replace = false, bool removeLineFeeds = true, bool doubleQuotes = true, bool spaceBeforeDelim = true, bool outputHeadings = true, int queryTimeout = -1)
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataReader. 
                rtnquery = ExecuteQueryToDataReaderInternal(sqlselect, queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Now export the Data Reader results to delimited file
                return ExportRecordsToDelimFileDr(outputFile, delim, replace, removeLineFeeds, doubleQuotes, spaceBeforeDelim, outputHeadings);
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return false;
            }
        }

        /// <summary>
        /// Query Table and Export Internal Data Table contents to delimited file
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
        public bool QueryAndExportRecordsToDelimFileDt(string sqlselect, string outputFile, string delim = ",", bool replace = false, bool removeLineFeeds = true, bool doubleQuotes = true, bool spaceBeforeDelim = true, bool outputHeadings = true, int queryTimeout = -1)
        {
            bool rtnquery;

            try
            {

                // Attempt to run query to DataTable. 
                rtnquery = ExecuteQueryToDataTableInternal(sqlselect, 0, 0, "Table1", queryTimeout);

                // Bail if errors
                if (rtnquery == false)
                    throw new Exception("Query failed. Error: " + GetLastError());

                // Now export the Data Table results to delimited file
                return ExportRecordsToDelimFileDt(outputFile, delim, replace, removeLineFeeds, doubleQuotes, spaceBeforeDelim, outputHeadings);
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

                _dTable = null;
                _dReader = null;
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
                _dReader = _cmd.ExecuteReader();

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

                _dTable = null;
                _dReader = null;
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
        /// Get Internal Data Table reference. Must be populated using ExecuteQueryDataTableInternal.
        /// </summary>
        /// <returns>Data Table</returns>
        public DataTable GetDataTableInternal()
        {
            try
            {
                _lastError = "";

                return _dTable;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return null.
                return null;
            }
        }
        /// <summary>
        /// Get Internal Data Reader reference. Must be populated using ExecuteQueryDataReaderInternal.
        /// </summary>
        /// <returns>Data Reader</returns>
        public OdbcDataReader GetDataReaderInternal()
        {
            try
            {
                _lastError = "";

                return _dReader;
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
        /// <returns>Data Reader</returns>
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
        /// Get next row from internal Data Reader 
        /// </summary>
        /// <returns>True-Next record read to internal reader, False-no records read. We're done</returns>
        public bool GetNextRowDrInternal()
        {
            try
            {
                _lastError = "";

                return _dReader.Read();
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return nothing.
                return false;
            }
        }
        /// <summary>
        /// Close data reader
        /// </summary>
        /// <returns></returns>
        public bool CloseDataReaderInternal()
        {
            try
            {
                _lastError = "";

                if (_dReader != null)
                {
                    _dReader.Close();
                }
                _dReader = null;

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
        /// Get Field from Data Reader based on ordinal column position
        /// </summary>
        /// <returns></returns>
        public string GetColValueByPosDr(int iCol)
        {
            try
            {
                _lastError = "";

                // get selected column number as tring
                return _dReader.GetValue(iCol).ToString();
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                // If errors occur, return blanks
                return "";
            }
        }
        /// <summary>
        /// Return current row from data reader as delimited record
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

                // Build delimited data from current data reader row
                for (iCurCol = 0; iCurCol <= _dReader.FieldCount - 1; iCurCol++)
                    swork = swork + _dReader.GetValue(iCurCol).ToString() + sDelim;

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
        /// Return data reader field/column count
        /// </summary>
        /// <returns></returns>
        public int GetColCountDr()
        {
            return _dReader.FieldCount;
        }

        /// <summary>
        /// Return data table row count
        /// </summary>
        /// <returns></returns>
        public int GetRowCountDt()
        {
            return _iDtRows;
        }

        /// <summary>
        /// Return data table column count
        /// </summary>
        /// <returns></returns>
        public int GetColCountDt()
        {
            return _iDtColumns;
        }
        /// <summary>
        /// Return Data Table column value for specified row/column
        /// </summary>
        /// <param name="iRow">Data table row number</param>
        /// <param name="iCol">Data table column number</param>
        /// <returns></returns>
        public string GetRowValueByPosDt(int iRow, int iCol)
        {
            try
            {
                string swork = "";
                _lastError = "";
                //swork = _dTable.Rows[iRow].Item[iCol].ToString;
                // TODO - Test this after conv to C#
                swork = _dTable.Rows[iRow][iCol].ToString();
                return swork;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "**ERROR";
            }
        }

        /// <summary>
        /// Return data table column for specified row based on column field name
        /// </summary>
        /// <param name="iRow">Data table row number</param>
        /// <param name="sColName">Data table column name</param>
        /// <returns>Field value or **ERROR if errors</returns>
        public string GetRowValueByNameDt(int iRow, string sColName)
        {
            try
            {
                string swork = "";
                _lastError = "";
                // TODO - test this after conversion to C#
                swork = _dTable.Rows[iRow][sColName].ToString();
                return swork;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "**ERROR";
            }
        }
        /// <summary>
        /// Return Data Reader column for current row based on column field name.
        /// This is a convenience function because the Data Reader needs ordinal positions
        /// when returning field data.
        /// </summary>
        /// <param name="sColName">Column field name</param>
        /// <returns>Field value or **ERROR if errors</returns>
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
                swork = _dReader.GetValue(iCol).ToString();

                return swork;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return "**ERROR";
            }
        }
        /// <summary>
        /// Return column names for current Data Table in delimited record
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
                foreach (DataColumn col in _dTable.Columns)
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
        /// Return column names for current Data Reader in delimited record
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
                for (iCount = 0; iCount <= _dReader.FieldCount - 1; iCount++)
                    swork = swork + _dReader.GetName(iCount) + sDelim;

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
        /// Return column ordinal position based on name.
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
                for (iCount = 0; iCount <= _dReader.FieldCount - 1; iCount++)
                {
                    if (sFieldName.ToLower().Trim() == _dReader.GetName(iCount).ToLower().Trim())
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
        /// Return Data Table current row as delimited record
        /// </summary>
        /// <param name="iRow">Data table row</param>
        /// <param name="sDelim">Field delimiter. Default = ,</param>
        /// <returns>Delimited string of data</returns>
        public string GetRowDelimDt(int iRow, string sDelim = ",")
        {
            try
            {
                string swork = "";

                _lastError = "";

                // Build delimited data from current row
                foreach (DataColumn col in _dTable.Columns)
                    swork = swork + _dTable.Rows[iRow][col.ColumnName] + sDelim;

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
        /// Run SQL Insert, Update, Delete or Other Command With no Resultset
        /// This function takes an SQL INSERT, UPDATE or DELETE statement and 
        /// connection string and runs the SQL command to update or 
        /// delete the data we want to work with.
        /// </summary>
        /// <param name="sqlCommand">SQL action command</param>
        /// <param name="commandTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
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
        /// Drop selected table
        /// </summary>
        /// <param name="tableschema">Table library/schema for table to drop.</param>
        /// <param name="tablename">Table name to drop.</param>
        /// <returns>True-Table dropped. False-Table not dropped.</returns>
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
        ///  Check for IBM i table existence
        /// </summary>
        /// <param name="tableschema">Table library/schema to check for.</param>
        /// <param name="tablename">Table name to check for.</param>
        /// <returns>True-Table exists. False-Table does not exist.</returns>
        public bool TableExists(string tableschema,string tablename)
        {
            try
            {
                _lastError = "";

                _dTable = null;
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

                    // Fill a Data Table using the data adapter
                    DataTable dtWork = new DataTable();

                    adapter.Fill(dtWork);

                    // Dispose of Adapter when we're done
                    adapter.Dispose();

                    if (dtWork == null)
                    {
                        _lastError = "SQL query returned no data table.";
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
        /// Execute CL command via SQL call to QSYS.QCMDEXC
        /// </summary>
        /// <param name="clCommand">CL command line</param>
        /// <returns>0=success,-2=errors</returns>
        public int ExecClCommandQsys(string clCommand)
        {
            OdbcCommand oCmd = null;
         
            String strClCmd;

            try
            {

                _lastError = "";

                // Build CL SQL command
                strClCmd = "CALL QSYS.QCMDEXC('" + clCommand.Trim() + "', " + (clCommand.Trim().Length).ToString("0000000000.00000").Replace(",", ".") + ")";

                // Create command object to run CL command
                oCmd = new OdbcCommand(strClCmd, _conn);

                // Execute the command. 
                // 0 is returned for success.
                int i = oCmd.ExecuteNonQuery();
            
                return i;
            }
            catch (Exception ex)
            {
                _lastError=ex.Message; 
                return -2;
            }
            finally
            {
                oCmd.Dispose();
            }

        }

        /// <summary>
        /// Execute CL command via SQL call to QSYS2.QCMDEXC
        /// </summary>
        /// <param name="clCommand">CL command line</param>
        /// <returns>0=success,-2=errors</returns>
        public int ExecClCommandQsys2(string clCommand)
        {
            OdbcCommand oCmd = null;

            String strClCmd;

            try
            {

                _lastError = "";

                // Build CL SQL command
                strClCmd = "CALL QSYS2.QCMDEXC('" + clCommand.Trim() + "', " + (clCommand.Trim().Length).ToString("0000000000.00000").Replace(",", ".") + ")";

                // Create command object to run CL command
                oCmd = new OdbcCommand(strClCmd, _conn);

                // Execute the command. 
                // 0 is returned for success.
                int i = oCmd.ExecuteNonQuery();

                return i;
            }
            catch (Exception ex)
            {
                _lastError = ex.Message;
                return -2;
            }
            finally
            {
                oCmd.Dispose();
            }

        }

    }
}