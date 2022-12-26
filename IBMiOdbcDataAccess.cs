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

/// <summary>
/// This class file contains a general ODBC data class wrapper
/// to simplify ODBC work.
/// This class can also be inherited and extended from a business object.
/// </summary>
/// <remarks></remarks>
public class IbmiOdbcDataAccess
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

    /// <summary>
    /// Get last error
    /// </summary>
    /// <returns></returns>
    public string GetLastError()
    {
        return _lastError;
    }

    /// <summary>
    /// Get last SQL query
    /// </summary>
    /// <returns></returns>
    public string GetLastSql()
    {
        return _lastSql;
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
            _lastError = ex.Message;
        }
    }

    /// <summary>
    /// Open database connection without passing explicit connection string
    /// If no connection string passed, SetConnectionString must be called beforehand 
    /// set connection info.
    /// </summary>
    public bool OpenConnection()
    {
        // Call open connection with no connection string
        return OpenConnection("");
    }
    /// <summary>
    /// Return connection status
    /// </summary>
    /// <returns></returns>
    public bool IsConnected()
    {
        return _bConnectionOpen;
    }
    /// <summary>
    /// Open database connection with set connection string
    /// </summary>
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

            // Bail if no connection string
            if (_connectionString.Trim() == "")
                throw new Exception("No database connection string set.");

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
    /// Close database connection
    /// </summary>
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
    /// <returns>Data Table or null</returns>
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
    /// <param name="sqlselect"></param>
    /// <param name="iStartRecord">Starting record. Default=0. If start and max are 0, all records will be exported to DataTable.</param>
    /// <param name="iMaxRecords">Ending record. Default = 0. If start and max are 0, all records will be exported to DataTable.</param>
    /// <param name="tablename">Data table name</param>
    /// <param name="queryTimeout">Query Timeout. 0=No Timeout,-1=Use default timeout which is usually 30 seconds</param>
    /// <returns>Boolean for query completion</returns>
    public bool ExecuteQueryToDataTableInternal(string sqlselect, int iStartRecord = 0, int iMaxRecords = 0, string tableName = "Table1",int queryTimeout=-1)
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
    /// get internal Data Table contents to delimited string
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
        string sql = "";
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
        string sql = "";
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
            if (File.Exists(outputFile))
            {
                bOutputFileExists = true;
                if (replace == true)
                {
                    File.Delete(outputFile);
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
            File.AppendAllText(outputFile, sbHdr.ToString() + sbDtl.ToString(), Encoding.UTF8);

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
        string sql = "";
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
            if (File.Exists(outputFile))
            {
                bOutputFileExists = true;
                if (replace == true)
                {
                    File.Delete(outputFile);
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
            File.AppendAllText(outputFile, sbHdr.ToString() + sbDtl.ToString(), Encoding.UTF8);

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
    public bool QueryAndExportRecordsToDelimFileDr(string sqlselect, string outputFile, string delim = ",", bool replace = false, bool removeLineFeeds = true, bool doubleQuotes = true, bool spaceBeforeDelim = true, bool outputHeadings = true,int queryTimeout = -1)
    {
        bool rtnquery;

        try
        {

            // Attempt to run query to DataReader. 
            rtnquery = ExecuteQueryToDataReaderInternal(sqlselect,queryTimeout);

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
    public bool QueryAndExportRecordsToDelimFileDt(string sqlselect, string outputFile, string delim = ",", bool replace = false, bool removeLineFeeds = true, bool doubleQuotes = true, bool spaceBeforeDelim = true, bool outputHeadings = true,int queryTimeout=-1)
    {
        bool rtnquery;

        try
        {

            // Attempt to run query to DataTable. 
            rtnquery = ExecuteQueryToDataTableInternal(sqlselect, 0, 0, "Table1",queryTimeout);

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
    public bool ExecuteQueryToDataReaderInternal(string sqlselect,int queryTimeout=-1)  
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

            if (_dReader!=null)
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
            string swork = "";
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
    /// <returns>Records affected or -2 on errors.</returns>
    public int ExecuteCommandNonQuery(string sqlCommand,int commandTimeout=-1)
    {
        try
        {
            _lastError = "";

            // Check for active connection
            if (IsConnected() == false)
                throw new Exception("Database connection not open.");

            // Bail if a SELECT
            if (sqlCommand.ToUpper().StartsWith("SELECT"))
                throw new Exception("SELECT queries are not allowed here.");

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
    /// <param name="connectionString">Connection string</param>
    /// <param name="tablename">Table name to drop</param>
    /// <returns></returns>
    public bool DropTable(string tablename)
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
            query = String.Format("DROP TABLE {0}", tablename);

            // create connection and command
            using (OdbcCommand cmd = new OdbcCommand(query, _conn))
            {
                // Define SQL command to run
                cmd.CommandText = query;

                // open connection, execute create command 
                iRtnCmd = cmd.ExecuteNonQuery();
            }
            // Return results
            // Return results
            if (iRtnCmd == -1)
            {
                _lastError = String.Format("Table {0} was dropped/deleted.", tablename);
                return true;
            }
            else
            {
                _lastError = String.Format("Table {0} was not dropped/deleted.", tablename);
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
    ///  Check for SQL Server table existence
    /// </summary>
    public bool TableExists(string tablename)
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

            string query = String.Format("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{0}'", tablename);

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
                else if (dtWork.Rows.Count > 0)
                {
                    _lastError = String.Format("{0} rows were returned. Table {1} exists.", dtWork.Rows.Count, tablename);
                    return true;
                }
                else
                {
                    _lastError = String.Format("No rows were returned.");
                    return false;
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

}
