using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Security;
using System.Text;
using System.Threading.Tasks;
using Microsoft.VisualBasic;
using System.Web;
using System.Runtime.Serialization.Json;
using System.Text.RegularExpressions;
using System.Data;
using Newtonsoft.Json;

/// <summary>
/// JSON Serialization and Deserialization Assistant Class
/// Source from: https://gist.github.com/monk8800/3760559
/// Note: Normally we would use the Newtonsoft JSON API, but this class removes any extra dependencies.
/// Latest version: 2/5/2024
/// </summary>

namespace IbmiOdbcDataAccess
{

    public class JsonHelper
    {
        private string _lasterror = "";

        /// <summary>
        /// Get last error
        /// </summary>
        /// <returns></returns>
        public string GetLastError()
        {
            return _lasterror;
        }

        /// <summary>
        /// JSON Serialization
        /// </summary>
        public string JsonSerializer<T>(T obj)
        {
            DataContractJsonSerializer ser = new DataContractJsonSerializer(typeof(T));
            MemoryStream ms = new MemoryStream();
            ser.WriteObject(ms, obj);
            string jsonString = Encoding.UTF8.GetString(ms.ToArray());
            ms.Close();
            // Replace Json Date String                                         
            string p = @"\\/Date\((\d+)\+\d+\)\\/";
            MatchEvaluator matchEvaluator = new MatchEvaluator(ConvertJsonDateToDateString);
            Regex reg = new Regex(p);
            jsonString = reg.Replace(jsonString, matchEvaluator);
            return jsonString;
        }

        /// <summary>
        /// JSON Deserialization
        /// </summary>
        public T JsonDeserialize<T>(string jsonString)
        {
            // Convert "yyyy-MM-dd HH:mm:ss" String as "\/Date(1319266795390+0800)\/"
            string p = @"\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}";
            MatchEvaluator matchEvaluator = new MatchEvaluator(ConvertDateStringToJsonDate);
            Regex reg = new Regex(p);
            jsonString = reg.Replace(jsonString, matchEvaluator);
            DataContractJsonSerializer ser = new DataContractJsonSerializer(typeof(T));
            MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(jsonString));
            T obj = (T)ser.ReadObject(ms);
            return obj;
        }

        /// <summary>
        /// Convert Serialization Time /Date(1319266795390+0800) as String
        /// </summary>
        private string ConvertJsonDateToDateString(Match m)
        {
            string result = string.Empty;
            DateTime dt = new DateTime(1970, 1, 1);
            dt = dt.AddMilliseconds(long.Parse(m.Groups[1].Value));
            dt = dt.ToLocalTime();
            result = dt.ToString("yyyy-MM-dd HH:mm:ss");
            return result;
        }

        /// <summary>
        /// Convert Date String as Json Time
        /// </summary>
        private string ConvertDateStringToJsonDate(Match m)
        {
            string result = string.Empty;
            DateTime dt = DateTime.Parse(m.Groups[0].Value);
            dt = dt.ToUniversalTime();
            TimeSpan ts = dt - DateTime.Parse("1970-01-01");
            result = string.Format(@"\/Date({0}+0800)\/", ts.TotalMilliseconds);
            return result;
        }
        /// <summary>
        /// Convert DataTable object to Json String
        /// Source: 'https://stackoverflow.com/questions/21648064/vb-net-datatable-serialize-to-json
        /// Now using Newtonsoft - 2/5/2024
        /// </summary>
        /// <param name="table">DataTable object</param>
        /// <returns>Serialized JSON DataTable as string</returns>
        public string ConvertDataTableToJson(DataTable table)
        {

            string JSONString = string.Empty;

            try
            {

                _lasterror = "";

                if (table == null)
                {
                    return string.Empty;
                }

                JSONString = JsonConvert.SerializeObject(table);
                return JSONString;
            }
            catch (Exception ex)
            {
                _lasterror = ex.Message;
                return "";
            }

        }

        /// <summary>
        /// Serialize DataTable with Newtonsoft.JSON
        /// Sample from:
        /// http://www.c-sharpcorner.com/UploadFile/9bff34/3-ways-to-convert-datatable-to-json-string-in-Asp-Net-C-Sharp/
        /// </summary>
        /// <param name="table">DataTable</param>
        /// <param name="formatJson">Format JSON. True-Format,False-No Format</param>
        /// <returns>JSON string</returns>
        public string ConvertDataTableToJsonWithNewtonSoft(DataTable table, bool formatJson = true)
        {
            string JSONString = string.Empty;

            try
            {

                _lasterror = "";

                if (table == null)
                {
                    return string.Empty;
                }

                // Serialize Data Table to formatted JSON
                if (formatJson)
                {
                    JSONString = JsonConvert.SerializeObject(table, Formatting.Indented);
                }
                else // Serialize Data Table to unformatted JSON
                {
                    JSONString = JsonConvert.SerializeObject(table, Formatting.None);
                }

                return JSONString;
            }
            catch (Exception ex)
            {
                _lasterror = ex.Message;
                return "";
            }
        }

        /// <summary>
        /// Convert DataTable to json string using StringBuilder
        /// https://stackoverflow.com/questions/17398019/convert-datatable-to-json-in-c-sharp
        /// </summary>
        /// <param name="table">DataTable input</param>
        /// <param name="debugINfo">True-Write debug info in response JSON. No debug info in error response</param>
        /// <returns>DataTable results as JSON string</returns>
        public string DataTableToJsonWithStringBuilder(DataTable table, bool debugInfo = false)
        {
            StringBuilder jsonString = new StringBuilder();

            try
            {
                // Convert table rows to JSON
                if (table.Rows.Count > 0)
                {
                    jsonString.Append("[");
                    for (int i = 0; i <= table.Rows.Count - 1; i++)
                    {
                        jsonString.Append("{");
                        for (int j = 0; j <= table.Columns.Count - 1; j++)
                        {
                            if (j < table.Columns.Count - 1)
                                jsonString.Append("\"" + table.Columns[j].ColumnName.ToString() + "\":" + "\"" + table.Rows[i][j].ToString() + "\",");
                            else if (j == table.Columns.Count - 1)
                                jsonString.Append("\"" + table.Columns[j].ColumnName.ToString() + "\":" + "\"" + table.Rows[i][j].ToString() + "\"");
                        }
                        if (i == table.Rows.Count - 1)
                            jsonString.Append("}");
                        else
                            jsonString.Append("},");
                    }
                    jsonString.Append("]");

                    // Return the JSON result
                    return jsonString.ToString();
                }
                else
                    return "[{\"message\":\"No json results returned\"}]";
            }
            catch (Exception ex)
            {
                if (debugInfo)
                    return "[{\"message\":\" Error converting DataTable results to json. Error: " + ex.Message + "\"}]";
                else
                    return "[{\"message\":\"Exception occurred returning json results\"}]";
            }
        }

        /// <summary>
        /// Serialize a DataTable Using System.text.Json
        /// https://code-maze.com/convert-datatable-json-csharp/
        /// </summary>
        /// <param name="dataTable">DataTable</param>
        /// <returns>Converted JSON as string</returns>
        public string DataTableSystemTextJson(DataTable dataTable)
        {
            if (dataTable == null)
            {
                return string.Empty;
            }
            var data = dataTable.Rows.OfType<DataRow>()
                        .Select(row => dataTable.Columns.OfType<DataColumn>()
                            .ToDictionary(col => col.ColumnName, c => row[c]));
            return System.Text.Json.JsonSerializer.Serialize(data);
        }

        /// <summary>
        /// Serialize a DataTable Using Newtonsoft.Json
        /// https://code-maze.com/convert-datatable-json-csharp/
        /// </summary>
        /// <param name="dataTable">DataTable</param>
        /// <returns>Converted JSON as string</returns>
        public string DataTableNewtonsoftJsonNet(DataTable dataTable)
        {
            if (dataTable == null)
            {
                return string.Empty;
            }
            return Newtonsoft.Json.JsonConvert.SerializeObject(dataTable);
        }


        /// <summary>
        /// Serialize a DataTable by Constructing a Json String
        /// https://code-maze.com/convert-datatable-json-csharp/
        /// </summary>
        /// <param name="dataTable">DataTable</param>
        /// <returns>Converted JSON as string</returns>
        public string DataTableStringBuilder(DataTable dataTable)
        {
            if (dataTable == null)
            {
                return string.Empty;
            }

            var jsonStringBuilder = new StringBuilder();
            if (dataTable.Rows.Count > 0)
            {
                jsonStringBuilder.Append("[");
                for (int i = 0; i < dataTable.Rows.Count; i++)
                {
                    jsonStringBuilder.Append("{");
                    for (int j = 0; j < dataTable.Columns.Count; j++)
                        jsonStringBuilder.AppendFormat("\"{0}\":\"{1}\"{2}",
                                dataTable.Columns[j].ColumnName.ToString(),
                                dataTable.Rows[i][j].ToString(),
                                j < dataTable.Columns.Count - 1 ? "," : string.Empty);

                    jsonStringBuilder.Append(i == dataTable.Rows.Count - 1 ? "}" : "},");
                }
                jsonStringBuilder.Append("]");
            }

            return jsonStringBuilder.ToString();
        }

        /// <summary>
        /// Serialize a DataTable to Json using Linq
        /// https://code-maze.com/convert-datatable-json-csharp/
        /// </summary>
        /// <param name="dataTable">DataTable</param>
        /// <returns>Converted JSON as string</returns>
        public string DataTableLinq(DataTable dataTable)
        {
            if (dataTable == null)
            {
                return string.Empty;
            }

            return "["
                    + string.Join(",", dataTable.Rows.OfType<DataRow>()
                    .Select(row =>
                        "{"
                        + string.Join(",", dataTable.Columns.OfType<DataColumn>()
                            .Select(col => string.Format("\"{0}\":\"{1}\"",
                                                col.ColumnName,
                                                row[col].ToString())))
                        + "}"))
                    + "]";
        }

    }

}

