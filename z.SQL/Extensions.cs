using Microsoft.VisualBasic;
//using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using z.Data;

namespace z.SQL
{
    public static class Extensions
    {

        public static string SQLFormat(this object inp, string escapeDelimiter = "''")
        {
            return QueryCom.SQLFormat(inp, escapeDelimiter);
        }

        public static string SQLFormats(this string Command, params object[] args)
        {
            return string.Format(Command, args.Select(x => x.SQLFormat()).ToArray());
        }

        public static void TestConnect(this SqlConnectionStringBuilder args)
        {
            using (Query sql = new Query(args)) { }
        }

        public static SqlConnection Connection(this SqlConnectionStringBuilder args)
        {
            return new SqlConnection(args.ConnectionString);
        }

        /// <summary>
        /// Get All Keys Base on Specified Parameter
        /// </summary>
        /// <param name="Command"></param>
        /// <param name="Parameter"></param>
        /// <returns></returns>
        public static List<string> ParseParameter(this string Command, char Parameter = '@')
        {
            //MatchCollection mtc = Regex.Matches(Command, $@"(\s|\(|\,|^)(?:@\S*)(?=\s|\)|\,|$)", RegexOptions.IgnoreCase); //@"@\S*"
            //var mtc = Regex.Matches(Command, @"\@\b\S*\b", RegexOptions.IgnoreCase);
            ////string[] rspcer = new string[] { ",", ")", ";", " " };
            //List<string> str = new List<string>();
            //foreach (Match m in mtc) str.Add(m.Value); //.Replace("", rspcer)
            //for (int i = str.Count - 1; i >= 0; --i)
            //    if (str[i].StartsWith("@@")) //dont add the special functions
            //        str.Remove(str[i]);
            //return str;
            var k = new List<string>();

            //dont use distinct by to prevent reordering
            foreach (var x in Regex.Matches(Command, $@"\{ Parameter }\b(\w+)\b", RegexOptions.IgnoreCase).Cast<Match>().Select(x => x.Value))
                if (!k.Contains(x))
                    k.Add(x);
            return k;
        }

        /// <summary>
        /// Replace all Keys base on Parameter Specified
        /// </summary>
        /// <param name="CommandText"></param>
        /// <param name="row"></param>
        /// <param name="PassKey"></param>
        /// <returns></returns>
        public static string PassParameter(this string CommandText, Pair row, char PassKey = '@')
        {
            try
            {
                if (CommandText == null) return CommandText;
                foreach (KeyValuePair<string, object> r in row)
                {
                    var prm = Regex.Replace(PassKey.ToString(), @"[-\()\[\]{}^$*+.?|]", @"\$&", RegexOptions.IgnoreCase);
                    var data = r.Value == null ? DBNull.Value : r.Value;
                    CommandText = Regex.Replace(CommandText, $@"{prm}\b{ r.Key }\b", data.SQLFormat(), RegexOptions.IgnoreCase); // $@"(\s|\(|\,|^)(?:{ prm })(?=\s|\)|\,|$)"
                }
                return CommandText;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static string Replace(this string data, string repval, params string[] rplace)
        {
            foreach (string r in rplace) data = data.Replace(r, repval);
            return data;
        }

        #region Save

        public static void Save(this IQuery sql, DataTable dt, string TableName)
        {
            using (var g = new QueryMerge(sql as Query)) g.Save(dt, TableName);
        }

        public static void Save(this IQuery sql, string TableName, string ViewName, DataRow dr, bool RebuildTable = false)
        {
            using (var g = new QueryMerge(sql as Query)) g.Save(TableName, ViewName, dr, RebuildTable);
        }

        public static DataRow Save(this IQuery sql, string TableName, string ViewName, Pair values)
        {
            using (DataTable dt = new DataTable())
            {
                if (!values.ContainsKey("ID"))
                    dt.Columns.Add("ID", typeof(int));

                foreach (KeyValuePair<string, object> hh in values) dt.Columns.Add(hh.Key, (hh.Value.GetType() == typeof(DBNull)) ? typeof(string) : hh.Value.GetType());
                DataRow dr = dt.NewRow();

                foreach (KeyValuePair<string, object> hh in values)
                {
                    if (!values.ContainsKey("ID")) dr["ID"] = 0;
                    dr[hh.Key] = hh.Value;
                }
                dt.AcceptChanges();

                using (var g = new QueryMerge(sql as Query)) return g.Save(TableName, ViewName, dr);
            }
        }

        public static void Save(this IQuery query, DataRow dr, string table)
        {
            if (query.GetType() == typeof(Query))
                using (QueryMerge mrg = new QueryMerge(query as Query)) mrg.Save(table, "", dr);
            else
                throw new Exception("This Method is not yet Implemented for this Object");
        }

        public static void Save(this IQuery query, string TableName, Pair values) => query.Save(TableName, "", values);

        public static void Save(this IQuery query, string TableName, PairCollection values) => values.Each(x => query.Save(TableName, x));

        #endregion

        #region ExecQuery 

        public static DataSet ExecQuery(this string query, SqlConnectionStringBuilder args)
        {
            return args.ExecQuery(query);
        }

        public static DataSet ExecQuery(this string query, string[] parameters, object[] values, SqlConnectionStringBuilder args)
        {
            using (Query qry = new Query(args)) return qry.ExecQuery(query, parameters, values);
        }

        public static DataSet ExecQuery(this SqlConnectionStringBuilder args, string Command, params object[] values)
        {
            using (Query sql = new Query(args)) return sql.ExecQuery(Command, values);
        }

        public static DataSet ExecQuery(this SqlConnectionStringBuilder args, string Command)
        {
            using (Query sql = new Query(args)) return sql.ExecQuery(Command);

        }

        #endregion

        #region TableQuery 

        public static DataTable TableQuery(this string query, SqlConnectionStringBuilder args)
        {
            return args.TableQuery(query);
        }

        public static DataTable TableQuery(this string query, string[] parameters, object[] values, SqlConnectionStringBuilder args)
        {
            using (Query qry = new Query(args)) return qry.TableQuery(query, parameters, values);
        }

        public static DataTable TableQuery(this SqlConnectionStringBuilder args, string Command, params object[] values)
        {

            using (Query sql = new Query(args)) return sql.TableQuery(Command, values);

        }

        public static DataTable TableQuery(this SqlConnectionStringBuilder args, string Command)
        {
            using (Query sql = new Query(args)) return sql.TableQuery(Command);

        }

        #endregion

        #region ExecScalar

        public static object ExecScalar(this string query, SqlConnectionStringBuilder args)
        {
            return args.ExecScalar(query);
        }

        public static object ExecScalar(this string query, string[] parameters, object[] values, SqlConnectionStringBuilder args, CommandType type = CommandType.StoredProcedure)
        {
            return args.ExecScalar(query, parameters, values, type);
        }

        public static object ExecScalar(this SqlConnectionStringBuilder args, string Command, params object[] values)
        {

            using (Query sql = new Query(args)) return sql.ExecScalar(Command, values);

        }

        public static object ExecScalar(this SqlConnectionStringBuilder args, string Command, string[] Parameters, object[] values, CommandType type = CommandType.StoredProcedure)
        {

            using (Query sql = new Query(args)) return sql.ExecScalar(Command, Parameters, values, type);

        }

        public static T ExecScalar<T>(this SqlConnectionStringBuilder args, string Command, params object[] values) where T : class
        {

            using (Query sql = new Query(args)) return sql.ExecScalar<T>(Command, values);

        }

        public static T ExecScalar<T>(this SqlConnectionStringBuilder args, string Command, string[] Parameters, object[] values, CommandType type = CommandType.StoredProcedure) where T : class
        {

            using (Query sql = new Query(args)) return sql.ExecScalar<T>(Command, Parameters, values, type);

        }

        public static T ExecScalar<T>(this string query, SqlConnectionStringBuilder args) where T : class
        {
            return args.ExecScalar<T>(query);
        }

        public static T ExecScalar<T>(this string query, string[] parameters, object[] values, SqlConnectionStringBuilder args, CommandType type = CommandType.StoredProcedure) where T : class
        {
            return args.ExecScalar<T>(query, parameters, values, type);
        }

        #endregion

        #region ExecNonQuery

        public static void ExecNonQuery(this string query, SqlConnectionStringBuilder args)
        {
            args.ExecNonQuery(query);
        }

        public static void ExecNonQuery(this string query, string[] parameters, object[] values, SqlConnectionStringBuilder args, CommandType type = CommandType.StoredProcedure)
        {
            args.ExecNonQuery(query, parameters, values, type);
        }

        public static void ExecNonQuery(this SqlConnectionStringBuilder args, string Command, params object[] values)
        {

            using (Query sql = new Query(args)) sql.ExecNonQuery(Command, values);

        }

        public static void ExecNonQuery(this SqlConnectionStringBuilder args, string Command)
        {

            using (Query sql = new Query(args)) sql.ExecNonQuery(Command);

        }

        public static void ExecNonQuery(this SqlConnectionStringBuilder args, string Command, string[] Parameters, object[] values, CommandType type = CommandType.StoredProcedure)
        {

            using (Query sql = new Query(args)) sql.ExecNonQuery(Command, Parameters, values, type);

        }

        #endregion

        public static string EncryptA(this string vData, int vKey)
        {
            char a = '\0';
            string s = "";
            int j = 0;
            foreach (char a_loopVariable in vData)
            {
                a = a_loopVariable;
                j = (int)a; //Strings.Asc(a);
                a = (char)(j ^ vKey);  //Strings.Chr((j ^ vKey));
                s += a;
            }
            return s;
        }

        public static string SQLInsertString(this DataTable dt, string TableName = "") => QueryCom.BuildInsertString(dt, TableName == "" ? dt.TableName : TableName);

        public static void FillTable(this DataTable pTable, string query, SqlConnection vConnection, bool FillSchema = false)
        {

            try
            {
                using (SqlDataAdapter a = new SqlDataAdapter(query, vConnection))
                {
                    switch (vConnection.State)
                    {
                        case ConnectionState.Closed:
                        case ConnectionState.Broken:
                            vConnection.Open();
                            break;
                    }
                    if (FillSchema)
                    {
                        a.FillSchema(pTable, SchemaType.Mapped);
                    }
                    a.Fill(pTable);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                vConnection.Close();
                GC.Collect();
            }
        }

        public static void FillTable(this DataTable pTable, string query, SqlTransaction vTransaction, bool FillSchema = false)
        {
            try
            {
                using (SqlDataAdapter a = new SqlDataAdapter(query, vTransaction.Connection))
                {
                    a.SelectCommand.Transaction = vTransaction;
                    if (FillSchema)
                    {
                        a.FillSchema(pTable, SchemaType.Mapped);
                    }
                    a.Fill(pTable);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        [MTAThread]
        public static void Parameterize(this SqlCommand mCmd, string[] Parameter, object[] Value)
        {
            try
            {
                mCmd.Parameters.Clear();
                if (Parameter != null)
                {
                    for (int i = 0; i < Parameter.Length; i++)
                    {
                        if (Value[i].GetType().Name.ToUpper() == "DOUBLE")
                        {
                            mCmd.Parameters.Add(Parameter[i], SqlDbType.Float);
                            mCmd.Parameters[Parameter[i]].Value = Value[i];
                        }
                        else
                            mCmd.Parameters.AddWithValue(Parameter[i], Value[i]);

                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static void Interpolate(ref string SQLScript, string Key, string Value)
        {
            Value = Regex.Replace(Value, "[$]", "$$$$", RegexOptions.IgnoreCase);
            SQLScript = Regex.Replace(SQLScript, $@"{{{{\b{ Key }\b}}}}", Value, RegexOptions.IgnoreCase);
        }
    }
}
