using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.VisualBasic;
using System.Data;

namespace z.SQL
{
    public static class QueryCom
    {
        public static bool Connect(Query.QueryArgs ConArgs)
        {
            try
            {
                bool b = false;
                using (Query sql = new Query(ConArgs))
                {
                    b = true;
                }
                return b;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static void ExecNonQuery(string scrpt, Query.QueryArgs ConArgs)
        {
            using (Query sql = new Query(ConArgs))
            {
                sql.ExecNonQuery(scrpt);
            }
        }

        public static void ExecNonQuery(string scrpt, string[] Param, object[] Value, Query.QueryArgs ConArgs)
        {
            using (Query sql = new Query(ConArgs))
            {
                sql.ExecNonQuery(scrpt, Param, Value);
            }
        }

        public static object ExecScalar(string scrpt, Query.QueryArgs ConArgs)
        {
            using (Query sql = new Query(ConArgs))
            {
                return sql.ExecScalar(scrpt);
            }
        }

        public static object ExecScalar(string scrpt, string[] Param, object[] Value, Query.QueryArgs ConArgs)
        {
            using (Query sql = new Query(ConArgs))
            {
                return sql.ExecScalar(scrpt, Param, Value);
            }
        }

        public static System.Data.DataSet ExecQuery(string scrpt, Query.QueryArgs ConArgs)
        {
            using (Query sql = new Query(ConArgs))
            {
                return sql.ExecQuery(scrpt);
            }
        }

        public static System.Data.DataSet ExecQuery(string scrpt, string[] Param, object[] Value, Query.QueryArgs ConArgs)
        {
            using (Query sql = new Query(ConArgs))
            {
                return sql.ExecQuery(scrpt, Param, Value);
            }
        }

        public static string SQLFormat(object o, string escapedelimiter = "''")
        {
            string s;
            string t;
            if (Information.IsDBNull(o) || Information.IsNothing(o))
            {
                s = "NULL";
            }
            else
            {
                t = Strings.UCase(Information.TypeName(o));
                switch (t)
                {
                    case "DATE":
                        s = o.ToString();
                        s = Strings.Replace(s, "'", escapedelimiter);
                        s = String.Format("'{0}'", s);
                        break;
                    case "STRING":
                    case "GUID":
                        s = o.ToString();
                        s = Strings.Replace(s, "'", escapedelimiter);
                        s = String.Format("'{0}'", s);
                        break;
                    case "BOOLEAN":
                        s = (Convert.ToBoolean(o)) ? "1" : "0";
                        break;
                    default:
                        s = o.ToString();
                        break;
                }
            }
            return s;
        }
         

        public static DataRow SaveDataRowAndFill(DataRow dr, string tablename, string KeyColumn, QueryOLE qry, bool Parameterize = false)
        {

            try
            {
                DataRow mdr = dr;
                List<string> col = new List<string>();
                List<object> val = new List<object>();
                foreach (DataColumn dc in dr.Table.Columns)
                {
                    if (dc.ColumnName == KeyColumn) continue;
                    val.Add(dr[dc.ColumnName]);
                    col.Add(dc.ColumnName);
                }

                //update
                int ID = (dr[KeyColumn] == DBNull.Value) ? 0 : Convert.ToInt32(dr[KeyColumn]);
                if (ID > 0)
                {
                    mdr = UpdateRow(tablename, col.ToArray(), val.ToArray(), KeyColumn, ID, qry, true, Parameterize);
                }
                else
                {
                    mdr = InsertRow(tablename, col.ToArray(), val.ToArray(), qry, KeyColumn, true, Parameterize);
                }

                return mdr;
            }
            catch (Exception ex)
            {
                throw ex;
            }

        }

        public static DataRow SaveDataRowAndFill(DataRow dr, string tablename, string KeyColumn, QueryODBC qry)
        {
            try
            {
                DataRow mdr = dr;
                List<string> col = new List<string>();
                List<object> val = new List<object>();
                foreach (DataColumn dc in dr.Table.Columns)
                {
                    if (dc.ColumnName == KeyColumn) continue;
                    val.Add(dr[dc.ColumnName]);
                    col.Add(dc.ColumnName);
                }

                //update
                int ID = (dr[KeyColumn] == DBNull.Value) ? 0 : Convert.ToInt32(dr[KeyColumn]);
                if (ID > 0)
                {
                    mdr = UpdateRow(tablename, col.ToArray(), val.ToArray(), KeyColumn, ID, qry, true);
                }
                else
                {
                    mdr = InsertRow(tablename, col.ToArray(), val.ToArray(), qry, KeyColumn, true);
                }

                return mdr;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        //public static DataRow SaveDataRowAndFill(DataRow dr, string tablename, string KeyColumn, QueryLite qry)
        //{

        //    try
        //    {
        //        DataRow mdr = dr;
        //        List<string> col = new List<string>();
        //        List<object> val = new List<object>();
        //        foreach (DataColumn dc in dr.Table.Columns)
        //        {
        //            if (dc.ColumnName == KeyColumn) continue;
        //            val.Add(dr[dc.ColumnName]);
        //            col.Add(dc.ColumnName);
        //        }

        //        //update
        //        int ID = (dr[KeyColumn] == DBNull.Value) ? 0 : Convert.ToInt32(dr[KeyColumn]);
        //        if (ID > 0)
        //        {
        //            mdr = UpdateRow(tablename, col.ToArray(), val.ToArray(), KeyColumn, ID, qry);
        //        }
        //        else
        //        {
        //            mdr = InsertRow(tablename, col.ToArray(), val.ToArray(), qry, KeyColumn);
        //        }

        //        return mdr;
        //    }
        //    catch (Exception ex)
        //    {
        //        throw ex;
        //    }

        //}

       

        public static DataRow InsertRow(string tablename, string[] Columns, object[] Values, QueryOLE qry, string KeyColumn = "ID", bool ReturnData = false, bool Parameterize = false)
        {
            try
            {
                DataRow dr = null;
                List<object> myQryStr = new List<object>();
                List<string> myColumns = new List<string>();

                //revalidate
                foreach (object j in Columns)
                {
                    myColumns.Add(string.Format("[{0}]", j));
                }

                foreach (object j in Values)
                {
                    if (Parameterize)
                    {
                        myQryStr.Add(j);
                    }
                    else
                    {
                        myQryStr.Add(string.Format("'{0}'", j));
                    }
                }

                if (Parameterize)
                {
                    List<string> myQryColumns = new List<string>();
                    foreach (object j in Columns)
                    {
                        myQryColumns.Add(string.Format("@{0}", j));
                    }

                    string k = string.Format("Insert into {0} ({1}) values ({2})",
                                        tablename,
                                        string.Join(",", myColumns.ToArray()),
                                        string.Join(",", myQryColumns.ToArray()));
                    qry.ExecNonQuery(k, myQryColumns.ToArray(), myQryStr.ToArray());
                }
                else
                {
                    string k = string.Format("Insert into {0} ({1}) values ({2})",
                                        tablename,
                                        string.Join(",", myColumns.ToArray()),
                                        string.Join(",", myQryStr.ToArray()));
                    qry.ExecNonQuery(k);
                }


                if (ReturnData)
                {
                    object ID = qry.ExecScalar(string.Format("Select MAX({0}) from {1}", KeyColumn, tablename));

                    if (ID != null)
                    {
                        dr = qry.ExecQuery(string.Format("Select * from {0} Where {1} = {2}", tablename, KeyColumn, ID)).Tables[0].Rows[0];
                    }
                }

                return dr;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static DataRow InsertRow(string tablename, string[] Columns, object[] Values, QueryODBC qry, string KeyColumn = "ID", bool ReturnData = false)
        {
            try
            {
                DataRow dr = null;
                List<string> myQryStr = new List<string>();
                List<string> myColumns = new List<string>();

                //revalidate
                foreach (object j in Columns)
                {
                    myColumns.Add(string.Format("[{0}]", j));
                }

                foreach (object j in Values)
                {
                    myQryStr.Add(SQLFormat(j));
                }

                string k = string.Format("Insert into {0} ({1}) values ({2})",
                                        tablename,
                                        string.Join(",", myColumns.ToArray()),
                                        string.Join(",", myQryStr.ToArray()));

                qry.ExecNonQuery(k);

                if (ReturnData)
                {
                    object ID = qry.ExecScalar(string.Format("Select MAX({0}) ID from {1}", KeyColumn, tablename));

                    if (ID != null)
                    {
                        dr = qry.ExecQuery(string.Format("Select * from {0} Where {1} = {2}", tablename, KeyColumn, ID)).Tables[0].Rows[0];
                    }
                }

                return dr;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static DataRow InsertRow(string tablename, string[] Columns, object[] Values, QueryMy qry, string KeyColumn = "ID")
        {
            try
            {
                DataRow dr = null;

                string k = string.Format("Insert into {0} ({1}) values ({2}); Select * from {0} where {3} = LAST_INSERT_ID();",
                                        tablename,
                                        string.Join(",", Columns),
                                        string.Join(",", Values.Select(x => x.SQLFormat()).ToArray()),
                                        KeyColumn);

                dr = qry.ExecQuery(k).Tables[0].Rows[0];

                return dr;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

       

        public static DataRow UpdateRow(string tablename, string[] Columns, object[] Values, string KeyColumn, object value, QueryOLE qry, bool ReturnData = false, bool parameterize = false)
        {
            try
            {
                DataRow dr = null;
                List<string> myQryStr = new List<string>();

                //revalidate
                for (int i = 0; i < Columns.Length; i++)
                {
                    if (parameterize)
                    {
                        myQryStr.Add(string.Format("[{0}] = @{1}", Columns[i], Columns[i]));
                    }
                    else
                    {
                        myQryStr.Add(string.Format("[{0}] = {1}", Columns[i], SQLFormat(Values[i])));
                    }
                }

                string k = string.Format("update {0} set {1} where {2} = {3}",
                                        tablename,
                                        string.Join(",", myQryStr.ToArray()),
                                        KeyColumn,
                                        value);

                if (parameterize)
                {
                    for (int i = 0; i < Columns.Length; i++)
                    {
                        Columns[i] = string.Format("@{0}", Columns[i]);
                    }
                    qry.ExecNonQuery(k, Columns, Values);
                }
                else
                {
                    qry.ExecNonQuery(k);
                }

                if (ReturnData)
                {
                    dr = qry.ExecQuery(string.Format("Select * from {0} Where {1} = {2}", tablename, KeyColumn, value)).Tables[0].Rows[0];
                }

                return dr;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static DataRow UpdateRow(string tablename, string[] Columns, object[] Values, string KeyColumn, object value, QueryODBC qry, bool ReturnData = false)
        {
            try
            {
                DataRow dr = null;
                List<string> myQryStr = new List<string>();

                //revalidate
                for (int i = 0; i < Columns.Length; i++)
                {
                    myQryStr.Add(string.Format("{0} = '{1}'", Columns[i], Values[i]));
                }

                string k = string.Format("update {0} set {1} where {2} = {3}",
                                        tablename,
                                        string.Join(",", myQryStr.ToArray()),
                                        KeyColumn,
                                        value);

                qry.ExecNonQuery(k);

                if (ReturnData)
                {
                    dr = qry.ExecQuery(string.Format("Select * from {0} Where {1} = {2}", tablename, KeyColumn, value)).Tables[0].Rows[0];
                }

                return dr;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static DataRow UpdateRow(string tablename, string[] Columns, object[] Values, string KeyColumn, object value, QueryMy qry)
        {
            try
            {
                DataRow dr = null;
                List<string> myQryStr = new List<string>();

                ////revalidate
                for (int i = 0; i < Columns.Length; i++)
                    myQryStr.Add(string.Format("{0} = {1}", Columns[i], SQLFormat(Values[i])));

                string k = string.Format("update {0} set {1} where {2} = {3}",
                                        tablename,
                                        string.Join(",", myQryStr.ToArray()),
                                        KeyColumn,
                                        value);

                qry.ExecNonQuery(k);

                dr = qry.ExecQuery(string.Format("Select * from {0} Where {1} = {2}", tablename, KeyColumn, value)).Tables[0].Rows[0];

                return dr;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
         
        public static DataTable AddNull(DataTable dt)
        {
            DataTable idt = dt.Clone();

            DataRow idr = idt.NewRow();
            idr["ID"] = DBNull.Value;
            idr["Name"] = "-";
            idt.Rows.Add(idr);

            foreach (DataRow dr in dt.Rows)
            {
                idr = idt.NewRow();
                foreach (DataColumn dc in dt.Columns)
                {
                    idr[dc.ColumnName] = dr[dc.ColumnName];
                }
                idt.Rows.Add(idr);
            }

            return idt;
        }

        public static List<QueryParameters> GetProcedureParameters(string ProcName, Query.QueryArgs Connection)
        {
            List<QueryParameters> param = new List<QueryParameters>();
            using (Query qry = new Query(Connection))
            {
                DataTable dt = qry.ExecQuery(string.Format("select ORDINAL_POSITION, PARAMETER_NAME, DATA_TYPE from information_schema.parameters where specific_name= {0}", SQLFormat(ProcName))).Tables[0];
                foreach (DataRow dr in dt.Rows)
                {
                    param.Add(new QueryParameters()
                    {
                        ORDINAL_POSITION = Convert.ToInt32(dr["ORDINAL_POSITION"]),
                        PARAMETER_NAME = dr["PARAMETER_NAME"].ToString(),
                        DATA_TYPE = dr["DATA_TYPE"].ToString()
                    }
                        );
                }
            }

            return param;
        }

        public static string ProcedureParameter(string[] Param, object[] Value)
        {
            try
            {
                if (Param.Length != Value.Length) throw new Exception("Parameter and Values length must match");

                List<string> str = new List<string>();

                for (int i = 0; i < Param.Length; i++)
                {
                    str.Add(string.Format("{0} = {1}", Param[i], SQLFormat(Value[i])));
                }

                return string.Join(", ", str.ToArray());
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public static string BuildCommandString(string tbl, string[] Param, object[] Value, string[] PrimaryKey = null, bool UpdateOnly = false, string[] excludedColumns = null)
        {
            List<string> str = new List<string>();
            Dictionary<string, object> dict = new Dictionary<string, object>();
            Dictionary<string, object> primo = new Dictionary<string, object>();
            List<string> strwhere = new List<string>();

            try
            {

                if (Param.Length != Value.Length) throw new Exception("Parameter and Values lenght must match");

                //remove Excluded Columns // e.g Computed
                if (excludedColumns != null) Param = Param.Except(excludedColumns).ToArray();

                //create combi
                for (int i = 0; i < Param.Length; i++)
                {
                    dict.Add(Param[i], SQLFormat(Value[i]));
                }

                str.Add("DECLARE @j TABLE (ID INT);");

                if (PrimaryKey != null && PrimaryKey.Length > 0)
                {

                    foreach (string prm in PrimaryKey)
                    {
                        primo.Add(prm, (from hh in dict.AsEnumerable()
                                        where hh.Key == prm
                                        select hh.Value).SingleOrDefault());
                    }

                    if (dict.Count == 0) goto tt;

                    //update
                    str.Add(string.Format("UPDATE dbo.{0}", tbl));

                    List<string> istr = new List<string>();
                    foreach (KeyValuePair<string, object> oj in dict)
                    {

                        if ((from prim in PrimaryKey
                             where prim == oj.Key
                             select prim).Count() > 0)
                            continue;

                        istr.Add(string.Format("{0} = {1}", oj.Key, oj.Value));
                    }

                    str.Add(string.Format("SET {0}", string.Join(", ", istr.ToArray())));

                    foreach (KeyValuePair<string, object> prmcomb in primo)
                    {
                        strwhere.Add(string.Format("{0} = {1}", prmcomb.Key, prmcomb.Value));
                    }

                    str.Add(string.Format("WHERE {0};", string.Join(" AND ", strwhere.ToArray())));

                    str.Add("INSERT INTO @j");
                    str.Add(string.Format("SELECT ID FROM dbo.{0} WHERE {1};", tbl, string.Join(" AND ", strwhere.ToArray())));
                }

                tt:

                //Return if Update only is set
                if (PrimaryKey != null && PrimaryKey.Length > 0 && UpdateOnly == true)
                {
                    //return
                    str.Add("SELECT TOP 1 ID FROM @j;");
                    return string.Join("\r\n", str.ToArray());
                }

                //remove primary ID
                int iexist = (from ggx in dict where ggx.Key == "ID" select ggx).Count();
                if (iexist > 0) { dict.Remove("ID"); }

                //insert
                str.Add(string.Format("INSERT INTO dbo.{0} ({1})", tbl, string.Join(", ", dict.Keys.ToArray())));
                str.Add("OUTPUT INSERTED.ID INTO @j");
                str.Add(string.Format("SELECT {0}", string.Join(", ", dict.Values.ToArray())));

                if (PrimaryKey != null && PrimaryKey.Length > 0)
                {
                    str.Add(string.Format("WHERE NOT EXISTS(SELECT * FROM dbo.{0} WHERE {1});", tbl, string.Join(" AND ", strwhere.ToArray())));
                }
                else
                {
                    str.Add(";");//break the line
                }

                //return
                str.Add("SELECT TOP 1 ID FROM @j;");
                return string.Join("\r\n", str.ToArray());
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                str = null;
                dict = null;
                primo = null;
                strwhere = null;

                GC.Collect();
            }

        }

        public static string BuildInsertString(DataTable dt, string TableName)
        {
            if (dt.Rows.Count == 0)
                return "";
            return $"Insert Into {TableName} ({string.Join(",", dt.Columns.Cast<DataColumn>().Select(x => x.ColumnName)) }) values { string.Join(",", dt.Rows.Cast<DataRow>().Select(x => $"({string.Join(",", x.ItemArray.Select(y => y.SQLFormat()).ToArray())})")) };";
        }

        public static Tuple<string[], object[]> BuildRowArray(DataRow dr)
        {
            List<string> cols = new List<string>();
            List<object> obj = new List<object>();

            foreach (DataColumn dc in dr.Table.Columns)
            {
                cols.Add(dc.ColumnName);
                obj.Add(dr[dc.ColumnName]);
            }

            return new Tuple<string[], object[]>(cols.ToArray(), obj.ToArray());
        }

        public static DataRow BuildDataRow(string[] Columns, object[] Data)
        {
            using (DataTable dt = new DataTable())
            {
                foreach (string cols in Columns)
                {
                    dt.Columns.Add(cols, typeof(object));
                }

                DataRow dr = dt.NewRow();

                for (int i = 0; i < Data.Length; i++)
                {
                    dr[i] = Data[i];
                }

                return dr;
            }
        }

        public static Type GetDbType(string Name)
        {
            Type tp = typeof(string);
            switch (Name.ToLower())
            {
                case "int": tp = typeof(int); break;
                case "bit": tp = typeof(bool); break;
                case "bigint": tp = typeof(Int64); break;
                case "decimal": tp = typeof(decimal); break;
                case "datetime": tp = typeof(DateTime); break;
            }
            return tp;
        }

        [Obsolete]
        public static List<string> QrybaseParameter(string Qry, string Delimiter = "@")
        {
            List<string> lst = new List<string>();

            if (Qry != "" && Qry.Contains(Delimiter))
            {
                string str = Qry;
                string[] param = str.Split(new string[] { Delimiter }, StringSplitOptions.None);
                for (int i = 0; i < param.Length; i++)
                {
                    param[i] = Delimiter + param[i];
                }

                for (int i = 1; i < param.Length; i++)
                {
                    string curVal = "";
                    char[] invalidchar = { ',', ' ', ')' };

                    foreach (char c in invalidchar)
                    {
                        string[] sdr = (curVal == "") ? param[i].Split(c) : curVal.Split(c);
                        for (int o = 0; o < sdr.Length; o++)
                        {
                            if (sdr[o].Contains(Delimiter))
                            {
                                curVal = sdr[o];
                                continue;
                            }
                        }
                    }

                    string[] invalidStr = { "insert", "update", "delete", "truncate" };
                    if (Char.IsLetter(Convert.ToChar(curVal.Substring(Delimiter.Length, 1))))
                    {
                        bool valid = true;
                        foreach (string atrinv in invalidStr)
                        {
                            if (curVal.ToLower().Contains(atrinv))
                            {
                                valid = false;
                                break;
                            }
                        }

                        if (valid == true)
                        {
                            lst.Add(curVal);
                        }
                    }

                }
            }

            return lst.Distinct().ToList();
        }
    }
}
