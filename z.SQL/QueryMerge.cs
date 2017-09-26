using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using z.Data;

namespace z.SQL
{
    public class QueryMerge : IDisposable
    {

        private Query q;

        public QueryMerge(Query query)
        {
            this.q = query;
        }

        public void BeginTran()
        {
            // q.BeginTran();
        }

        public void CommitTran()
        {
            //  q.CommitTran();
        }

        public void RollbackTran()
        {
            // q.RollbackTran();
        }

        public Tuple<DataTable, List<SchemaIndexes>> ReBuildDataSchema(string Table, DataTable dt)
        {
            var indexes = new List<SchemaIndexes>();
            using (DataTable sdt = q.mArgs.TableQuery($"Select top 0 * from dbo.{Table}")) //q.ExecQuery(string.Format("Select top 0 * from dbo.{0}", Table)).Tables[0]
            {
                using (DataTable rdt = new DataTable(Table))
                {
                    foreach (DataColumn dc in sdt.Columns) { rdt.Columns.Add(dc.ColumnName, dc.DataType); }

                    int i = 0;
                    foreach (DataRow dr in dt.Rows)
                    {
                        DataRow rdr = rdt.NewRow();
                        indexes.Add(new SchemaIndexes()
                        {
                            Index = i,
                            ID_Orig = Convert.ToInt32(dr["XXXID_" + Table.Substring(1)].IsNull(0)),
                            ID_Table = Convert.ToInt32(dr["ID"].IsNull(0))
                        });

                        foreach (DataColumn dc in sdt.Columns) { rdr[dc.ColumnName] = dr[dc.ColumnName]; }
                        rdt.Rows.Add(rdr);
                        i++;
                    }

                    return new Tuple<DataTable, List<SchemaIndexes>>(rdt, indexes);
                }
            }
        }

        public DataTable ReBuildDataTable(string Table, DataTable dt)
        {
            using (DataTable sdt = q.mArgs.TableQuery($"SELECT * FROM dbo.fGetTableDef({  QueryCom.SQLFormat(Table) }) WHERE Computed = 0 and  datatype != 'uniqueidentifier' ORDER BY Column_id"))//q.ExecQuery(string.Format("SELECT * FROM dbo.fGetTableDef({0}) WHERE Computed = 0 ORDER BY Column_id", QueryCom.SQLFormat(Table))).Tables[0]
            {
                using (DataTable rdt = new DataTable(Table))
                {

                    foreach (DataRow dc in sdt.Rows)
                    {
                        if (!dt.Columns.Contains(dc["ColumnName"].ToString())) continue;
                        rdt.Columns.Add(dc["ColumnName"].ToString(), QueryCom.GetDbType(dc["DataType"].ToString()));
                    }

                    //check if ID is included
                    if (!rdt.Columns.Contains("ID"))
                    {
                        rdt.Columns.Add("ID", typeof(int));
                        rdt.Columns["ID"].DefaultValue = 0;
                    }

                    foreach (DataRow dr in dt.Rows)
                    {
                        DataRow rdr = rdt.NewRow();
                        foreach (DataRow dc in sdt.Rows)
                        {
                            if (!dt.Columns.Contains(dc["ColumnName"].ToString())) continue;
                            object val = DBNull.Value;
                            string name = dc["ColumnName"].ToString();
                            if (!dt.Columns.Contains(name))
                            {
                                if (dc["Default"] != DBNull.Value)
                                {
                                    if (QueryCom.GetDbType(dc["DataType"].ToString()) == typeof(bool))
                                    {
                                        val = Convert.ToBoolean(Convert.ToInt32(dc["Default"].ToString().Replace("(", "").Replace(")", "")));
                                    }
                                    else
                                    {
                                        val = dc["Default"];
                                    }
                                }
                            }
                            else
                            {
                                if (dr[name] == DBNull.Value)
                                {
                                    if (dc["Default"] != DBNull.Value)
                                    {
                                        if (QueryCom.GetDbType(dc["DataType"].ToString()) == typeof(bool))
                                        {
                                            val = Convert.ToBoolean(Convert.ToInt32(dc["Default"].ToString().Replace("(", "").Replace(")", "")));
                                        }
                                        else
                                        {
                                            val = dc["Default"];
                                        }
                                    }
                                }
                                else
                                {
                                    val = dr[name];
                                }
                            }

                            rdr[name] = val;
                        }
                        rdt.Rows.Add(rdr);
                    }
                    return rdt; //.Rows[0];
                }
            }
        }

        public DataRow ReBuildDataSchema(string Table, DataRow dr)
        {
            using (DataTable sdt = q.mArgs.TableQuery($"SELECT * FROM dbo.fGetTableDef({ QueryCom.SQLFormat(Table) }) WHERE Computed = 0  and datatype != 'uniqueidentifier' ORDER BY Column_id"))
            {
                using (DataTable rdt = new DataTable(Table))
                {
                    DateTime b = DateTime.Now;
                    foreach (DataRow dc in sdt.Rows)
                    {
                        rdt.Columns.Add(dc["ColumnName"].ToString(), QueryCom.GetDbType(dc["DataType"].ToString()));
                        if ((rdt.Columns[dc["ColumnName"].ToString()].DataType == typeof(DateTime) && DateTime.TryParse(dc["Default"].ToString(), out b)) || rdt.Columns[dc["ColumnName"].ToString()].DataType != typeof(DateTime))
                            rdt.Columns[dc["ColumnName"].ToString()].DefaultValue = dc["Default"];
                        rdt.Columns[dc["ColumnName"].ToString()].AllowDBNull = Convert.ToBoolean(dc["Nullable"]);
                    }
                    DataRow rdr = rdt.NewRow();
                    foreach (DataRow dc in sdt.Rows)
                    {
                        object val = DBNull.Value;
                        string name = dc["ColumnName"].ToString();
                        if (dr.Table.Columns.Contains(name))
                        {
                            val = dr[name];
                            val = (val.ToString() == "NULL" || val.ToString() == "") ? DBNull.Value : val;
                            if (!rdt.Columns[name].AllowDBNull && val == DBNull.Value)
                            {
                                if (rdt.Columns[name].DataType == typeof(DateTime))
                                    val = DateTime.Now; //set by default
                                else if (rdt.Columns[name].DefaultValue != DBNull.Value)
                                    val = rdt.Columns[name].DefaultValue;
                            }
                            rdr[name] = val;
                        }
                    }
                    rdt.Rows.Add(rdr);
                    return rdt.Rows[0];
                }
            }
        }

        public void Save(DataTable dt, string Tablename)
        {
            var rdt = ReBuildDataTable(Tablename, dt);
            foreach (DataRow idt in rdt.Rows) Save(Tablename, "", idt);
        }

        public Tuple<DataTable, List<SchemaIndexes>> Save(string Table, string View, DataTable dt, bool CheckRowState = true, List<SchemaIndexes> ParentIndexes = null, List<SchemaIndexes> UpdateIndexes = null, string ParentColumn = null)
        {
            DataTable rdt = new DataTable(dt.TableName);

            if (ParentIndexes != null && ParentColumn == null) throw new Exception("Parent Column Not Specified.");

            foreach (DataColumn dc in dt.Columns) { rdt.Columns.Add(dc.ColumnName, dc.DataType); }

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                if (ParentIndexes != null)
                {
                    var p = (from g in ParentIndexes where g.ID_Table == Convert.ToInt32(dt.Rows[i][ParentColumn]) select g).SingleOrDefault();
                    if (p != null) dt.Rows[i][ParentColumn] = p.ID_Orig;
                }

                DataRow idr = this.Save(Table, View, dt.Rows[i], CheckRowState);

                if (UpdateIndexes != null)
                {
                    var j = (from g in UpdateIndexes where g.Index == i select g).SingleOrDefault();
                    j.ID_Orig = Convert.ToInt32(idr["ID"]);
                }

                rdt.ImportRow(idr);
            }

            return new Tuple<DataTable, List<SchemaIndexes>>(rdt, UpdateIndexes);
        }

        public DataRow Save(string Table, string View, DataRow dr, bool RebuildTable = false)
        {
            if (RebuildTable) dr = ReBuildDataSchema(Table, dr);

            Dictionary<string, object> obj = new Dictionary<string, object>();
            List<string> lst = new List<string>();
            List<string> cols = new List<string>();
            List<string> upd = new List<string>();
            Dictionary<string, object> ins = new Dictionary<string, object>();

            try
            {
                lst.Add("DECLARE @tbl AS TABLE (ID INT)");
                lst.Add(string.Format("Merge dbo.{0} as target", Table));

                foreach (DataColumn dc in dr.Table.Columns)
                {
                    obj.Add(string.Format("@{0}", dc.ColumnName), dr[dc.ColumnName]);
                    cols.Add(string.Format("@{0} [{0}]", dc.ColumnName));
                    //cols.Add(string.Format("{0} [{1}]", QueryCom.SQLFormat(dr[dc.ColumnName], dc.DefaultValue), dc.ColumnName));

                    if (dc.ColumnName.ToUpper() == "ID") continue;
                    upd.Add(string.Format("[{0}] = Source.[{0}]", dc.ColumnName));
                    ins.Add(string.Format("[{0}]", dc.ColumnName), string.Format("Source.[{0}]", dc.ColumnName));
                }

                //Create
                lst.Add(string.Format("Using(select {0}) as Source On target.ID = Source.ID", string.Join(", ", cols.ToArray()))); //cols.ToArray()
                //Insert
                lst.Add(string.Format("When Not Matched by target Then Insert ({0}) values ({1})", string.Join(", ", ins.Keys.ToArray()), string.Join(", ", ins.Values.ToArray())));
                //Update
                lst.Add(string.Format("When Matched Then Update Set {0}", string.Join(", ", upd.ToArray())));
                //Delete
                // lst.Add(string.Format("When Not Matched by Source Then Delete;"));
                lst.Add("OUTPUT INSERTED.ID INTO @tbl;");

                if (View != "")
                {
                    //Select
                    lst.Add(string.Format("Select * from dbo.{0} a where a.ID = (SELECT MAX(ID) FROM @tbl)", View));

                    DataTable dt;

                    dt = q.ExecQuery(string.Join("\r\n", lst.ToArray()), obj.Keys.ToArray(), obj.Values.ToArray()).Tables[0];

                    if (dt.Rows.Count == 0) throw new Exception("Data Could not Save");
                    return dt.Rows[0];
                }
                else
                {
                    q.ExecNonQuery(string.Join("\r\n", lst.ToArray()), obj.Keys.ToArray(), obj.Values.ToArray(), CommandType.Text);
                    return null;
                }
            }
            catch (Exception ex)
            {
                //throw new Exception(ex.Message + "->" + string.Join("\r\n", lst.ToArray()));
                throw ex;
            }
            finally
            {
                obj = null;
                cols = null;
                lst = null;
                upd = null;
                ins = null;
            }
        }

        public void SetParentID(DataTable dt, string ParentColumn, object value)
        {
            for (int i = 0; i < dt.Rows.Count; i++)
            {
                dt.Rows[i][ParentColumn] = value;
            }
        }

        public void Delete(DataTable dt, string TableName, params string[] keyColumns)
        {
            List<string> p = new List<string>();
            List<string> k;
            try
            {
                foreach (DataRow dr in dt.Rows)
                {
                    k = new List<string>();
                    foreach (string keys in keyColumns)
                    {
                        k.Add(string.Format("{0} = {1}", keys, QueryCom.SQLFormat(dr[keys])));
                    }
                    p.Add(string.Format("Delete from dbo.{0} Where {1};", TableName, string.Join(" AND ", k.ToArray())));
                }

                if (string.Join("\r\n", p.ToArray()).Trim() != "")
                {
                    q.ExecNonQuery(string.Join("\r\n", p.ToArray()));
                }

            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                p = null;
                k = null;
            }
        }

        protected virtual void Dispose(bool disposing)
        {
            if (disposing) q.Dispose();
            q = null;
        }

        ~QueryMerge()
        {
            Dispose(false);
        }

        public void Dispose()
        {
            Dispose(true);
            GC.Collect();
            GC.SuppressFinalize(this);
        }
    }
}
