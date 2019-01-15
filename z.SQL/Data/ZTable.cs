using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using z.Data;
using static z.SQL.Extensions;

namespace z.SQL.Data
{
    /// <summary>
    /// LJ 20160505
    /// For Web
    /// </summary>
    public class ZTable : XTableWithSchema, IDisposable
    {
        protected string ScrptSelect;
        public SqlDataAdapter Adapter;
        public SqlCommand SelectCommand;
        private int IndexID = 0;

        private bool HasColumnIndex = false;

        protected string SQLScript; //{ get; set; }

        public List<int?> DeletedID { protected get; set; } = new List<int?>();

        protected bool UseViewForSelect = true;

        public ZTable(SqlConnection conn, string tableName, bool UseViewForSelect = true) : base(conn, tableName)
        {
            Adapter = new SqlDataAdapter();
            SelectCommand = new SqlCommand("", conn);
            var dtname = UseViewForSelect ? "v" + this.TableName.Substring(1) : this.TableName;
            Adapter.SelectCommand = new SqlCommand("SELECT * FROM " + dtname, conn);
            Adapter.FillSchema(this, SchemaType.Mapped);

            Adapter.SelectCommand = SelectCommand;
            this.UseViewForSelect = UseViewForSelect;

            AfterNew();
            BuildScript();
        }

        public ZTable(IQueryArgs args, string tablename, bool UseViewForSelect = true) : this(new SqlConnection((args as Query.QueryArgs).GetConnectionString()), tablename, UseViewForSelect)
        {
        }

        public void Load(params int[] ID)
        {
            if (ID == null)
                SelectCommand.CommandText = ScrptSelect;
            else
                SelectCommand.CommandText = $"{ ScrptSelect } Where ID in ({ ID.Join() })";
            Adapter.Fill(this);
        }

        protected ZTableColumnModel BuildScriptCore()
        {
            var g = new ZTableColumnModel();

            var col = this.SchemaTable.Rows.Cast<DataRow>().Where(x => x["ID_ColumnSource"].ToInt32() == 1 && !x["Computed"].ToBool() && x["DataType"].ToString().ToLower() != "uniqueidentifier").OrderBy(x => x["SeqNo"].ToInt32());
            g.ColString = col.Where(x => x["Identity"].ToInt32() == 0).Select(x => new ColData()
            {
                Insert = $"[{x["ColumnName"].ToString()}]",
                OutInsert = $"S.[{x["ColumnName"].ToString()}]",
                TargetInsert = $"T.[{x["ColumnName"].ToString()}]",
                Update = $"[{x["ColumnName"]}] = S.[{x["ColumnName"]}]"
            });

            g.SchemaString = col.Select(x =>
            {
                string sd = $"[{ x["ColumnName"].ToString() }] ";

                if (x["DataType"].ToString() == "varchar")
                {
                    string lngth = (x["DataType"].ToString() == "varchar" && x["Length"].ToInt32() == -1) ? "MAX" : x["Length"].ToString();
                    sd += $"{x["DataType"].ToString()}({ lngth })";
                }
                else if (x["DataType"].ToString() == "decimal")
                    sd += $"{x["DataType"].ToString()}({ x["Precision"].ToInt32() }, { x["Scale"].ToInt32() })";
                else
                    sd += x["DataType"].ToString();

                sd += x["AllowDBNull"].ToBool() ? " NULL " : " NOT NULL ";
                sd += x["DefaultValue"].IsNull("").ToString() != "" ? "DEFAULT " + x["DefaultValue"].ToString() : "";
                return sd;
            });

            return g;
        }

        public virtual void BuildScript()
        {
            SQLScript = Properties.Resources.Dynamic_Save;
            var g = BuildScriptCore();

            Interpolate(ref SQLScript, "TableName", TableName);
            Interpolate(ref SQLScript, "SchemaTable", g.SchemaString.Join());
            Interpolate(ref SQLScript, "SchemaUpdate", g.ColString.Select(x => x.Update).Join());
            Interpolate(ref SQLScript, "SchemaInsert", g.ColString.Select(x => x.Insert).Join());
            Interpolate(ref SQLScript, "ColumnInsert", g.ColString.Select(x => x.OutInsert).Join());
            Interpolate(ref SQLScript, "SelectInsert", g.ColString.Select(x => x.TargetInsert).Join());

            //Scrpt = s.ToString();

            //Select
            var c = this.SchemaTable.Rows.Cast<DataRow>().OrderBy(x => x["SeqNo"].ToInt32());

            if (UseViewForSelect)
                ScrptSelect = $"Select { c.Select(x => $"[{ x["ColumnName"].ToString() }]").Join() } From v{ this.TableName.Substring(1) }";
            else
                ScrptSelect = $"Select { c.Where(x => x["ID_ColumnSource"].ToInt32() == 1).Select(x => $"[{ x["ColumnName"].ToString() }]").Join() } From { this.TableName }";

        }

        private void AfterNew()
        {
            string s = null;
            string def = "";
            foreach (DataColumn dc in this.Columns)
            {
                var dra = mSchemaTable.Rows.Cast<DataRow>().Where(x => x["ColumnName"].ToString() == dc.ColumnName);
                if (dra.Any())
                {
                    var rda = dra.Single();
                    s = rda["Caption"].ToString();
                    if (string.IsNullOrEmpty(s)) dc.Caption = dc.ColumnName;
                    else dc.Caption = s;
                    s = rda["Expression"].ToString();
                    dc.ExtendedProperties.Add("Expression", s);
                    def = rda["DefaultValue"].ToString();

                    if (rda["ID_ColumnSource"].ToInt32() == 2)
                        dc.AllowDBNull = true;
                }

                switch (dc.DataType.ToString().ToUpper())
                {
                    case "SYSTEM.DATETIME":
                        if (def.ToUpper() == "(getdate())".ToUpper()) dc.DefaultValue = DateTime.Now.Date;
                        break;
                    case "SYSTEM.INT32":
                    case "SYSTEM.DECIMAL":
                        //ROBBIE NOTE: StartsWith is casesensitive 
                        if ((dc.ColumnName == "ID" | dc.ColumnName.StartsWith("ID_")))
                        {
                            if (!string.IsNullOrEmpty(def))
                            {
                                dc.DefaultValue = Convert.ToInt32(def.Trim(new char[] { '(', ')' }));
                            }
                        }
                        else
                        {
                            if (string.IsNullOrEmpty(def))
                            {
                                dc.DefaultValue = DBNull.Value; //0
                            }
                            else
                            {
                                dc.DefaultValue = Convert.ToDecimal(def.Trim(new char[] { '(', ')' }));
                            }
                        }
                        break;
                    case "SYSTEM.BOOLEAN":
                        if (string.IsNullOrEmpty(def))
                        {
                            dc.DefaultValue = 0;
                        }
                        else
                        {
                            dc.DefaultValue = def.Trim(new char[] { '(', ')' }).ToBool();
                        }
                        break;
                }
            }
        }

        public virtual ZTable Update()
        {
            try
            {
                if (SchemaTable.ObjectType == SchemaTable.ObjectTypeEnum.Table)
                {
                    if (!HasColumnIndex)
                    {
                        Interpolate(ref SQLScript, "HasIndex", "0");
                        Interpolate(ref SQLScript, "WhereIndex", "1 = 1");
                    }

                    var str = new List<string>();
                    var col = this.SchemaTable.Rows.Cast<DataRow>().Where(x => x["ID_ColumnSource"].ToInt32() == 1).OrderBy(x => x["SeqNo"].ToInt32());
                    var colid = col.Where(x => x["Identity"].ToInt32() == 1).Select(x => x["ColumnName"].ToString()).Single();
                    foreach (DataRow dr in this.Rows)
                    {
                        var df = new List<string>();
                        int id = dr[colid].ToInt32();
                        df.Add((id < 0 ? 0 : id).SQLFormat());
                        foreach (DataRow dc in col.Where(x => x["Identity"].ToInt32() == 0 && !x["Computed"].ToBool() && x["DataType"].ToString().ToLower() != "uniqueidentifier"))
                        {
                            df.Add(dr[dc["ColumnName"].ToString()].SQLFormat());
                        }
                        str.Add($"({ df.Join() })");
                    }

                    if (str.Count <= 0)
                        Interpolate(ref SQLScript, "InsertDataTable", "-- No Insert Here");
                    else
                    {
                        Interpolate(ref SQLScript, "InsertDataTable", "INSERT INTO #STable values {{DataTable}} ");
                        Interpolate(ref SQLScript, "DataTable", str.Join());
                    }

                    if (DeletedID?.Count > 0)
                        Interpolate(ref SQLScript, "DeletedTable", DeletedID.Select(x => $"({x})").Join());
                    else
                        Interpolate(ref SQLScript, "DeletedTable", "(NULL)");

                    SelectCommand.CommandText = SQLScript;
                    using (var dt = new DataTable())
                    {
                        if (this.Rows.Count > 0)
                        {
                            Adapter.Fill(dt);
                            this.Rows.Clear();
                            if (dt.Rows.Count > 0)
                            {
                                SelectCommand.CommandText = $"{ ScrptSelect } Where ID in ({ dt.Rows.Cast<DataRow>().Select(x => x["ID"].ToInt32()).Join() })";
                                Adapter.Fill(this);
                            }
                        }
                        else
                            SelectCommand.ExecuteNonQuery();
                    }
                }
                return this;
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message, new Exception(SQLScript));
            }
        }

        public new DataRow NewRow()
        {
            var dr = base.NewRow();
            dr["ID"] = IndexID;
            --IndexID;
            return dr;
        }

        public DataRow AddRow()
        {
            var r = this.NewRow();
            this.Rows.Add(r);
            return r;
        }

        public void AddRow(DataRow row) => this.Rows.Add(row);

        public string UpdateCommandText
        {
            get
            {
                return SelectCommand.CommandText;
            }
        }

        public void ColumnIndexes(params string[] Columns)
        {
            HasColumnIndex = true;
            var ColumnIndex = Columns.Select(x => $"T.[{ x }] = S.[{ x }]").Join(" AND ");

            Interpolate(ref SQLScript, "HasIndex", "1");
            Interpolate(ref SQLScript, "WhereIndex", ColumnIndex);
        }

        protected override void Dispose(bool disposing)
        {
            SelectCommand?.Dispose();
            Adapter?.Dispose();
            GC.Collect();
            base.Dispose(disposing);
        }
    }
}
