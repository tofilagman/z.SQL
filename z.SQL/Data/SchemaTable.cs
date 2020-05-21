using Microsoft.VisualBasic;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using z.Data;

namespace z.SQL.Data
{
    public class SchemaTable : DataTable
    {
        public const string DefaultDateFormat = "MM-dd-yyyy";
        public const string DefaultDateTimeFormat = "hh:mm tt MM.dd.yy";
        public const string DefaultTimeFormat = "hh:mm tt";

        public ObjectTypeEnum ObjectType { get; private set; }

        public SchemaTable(string vTableName) : base(vTableName) { }

        public SchemaTable(SqlConnection vConnection, string vTableName) : base(vTableName)
        {
            string s = null;
            string vViewName = null;
            vViewName = "v" + vTableName.Substring(1); //Strings.Right(vTableName, vTableName.Length - 1);
            s = "SELECT * FROM dbo.fViewSchema('" + vTableName + "','" + vViewName + "')";
            this.FillTable(s, vConnection); //  FillTable(this, s, vConnection);
            this.DefaultView.Sort = "SeqNo";
            this.PrimaryKey = new DataColumn[] { this.Columns["ColumnName"] };
            //Return dt
            this.GetObjectType(vConnection);
            this.SetColumnStringFormat();
        }

        public SchemaTable(DataTable dt, string vTableName) : base(vTableName)
        {

            foreach (DataColumn dc in dt.Columns)
            {
                this.Columns.Add(dc.ColumnName, dc.DataType);
            }

            foreach (DataRow dr in dt.Rows)
            {
                DataRow idr = this.NewRow();
                foreach (DataColumn dc in dt.Columns)
                {
                    idr[dc.ColumnName] = dr[dc.ColumnName];
                }
                this.Rows.Add(idr);
            }

            this.DefaultView.Sort = "SeqNo";

            this.PrimaryKey = new DataColumn[] { this.Columns["ColumnName"] };
        }

        private void SetColumnStringFormat()
        {
            string s = "";
            //("ColumnName=" & EXIA.SQL.SQLFormat(c.ColumnName))
            foreach (DataRow dr in this.Select())
            {
                s = dr["ColumnName"].ToString();
                //If Strings.Left(s, 3) <> "ID_" Then
                switch (dr["DataType"].ToString().ToLower())
                {
                    case "decimal":
                        byte vScale = 0;
                        //20090401
                        //If dra.Length > 0 Then
                        vScale = Convert.ToByte(dr["Scale"]);
                        //End If
                        if (Convert.ToBoolean(vScale <= 4))
                        {
                            s = "#,##0" + (vScale > 0 ? "." + new string('0', vScale) : "").ToString(); //Strings.StrDup(vScale, "0")
                        }
                        else
                        {
                            s = "#,##0.00";
                        }
                        break;
                    case "date":
                    case "datetime":
                        bool d = false;
                        bool tt = false;
                        d = dr["ColumnName"].ToString().Contains("Date"); //Strings.InStr(dr["ColumnName"].ToString(), "Date", CompareMethod.Binary) > 0;
                        tt = dr["ColumnName"].ToString().Contains("Time"); //Strings.InStr(dr["ColumnName"].ToString(), "Time", CompareMethod.Binary) > 0;
                        if (d & tt)
                            s = DefaultDateTimeFormat;
                        else if (d)
                            s = DefaultDateFormat;
                        else
                            s = DefaultTimeFormat;
                        break;
                    case "bit":
                        s = "YES/NO";
                        break;
                    case "nvarchar":
                    case "varchar":
                    case "int":
                    case "uniqueidentifier":
                    case "text":
                        s = "";
                        break;
                    default:
                        s = "";
                        throw new Exception(this.TableName + "." + dr["ColumnName"].ToString() + ". Warning: use of " + dr["DataType"].ToString().ToLower());
                }
                dr["StringFormat"] = s;
            }
        }

        public DataRow SchemaRow(string pColumnName)
        {
            return this.Rows.Cast<DataRow>().SingleOrDefault(x => x["ColumnName"].ToString() == pColumnName);
        }

        public ObjectTypeEnum GetObjectType(SqlConnection vConnection)
        {
            var type = new SqlConnectionStringBuilder(vConnection.ConnectionString).ExecScalar("SELECT [type] FROM sys.objects WHERE name = @ZTable", this.TableName).ToString().Trim();
            switch (type)
            {
                case "U": this.ObjectType = ObjectTypeEnum.Table; break;
                case "V": this.ObjectType = ObjectTypeEnum.View; break;
                case "FN": this.ObjectType = ObjectTypeEnum.InlineFunction; break;
                case "IF": this.ObjectType = ObjectTypeEnum.Function; break;
                case "P ": this.ObjectType = ObjectTypeEnum.StoredProcedure; break;
            }
            return this.ObjectType;
        }

        public enum ObjectTypeEnum : int
        {
            Table = 0,
            View = 1,
            Function = 2,
            InlineFunction = 3,
            StoredProcedure = 4
        }
    }

}
