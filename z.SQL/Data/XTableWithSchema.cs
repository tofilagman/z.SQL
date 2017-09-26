using System;
using System.Data;
using System.Data.SqlClient;
using z.Data;

namespace z.SQL.Data
{
    public class XTableWithSchema : DataTable, IDisposable
    {

        protected SchemaTable mSchemaTable;

        public XTableWithSchema() : base()
        {
        }

        public DataRow SchemaRow(string pColumnName)
        {
            return mSchemaTable.SchemaRow(pColumnName);
        }

        public XTableWithSchema(SqlConnection conn, string tableName) : base(tableName)
        {
            mSchemaTable = new SchemaTable(conn, tableName);
        }
        public SchemaTable SchemaTable
        {
            get { return mSchemaTable; }
        }

        protected override void Dispose(bool disposing)
        {
            mSchemaTable?.Dispose();
            base.Dispose(disposing);
        }
    }
}
