using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace z.SQL
{
  public class QueryBulkCopy : IDisposable
    {

      /// <summary>
      /// must have dbo.[tablename]
      /// </summary>
      private string tablename;
      private string qargs;

      public QueryBulkCopy(string table, string connectionstring)
      {
          this.tablename = table;
          this.qargs = connectionstring;
      }

      public void ProcessData(DataTable dt)
      {
          try
          {
              using (SqlConnection sqlcon = new SqlConnection(this.qargs))
              {
                  using (SqlBulkCopy copy = new SqlBulkCopy(sqlcon))
                  {
                      copy.BatchSize = 500;
                      copy.BulkCopyTimeout = 30;
                      //copy.EnableStreaming = true;
                      copy.DestinationTableName = this.tablename;
                      copy.ColumnMappings.Clear();

                      foreach (DataColumn dc in dt.Columns)
                      {
                          copy.ColumnMappings.Add(dc.ColumnName, dc.ColumnName);
                      }

                      sqlcon.Open();
                      copy.WriteToServer(dt);
                      copy.Close();   
                  }

                  sqlcon.Close();
              }
          }
          catch (Exception ex)
          {
              throw ex;
          }
      }

      public void Dispose()
      {
          GC.Collect();
          GC.SuppressFinalize(this);
      }
    }
}
