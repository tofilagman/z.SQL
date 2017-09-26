using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlServerCe;

namespace z.SQL
{
   public class QueryCE : IDisposable
    {

       private string mDB;
       private string mPassword;

       public SqlCeConnection conn { get; set; }

       public QueryCE(string DB, string Password = null)
       {
           this.mDB = DB;
           this.mPassword = Password;
       }

       public SqlCeConnection Connect()
       {
           try
           {
               conn = new SqlCeConnection(this.GetConnectionStr());
               conn.Open();

               return conn;
           }
           catch (Exception ex)
           {
               throw ex;
           }
           finally
           {
               conn.Close();
           }
       }

       public string GetConnectionStr()
       {
           return string.Format("Data Source = {0}; Persist Security Info=False; Password={1}", this.mDB, (this.mPassword == null) ? "" : this.mPassword);
       }

       public void ExecuteNonQuery(string qry)
       {
           SqlCeCommand cmd = new SqlCeCommand();

           try
           {
               if (this.conn == null) this.Connect();

               while (this.conn.State != System.Data.ConnectionState.Open)
               {
                   this.conn.Open();
               }

               cmd.Connection = this.conn;
               cmd.CommandText = qry;
               cmd.CommandTimeout = 0;
               cmd.CommandType = System.Data.CommandType.Text;
               cmd.ExecuteNonQuery();
           }
           catch (Exception ex)
           {
               throw ex;
           }
           finally
           {
               cmd.Dispose();
               cmd = null;
               this.conn.Close();
           }
       }

       public object ExecuteScalar(string Query)
       {
           SqlCeCommand cmd = new SqlCeCommand();

           try
           {
               if (this.conn == null) this.Connect();
               while (this.conn.State != System.Data.ConnectionState.Open)
               {
                   this.conn.Open();
               }

               cmd.Connection = this.conn;
               cmd.CommandText = Query;
               cmd.CommandTimeout = 0;
               cmd.CommandType = System.Data.CommandType.Text;

               return cmd.ExecuteScalar();
           }
           catch (Exception ex)
           {
               throw ex;
           }
           finally
           {
               cmd.Dispose();
               cmd = null;
               conn.Close();
           }
       }

       public System.Data.DataSet ExecuteQuery(string Query)
       {
           SqlCeCommand cmd = new SqlCeCommand();
           SqlCeDataAdapter adp = new SqlCeDataAdapter();
           System.Data.DataSet ds = new System.Data.DataSet();

           try
           {
               if (this.conn == null) this.Connect();
               while (this.conn.State != System.Data.ConnectionState.Open)
               {
                   this.conn.Open();
               }

               cmd.Connection = this.conn;
               cmd.CommandText = Query;
               cmd.CommandTimeout = 0;
               cmd.CommandType = System.Data.CommandType.Text;

               adp.SelectCommand = cmd;
               adp.Fill(ds);

               return ds;
           }
           catch (Exception ex)
           {
               throw ex;
           }
           finally
           {
               cmd.Dispose();
               adp.Dispose();
               cmd = null;
               adp = null;
               this.conn.Close();
           }
       }

       public System.Data.DataSet ExecuteQuery(string Query, string[] Parameter, object[] Value)
       {
           SqlCeCommand cmd = new SqlCeCommand();
           SqlCeDataAdapter adp = new SqlCeDataAdapter();
           System.Data.DataSet ds = new System.Data.DataSet();

           try
           {
               if (this.conn == null) this.Connect();
               while (this.conn.State != System.Data.ConnectionState.Open)
               {
                   this.conn.Open();
               }

               cmd.Connection = this.conn;
               cmd.CommandText = Query;
               cmd.CommandTimeout = 0;
               cmd.CommandType = System.Data.CommandType.Text;
               this.Parameterize(cmd, Parameter, Value);
               adp.SelectCommand = cmd;
               adp.Fill(ds);

               return ds;
           }
           catch (Exception ex)
           {
               throw ex;
           }
           finally
           {
               cmd.Dispose();
               adp.Dispose();
               cmd = null;
               adp = null;
               this.conn.Close();
           }
       }

       private void Parameterize(SqlCeCommand mCmd, string[] Parameter, object[] Value)
       {
           try
           {
               if (Parameter != null)
               {
                   for (int i = 0; i < Parameter.Length; i++)
                   {
                       if (Value.GetType().Name.ToUpper() == "DOUBLE")
                       {
                           mCmd.Parameters.Add(Parameter[i], System.Data.SqlDbType.Float);
                           mCmd.Parameters[Parameter[i]].Value = Value[i];
                       }
                       else
                       {
                           mCmd.Parameters.AddWithValue(Parameter[i], Value[i]);
                       }
                   }
               }
           }
           catch (Exception ex)
           {
               throw ex;
           }
       }

       void IDisposable.Dispose()
       {
           if (this.conn != null) this.conn.Dispose();
           GC.Collect();
           GC.SuppressFinalize(this);
       }
    }
}
