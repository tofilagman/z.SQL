using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Odbc;
using System.Data;

namespace z.SQL
{
   public class QueryODBC: IDisposable
    {

       public string ConnectionString { get; private set; }
       private OdbcConnection mConn;
       private OdbcTransaction mTran;
       private OdbcCommand mCmd;
       private OdbcDataAdapter mAdp;

       public QueryODBC(string dbpath)
       {
           this.ConnectionString = "Driver={Microsoft Access Driver(*.mdb, *.accdb)};DBQ=" + dbpath + ";";
           try
           {
               mConn = new OdbcConnection(this.ConnectionString);
               mConn.Open();
           }
           catch (Exception ex)
           {
               throw ex;
           }
           finally
           {
               mConn.Close();
           }
       }


       [MTAThread]
       private void OpenConnection()
       {
           try
           {
               while (this.mConn.State != System.Data.ConnectionState.Open)
               {
                   this.mConn.Open();
               }
           }
           catch (Exception ex)
           {
               throw ex;
           }
       }

       [MTAThread]
       public DataSet ExecQuery(string Command)
       {
           try
           {
               DataSet ds;
               this.OpenConnection();
               using (ds = new DataSet())
               {
                   using (this.mCmd = new OdbcCommand())
                   {
                       this.mTran = this.mConn.BeginTransaction();
                       this.mCmd.Connection = this.mConn;
                       this.mCmd.Transaction = this.mTran;
                       this.mCmd.CommandText = Command;
                       this.mCmd.CommandTimeout = 3000;
                       this.mCmd.CommandType = CommandType.Text;


                       using (this.mAdp = new OdbcDataAdapter())
                       {
                           this.mAdp.SelectCommand = this.mCmd;
                           this.mAdp.Fill(ds);
                       }

                       this.mTran.Commit();
                   }
               }
               return ds;
           }
           catch (OdbcException ex)
           {
               this.mTran.Rollback();
               throw ex;
           }
           catch (Exception ex)
           {
               this.mTran.Rollback();
               throw ex;
           }
           finally
           {
               this.mConn.Close();
               this.mTran.Dispose();
               this.mCmd.Dispose();
           }
       }

       [MTAThread]
       public void ExecNonQuery(string Command)
       {
           try
           {
               this.OpenConnection();
               using (this.mCmd = new OdbcCommand())
               {
                   this.mTran = this.mConn.BeginTransaction();
                   this.mCmd.Connection = this.mConn;
                   this.mCmd.Transaction = this.mTran;
                   this.mCmd.CommandText = Command;
                   this.mCmd.CommandTimeout = 3000;
                   this.mCmd.CommandType = CommandType.Text;
                   this.mCmd.ExecuteNonQuery();
                   this.mTran.Commit();
               }
           }
           catch (OdbcException ex)
           {
               this.mTran.Rollback();
               throw ex;
           }
           catch (Exception ex)
           {
               this.mTran.Rollback();
               throw ex;
           }
           finally
           {
               this.mConn.Close();
               this.mTran.Dispose();
               this.mCmd.Dispose();
           }
       }

       [MTAThread]
       public object ExecScalar(string Command)
       {
           try
           {
               object RetVal = DBNull.Value;
               this.OpenConnection();
               using (this.mCmd = new OdbcCommand())
               {
                   this.mTran = this.mConn.BeginTransaction();
                   this.mCmd.Connection = this.mConn;
                   this.mCmd.Transaction = this.mTran;
                   this.mCmd.CommandText = Command;
                   this.mCmd.CommandTimeout = 3000;
                   this.mCmd.CommandType = CommandType.Text;
                   RetVal = this.mCmd.ExecuteScalar();
                   this.mTran.Commit();
               }
               return RetVal;
           }
           catch (OdbcException ex)
           {
               this.mTran.Rollback();
               throw ex;
           }
           catch (Exception ex)
           {
               this.mTran.Rollback();
               throw ex;
           }
           finally
           {
               this.mConn.Close();
               this.mTran.Dispose();
               this.mCmd.Dispose();
           }
       }

       ~QueryODBC()
       {
           Dispose();
       }

       public void Dispose()
       {
           try
           {
               this.mConn.Dispose();
               if (this.mTran != null) { this.mTran.Dispose(); }
               if (this.mCmd != null) { this.mCmd.Dispose(); }
               if (this.mAdp != null) { this.mAdp.Dispose(); }
               this.mConn = null;
               this.mTran = null;
               this.mCmd = null;
               this.mAdp = null;
           }
           catch (Exception ex)
           {
               throw ex;
           }
       }

    }
}
