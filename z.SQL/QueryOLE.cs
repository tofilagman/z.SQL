using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.OleDb;
using System.Data;

namespace z.SQL
{
    /// <summary>
    /// Requires MS OFFICE to be installed
    /// Supports, Old and New
    /// LJ, 20140401
    /// </summary>
   public class QueryOLE : IDisposable
    {

       private OleDbConnection mConn;
       private OleDbTransaction mTran;
       private OleDbCommand mCmd;
       private OleDbDataAdapter mAdp;

       public QueryOLE(string db)
       {
            try
            {
                mConn = new OleDbConnection(getSource(db, "",""));
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

       public QueryOLE(string db, string password)
       {
           try
           {
               mConn = new OleDbConnection(getSource(db, "", password));
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

       public QueryOLE(string db, string user, string password)
       {
           try
           {
               mConn = new OleDbConnection(getSource(db, user, password));
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

       private string getSource(string db, string User = "", string Password= "")
       {
           string str = "";
           switch (System.IO.Path.GetExtension(db).ToLower())
           {
               case ".accdb":
                   if (Password != "")
                   {
                       str = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0};Jet OLEDB:Database Password={1};", db, Password);
                   }
                   else
                   {
                       str = string.Format("Provider=Microsoft.ACE.OLEDB.12.0;Data Source={0};", db);
                   }
                   break;
               case ".mdb":
                   if (User != "" && Password != "")
                   {
                        str = string.Format("Provider=Microsoft.Jet.OLEDB.4.0;Data Source={0};User Id={1};Password={2};", db, User, Password);
                   }
                   else if(User == "" && Password != "")
                   {
                       str = string.Format("Provider=Microsoft.Jet.OLEDB.4.0;Data Source={0};Jet OLEDB:Database Password={1};", db, Password);
                   }
                   else
                   {
                       str = string.Format("Provider=Microsoft.Jet.OLEDB.4.0;Data Source={0};", db);
                   }
                   break;
           }

           return str;
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
       public DataSet ExecQuery(string Command, string[] Parameter = null, object[] value = null)
       {
           try
           {
               DataSet ds;
               this.OpenConnection();
               lock (mConn)
               {
                   using (ds = new DataSet())
                   {
                       using (this.mCmd = new OleDbCommand())
                       {
                           this.mTran = this.mConn.BeginTransaction();
                           this.mCmd.Connection = this.mConn;
                           this.mCmd.Transaction = this.mTran;
                           this.mCmd.CommandText = Command;
                           this.mCmd.CommandTimeout = 3000;
                           this.mCmd.CommandType = CommandType.Text;
                           if (Parameter != null)
                           {
                               this.Parameterize(this.mCmd, Parameter, value);
                           }
                           using (this.mAdp = new OleDbDataAdapter())
                           {
                               this.mAdp.SelectCommand = this.mCmd;
                               this.mAdp.Fill(ds);
                           }

                           this.mTran.Commit();
                       }
                   }
               }
               return ds;
           }
           catch (OleDbException ex)
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
       public void ExecNonQuery(string Command, string[] Parameters = null, object[] values = null)
       {
           try
           {
               this.OpenConnection();
               using (this.mCmd = new OleDbCommand())
               {
                   this.mTran = this.mConn.BeginTransaction();
                   this.mCmd.Connection = this.mConn;
                   this.mCmd.Transaction = this.mTran;
                   this.mCmd.CommandText = Command;
                   this.mCmd.CommandTimeout = 3000;
                   this.mCmd.CommandType = CommandType.Text;
                   if (Parameters != null)
                   {
                       this.Parameterize(this.mCmd, Parameters, values);
                   }
                   this.mCmd.ExecuteNonQuery();
                   this.mTran.Commit();
               }
           }
           catch (OleDbException ex)
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
               using (this.mCmd = new OleDbCommand())
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
           catch (OleDbException ex)
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

       void Parameterize(OleDbCommand cmd, string[] keys, object[] values)
       {
           for (int i = 0; i < keys.Length; i++)
           {
               cmd.Parameters.AddWithValue(keys[i], values[i]);
           }
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
