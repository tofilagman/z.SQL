using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace z.SQL
{
   public class QueryEx : IDisposable
    {

       private Query.QueryArgs qrrgs;
       private SqlConnection conn;
       private SqlTransaction tran;
       private SqlCommand command;
       
       public QueryEx(Query.QueryArgs QryArgs)
       {
           try{
                qrrgs = QryArgs;
                conn = new SqlConnection(qrrgs.GetConnectionString());
                this.command = new SqlCommand();
                this.command.Connection = conn;
                this.command.Transaction = tran;
                this.command.CommandType = CommandType.Text;
                this.command.CommandTimeout = 0;
           }catch(Exception ex){
               throw ex;
           }
           
       }

       public void Connect()
       {
          
           try
           {
               conn.Open();
           }
           catch (Exception ex)
           {
               throw ex;
           }
       }

       public void Transact()
       {
           try
           {
               this.tran = this.conn.BeginTransaction(IsolationLevel.Serializable);
           }
           catch (Exception ex)
           {
               throw ex;
           }
       }

       public void TransactCommit()
       {
           try
           {
               if (this.tran != null)
               {
                   this.tran.Commit();
               }
           }
           catch (Exception ex)
           {
               throw ex;
           }
       }

       public void TransactRollBack()
       {
           try
           {
               if (this.tran != null)
               {
                   this.tran.Rollback();
               }
           }
           catch (Exception ex)
           {
               throw ex;
           }
       }

       public void Close()
       {
           try
           {
               if (this.tran != null)
               {
                   this.conn.Close();
               }
           }
           catch (Exception ex)
           {
               throw ex;
           }
           finally
           {
               if (this.tran != null)
               {
                   this.tran.Dispose();
               }
               if (this.conn != null)
               {
                   SqlConnection.ClearPool(this.conn);
                   this.conn.Dispose();
               }

               GC.Collect();
           }
       }

       public void ExecuteScript(string scrpt)
       {
           try
           {
               this.command.Transaction = this.tran;
               this.command.CommandText = scrpt;
               this.command.ExecuteNonQuery();
           }
           catch (SqlException ex)
           {
               if (ex.Number == 1205)
               {
                   this.command.ExecuteNonQuery();
               }
               else
               {
                   throw ex;
               }
           }
           catch (IndexOutOfRangeException ex)
           {
               throw new IndexOutOfRangeException("InSys: Index Out of Range Exception", ex);
           }
           catch (StackOverflowException ex)
           {
               throw new StackOverflowException("InSys: Stack OverFlow", ex);
           }
           catch (Exception ex) //Fatal Error, Close Connection, Connecting
           {
               throw ex;
           }
       }

       public object ExecuteScalar(string scrpt)
       {
           object obj = DBNull.Value;
           try
           {
               this.command.Transaction = this.tran;
               this.command.CommandText = scrpt;
               obj = this.command.ExecuteScalar();
           }
           catch (SqlException ex)
           {
               if (ex.Number == 1205)
               {
                   this.command.ExecuteNonQuery();
               }
               else
               {
                   throw ex;
               }
           }
           catch (IndexOutOfRangeException ex)
           {
               throw new IndexOutOfRangeException("InSys: Index Out of Range Exception", ex);
           }
           catch (StackOverflowException ex)
           {
               throw new StackOverflowException("InSys: Stack OverFlow", ex);
           }
           catch (Exception ex) //Fatal Error, Close Connection, Connecting
           {
               throw ex;
           }

           return obj;
       }

       public void Dispose()
        {
            this.command.Dispose();
            GC.Collect();
            GC.SuppressFinalize(this);
        }
    }
}
