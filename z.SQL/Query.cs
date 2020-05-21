using System;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using z.Data;
using System.IO;
using static z.SQL.Extensions;

namespace z.SQL
{
    public class Query : IDisposable, IQuery
    {


        public readonly SqlConnectionStringBuilder mArgs;

        public Query(SqlConnectionStringBuilder QArgs)
        {
            this.mArgs = QArgs;
        }
         
        public Query(string ConnectionString) : this(new SqlConnectionStringBuilder(ConnectionString)) { }

        [MTAThread]
        public SqlConnection OpenConnection()
        {
            try
            {
                using (var mConn = new SqlConnection(this.mArgs.ConnectionString))
                {
                    while (mConn.State != ConnectionState.Open)
                        mConn.Open();

                    return mConn;
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        [MTAThread]
        public DataSet ExecQuery(string Command) => On<DataSet>(mCmd =>
        {
            using (var ds = new DataSet())
            {
                mCmd.CommandText = Command;
                mCmd.CommandTimeout = 3000;
                mCmd.CommandType = CommandType.Text;
                using (var mAdp = new SqlDataAdapter())
                {
                    mAdp.SelectCommand = mCmd;
                    mAdp.Fill(ds);
                    return ds;
                }
            }
        });

        [MTAThread]
        public DataSet ExecQuery(string Command, string[] Parameter, object[] Value) => On<DataSet>(mCmd =>
        {
            using (DataSet ds = new DataSet())
            {
                mCmd.CommandText = Command;
                mCmd.CommandTimeout = 3000;
                //this.mCmd.CommandType = CommandType.StoredProcedure;
                mCmd.Parameterize(Parameter, Value);
                using (var mAdp = new SqlDataAdapter())
                {
                    mAdp.SelectCommand = mCmd;
                    mAdp.Fill(ds);
                    return ds;
                }
            }
        });

        [MTAThread]
        public DataSet ExecQuery(string Command, params object[] Value)
        {
            string[] arr = Command.ParseParameter().ToArray();
            if (arr.Length != Value.Length) throw new Exception("Specified Paramater and Value count is Incorrect");
            return ExecQuery(Command, arr, Value);
        }

        [MTAThread]
        public DataTable TableQuery(string Command) => On<DataTable>(mCmd =>
        {
            using (var ds = new DataTable())
            {
                mCmd.CommandText = Command;
                mCmd.CommandTimeout = 3000;
                mCmd.CommandType = CommandType.Text;
                using (var mAdp = new SqlDataAdapter())
                {
                    mAdp.SelectCommand = mCmd;
                    mAdp.Fill(ds);
                    return ds;
                }
            }
        });

        [MTAThread]
        public DataTable TableQuery(string Command, string[] Parameter, object[] Value) => On<DataTable>(mCmd =>
        {
            using (DataTable ds = new DataTable())
            {
                mCmd.CommandText = Command;
                mCmd.CommandTimeout = 3000;
                mCmd.Parameterize(Parameter, Value);
                using (var mAdp = new SqlDataAdapter())
                {
                    mAdp.SelectCommand = mCmd;
                    mAdp.Fill(ds);
                    return ds;
                }
            }
        });

        [MTAThread]
        public DataTable TableQuery(string Command, params object[] Value)
        {
            string[] arr = Command.ParseParameter().ToArray();
            if (arr.Length != Value.Length) throw new Exception("Specified Paramater and Value count is Incorrect");
            return TableQuery(Command, arr, Value);
        }

        [MTAThread]
        public void ExecNonQuery(string Command) => On(mCmd =>
        {
            mCmd.CommandText = Command;
            mCmd.CommandTimeout = 3000;
            mCmd.CommandType = CommandType.Text;
            mCmd.ExecuteNonQuery();
        });
          
        [MTAThread]
        public void ExecNonQuery(string Command, string[] Parameter, object[] Value, CommandType type = CommandType.Text) => On(mCmd =>
        {
            mCmd.CommandText = Command;
            mCmd.CommandTimeout = 3000;
            mCmd.CommandType = type;
            mCmd.Parameterize(Parameter, Value);
            mCmd.ExecuteNonQuery();
        });

        /// <summary>
        /// Execute SQL Procedures Only
        /// </summary>
        /// <param name="Command">pTest @param1, @param2</param>
        /// <param name="args"></param>
        [MTAThread]
        public void ExecNonQuery(string Command, params object[] args)
        {
            string[] arr = Command.ParseParameter().ToArray();
            if (arr.Length != args.Length) throw new Exception("Specified Paramater and Value count is Incorrect");
            this.ExecNonQuery(Command, arr, args);
        }

        [MTAThread]
        public object ExecScalar(string Command) => On<object>(mCmd =>
        {
            mCmd.CommandText = Command;
            mCmd.CommandTimeout = 3000;
            mCmd.CommandType = CommandType.Text;
            return mCmd.ExecuteScalar();
        });

        [MTAThread]
        public T ExecScalar<T>(string Command) where T : class => Convert.ChangeType(ExecScalar(Command), typeof(T)) as T;

        [MTAThread]
        public object ExecScalar(string Command, string[] Parameter, object[] Value, CommandType type = CommandType.Text) => On<object>(mCmd =>
        {
            mCmd.CommandText = Command;
            mCmd.CommandTimeout = 3000;
            mCmd.CommandType = type;
            mCmd.Parameterize(Parameter, Value);
            return mCmd.ExecuteScalar();
        });

        [MTAThread]
        public T ExecScalar<T>(string Command, string[] Parameter, object[] Value, CommandType type = CommandType.Text) where T : class => Convert.ChangeType(ExecScalar(Command, Parameter, Value, type), typeof(T)) as T;

        /// <summary>
        /// Executes SP that returns Scalar Value
        /// </summary>
        /// <param name="Command">ptest @arg1, @arg2</param>
        /// <param name="Value"></param>
        /// <returns></returns>
        [MTAThread]
        public object ExecScalar(string Command, params object[] Value)
        {
            string[] arr = Command.ParseParameter().ToArray();
            if (arr.Length != Value.Length) throw new Exception("Specified Paramater and Value count is Incorrect");
            return ExecScalar(Command, arr, Value);
        }

        [MTAThread]
        public T ExecScalar<T>(string Command, params object[] Value) where T : class => Convert.ChangeType(ExecScalar(Command, Value), typeof(T)) as T;

        public void ScriptExec(string Script, Pair Parameters)
        {
            Parameters.Each(x => Interpolate(ref Script, x.Key, x.Value.ToString()));
            ExecNonQuery(Script);
        }

        public SqlConnectionStringBuilder ConnectionParameter
        {
            get { return this.mArgs; }
        }

        protected T On<T>(Func<SqlCommand, T> action)
        {
            try
            {
                using (var mConn = new SqlConnection(this.mArgs.ConnectionString))
                {
                    while (mConn.State != ConnectionState.Open)
                        mConn.Open();

                    using (var mCmd = new SqlCommand())
                    {
                        //this.OpenConnection();
                        //if (UseTran) this.mCmd.Transaction = this.mTran;
                        mCmd.Connection = mConn;
                        return action(mCmd);
                    }
                }
            }
            catch (SqlException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                //    this.mCmd?.Dispose();
                //    this.mConn?.Close();
            }
        }

        protected void On(Action<SqlCommand> action)
        {
            try
            {
                using (var mConn = new SqlConnection(this.mArgs.ConnectionString))
                {
                    while (mConn.State != ConnectionState.Open)
                        mConn.Open();

                    using (var mCmd = new SqlCommand())
                    {
                        mCmd.Connection = mConn;
                        action(mCmd);
                    }
                }
            }
            catch (SqlException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {

            }
        }

        public void Dispose()
        {
            Dispose(true);
            GC.Collect();
            GC.SuppressFinalize(this);
        }

        protected virtual void Dispose(bool disposing)
        {

            GC.Collect();
            GC.SuppressFinalize(this);
        }

        ~Query()
        {
            Dispose(true);
        } 
    }
}
