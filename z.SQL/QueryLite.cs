using System;
using System.Data.SQLite;
using System.Data;
using z.Data;

namespace z.SQL
{
    public class QueryLite : IDisposable
    {
        public string ConnectionString { get; private set; }
        
        public QueryLite(string DBPath)
        {
            //"Data Source=(local);Initial Catalog=sqlite;Integrated Security=True;Max Pool Size=10"
            this.ConnectionString = String.Format("Data Source={0};Pooling=true;FailIfMissing=false", DBPath); 
        }

        [MTAThread]
        public SQLiteConnection OpenConnection()
        {
            try
            {
                using (var mConn = new SQLiteConnection(this.ConnectionString))
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
                using (var mAdp = new SQLiteDataAdapter())
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
                using (var mAdp = new SQLiteDataAdapter())
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
                using (var mAdp = new SQLiteDataAdapter())
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
                using (var mAdp = new SQLiteDataAdapter())
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

        protected T On<T>(Func<SQLiteCommand, T> action)
        {
            try
            {
                using (var mConn = new SQLiteConnection(this.ConnectionString))
                {
                    while (mConn.State != ConnectionState.Open)
                        mConn.Open();

                    using (var mCmd = new SQLiteCommand())
                    {
                        //this.OpenConnection();
                        //if (UseTran) this.mCmd.Transaction = this.mTran;
                        mCmd.Connection = mConn;
                        return action(mCmd);
                    }
                }
            }
            catch (SQLiteException ex)
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

        protected void On(Action<SQLiteCommand> action)
        {
            try
            {
                using (var mConn = new SQLiteConnection(this.ConnectionString))
                {
                    while (mConn.State != ConnectionState.Open)
                        mConn.Open();

                    using (var mCmd = new SQLiteCommand())
                    {
                        mCmd.Connection = mConn;
                        action(mCmd);
                    }
                }
            }
            catch (SQLiteException ex)
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

        ~QueryLite()
        {
            Dispose();
        }

        public void Dispose()
        {
            GC.Collect();
            GC.SuppressFinalize(this);
        }
    }
}
