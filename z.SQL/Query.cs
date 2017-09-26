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

 
        public QueryArgs mArgs { get; set; }

        public Query(IQueryArgs QArgs)
        {
            this.mArgs = QArgs as QueryArgs;
        }

        public Query(SqlConnection Con) : this(Con.ConnectionString) { }

        public Query(string ConnectionString) : this(new QueryArgs(ConnectionString)) { }

        [MTAThread]
        public SqlConnection OpenConnection()
        {
            try
            {
                using (var mConn = new SqlConnection(this.mArgs.GetConnectionString()))
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

        public IQueryArgs ConnectionParameter
        {
            get { return this.mArgs; }
        }

        protected T On<T>(Func<SqlCommand, T> action)
        {
            try
            {
                using (var mConn = new SqlConnection(this.mArgs.GetConnectionString()))
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
                using (var mConn = new SqlConnection(this.mArgs.GetConnectionString()))
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

        public class QueryArgs : IDisposable, IQueryArgs
        {
            public String Server { get; set; }
            public String UserName { get; set; }
            public String Password { get; set; }
            public String Database { get; set; }
            public int Port { get; set; }
            public string AttachDBFileName { get; set; }
            public bool IntegratedSecurity { get; set; } = false;

            public QueryArgs()
            {
                this.Port = 1433;
            }

            public QueryArgs(string ConnectionString)
            {
                this.Port = 1433;
                this.GetConnection(ConnectionString);
            }

            public QueryArgs(string Server, string User, string pass, string Dbase, int Port = 1433)
            {
                this.Server = Server;
                this.UserName = User;
                this.Password = pass;
                this.Database = Dbase;
                this.Port = Port;
            }

            public QueryArgs(string Server, string User, string pass, int Port = 1433)
            {
                this.Server = Server;
                this.UserName = User;
                this.Password = pass;
                this.Port = Port;
            }

            public string GetConnectionString()
            {
                StringBuilder sb;

                try
                {
                    sb = new StringBuilder();
                    if (!IntegratedSecurity)
                    {
                        sb.AppendFormat("Password={0};", this.Password);
                        sb.AppendFormat("Persist Security Info=True;");
                        sb.AppendFormat("User ID={0};", this.UserName);
                    }
                    else
                        sb.AppendFormat("Integrated Security=True;");

                    if (this.Database != null)
                        sb.AppendFormat("Initial Catalog={0};", this.Database);

                    if (this.Port == 1433)
                    {
                        sb.AppendFormat("Server={0}", this.Server);
                    }
                    else
                    {
                        sb.AppendFormat("Server={0},{1}", this.Server, this.Port);
                    }

                    if (AttachDBFileName != null)
                        sb.AppendFormat("AttachDbFilename={0};", this.AttachDBFileName);

                    return sb.ToString();
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    sb = null;
                }
            }

            public string GetConnectionStringWOCatalog()
            {
                StringBuilder sb;

                try
                {
                    sb = new StringBuilder();
                    sb.AppendFormat("Password={0};", this.Password);
                    sb.AppendFormat("Persist Security Info=True;");
                    sb.AppendFormat("User ID={0};", this.UserName);

                    if (this.Port == 1433)
                    {
                        sb.AppendFormat("Server={0}", this.Server);
                    }
                    else
                    {
                        sb.AppendFormat("Server={0},{1}", this.Server, this.Port);
                    }
                    return sb.ToString();
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    sb = null;
                }
            }

            void GetConnection(string strConnection)
            {
                string[] s = strConnection.Split(new string[] { ";" }, StringSplitOptions.RemoveEmptyEntries);
                foreach (string ss in s)
                {
                    string[] h = ss.Split(new string[] { "=" }, StringSplitOptions.RemoveEmptyEntries);
                    switch (h[0].Trim())
                    {
                        case "Password": this.Password = h[1].Trim(); break;
                        case "User ID": this.UserName = h[1].Trim(); break;
                        case "Initial Catalog": this.Database = h[1].Trim(); break;
                        case "Data Source":
                        case "Server":
                            string[] sf = h[1].Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
                            this.Server = sf[0].Trim();
                            if (sf.Length == 2)
                            {
                                this.Port = Convert.ToInt32(sf[1].Trim());
                            }
                            else
                            {
                                this.Port = 1433;
                            }

                            break;
                        case "AttachDbFilename": this.AttachDBFileName = h[1].Trim(); break;
                        case "Integrated Security": this.IntegratedSecurity = true; break;
                    }
                }
            }

            void IDisposable.Dispose()
            {
                //this.Server = null;
                //this.Password = null;
                //this.UserName = null;
                //this.Database = null;
                GC.SuppressFinalize(this);
            }
        }
    }
}
