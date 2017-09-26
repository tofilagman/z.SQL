using System;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using System.Data;
using System.Threading.Tasks;
using z.Data;

namespace z.SQL
{
    /// <summary>
    /// Wrapper for MYSQL Service
    /// </summary>
    public class QueryMy : IDisposable, IQuery
    {
        public MySqlConnection mConn;
        private MySqlTransaction mTran;
        private MySqlCommand mCmd;
        private MySqlDataAdapter mAdp;

        public QueryArgs mArgs { get; set; }

        public QueryMy(QueryArgs QArgs)
        {
            try
            {
                using (QArgs)
                {
                    mConn = new MySqlConnection(QArgs.GetConnectionString());
                    mConn.Open();
                    this.mArgs = QArgs;
                }
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

        public QueryMy(string ConnectionString)
        {
            try
            {
                mConn = new MySqlConnection(ConnectionString);
                mConn.Open();
                this.mArgs = new QueryArgs(ConnectionString);
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
        public void OpenConnection()
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
        public void Parameterize(string[] Parameter, object[] Value)
        {
            try
            {
                if (Parameter != null)
                {
                    for (int i = 0; i < Parameter.Length; i++)
                    {
                        if (Value.GetType().Name.ToUpper() == "DOUBLE")
                        {
                            this.mCmd.Parameters.Add(Parameter[i], MySqlDbType.Float);
                            this.mCmd.Parameters[Parameter[i]].Value = Value[i];
                        }
                        else
                        {
                            this.mCmd.Parameters.AddWithValue(Parameter[i], Value[i]);
                        }
                    }
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
                    using (this.mCmd = new MySqlCommand())
                    {
                        this.mTran = this.mConn.BeginTransaction();
                        this.mCmd.Connection = this.mConn;
                        this.mCmd.Transaction = this.mTran;
                        this.mCmd.CommandText = Command;
                        this.mCmd.CommandTimeout = 3000;
                        this.mCmd.CommandType = CommandType.Text;


                        using (this.mAdp = new MySqlDataAdapter())
                        {
                            this.mAdp.SelectCommand = this.mCmd;
                            this.mAdp.Fill(ds);
                        }

                        this.mTran.Commit();
                    }
                }
                return ds;
            }
            catch (MySqlException ex)
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
        public void ExecQueryThenFill(string Command, DataTable dt)
        {
            try
            {
                this.OpenConnection();

                using (this.mCmd = new MySqlCommand())
                {
                    this.mTran = this.mConn.BeginTransaction();
                    this.mCmd.Connection = this.mConn;
                    this.mCmd.Transaction = this.mTran;
                    this.mCmd.CommandText = Command;
                    this.mCmd.CommandTimeout = 3000;
                    this.mCmd.CommandType = CommandType.Text;


                    using (this.mAdp = new MySqlDataAdapter())
                    {
                        this.mAdp.SelectCommand = this.mCmd;
                        this.mAdp.Fill(dt);
                    }

                    this.mTran.Commit();
                }
            }
            catch (MySqlException ex)
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
        public DataSet ExecQuery(string Command, string[] Parameter, object[] Value, CommandType type = CommandType.Text)
        {
            try
            {
                DataSet ds;
                this.OpenConnection();
                using (ds = new DataSet())
                {
                    using (this.mCmd = new MySqlCommand())
                    {
                        this.mTran = this.mConn.BeginTransaction();
                        this.mCmd.Connection = this.mConn;
                        this.mCmd.Transaction = this.mTran;
                        this.mCmd.CommandText = Command;
                        this.mCmd.CommandTimeout = 3000;
                        this.mCmd.CommandType = type;
                        this.Parameterize(Parameter, Value);
                        using (this.mAdp = new MySqlDataAdapter())
                        {
                            this.mAdp.SelectCommand = this.mCmd;
                            this.mAdp.Fill(ds);
                        }

                        this.mTran.Commit();
                    }
                }
                return ds;
            }
            catch (MySqlException ex)
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
        public DataSet ExecQuery(string Command, params object[] Value)
        {
            string[] arr = Command.ParseParameter().ToArray();
            if (arr.Length != Value.Length) throw new Exception("Specified Paramater and Value count is Incorrect");
            return ExecQuery(Command, arr, Value);
        }

        [MTAThread]
        public void ExecNonQuery(string Command)
        {
            try
            {
                this.OpenConnection();
                using (this.mCmd = new MySqlCommand())
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
            catch (MySqlException ex)
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
        public void ExecNonQuery(string Command, params object[] Value)
        {
            string[] arr = Command.ParseParameter().ToArray();
            if (arr.Length != Value.Length) throw new Exception("Specified Paramater and Value count is Incorrect");

            ExecNonQuery(Command, arr, Value);
        }

        [MTAThread]
        public void ExecNonQuery(string Command, string[] Parameter, object[] Value, CommandType type = CommandType.Text)
        {
            try
            {
                this.OpenConnection();
                using (this.mCmd = new MySqlCommand())
                {
                    this.mTran = this.mConn.BeginTransaction();
                    this.mCmd.Connection = this.mConn;
                    this.mCmd.Transaction = this.mTran;
                    this.mCmd.CommandText = Command;
                    this.mCmd.CommandTimeout = 3000;
                    this.mCmd.CommandType = type;
                    this.Parameterize(Parameter, Value);
                    this.mCmd.ExecuteNonQuery();
                    this.mTran.Commit();
                }
            }
            catch (MySqlException ex)
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
                using (this.mCmd = new MySqlCommand())
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
            catch (MySqlException ex)
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
        public T ExecScalar<T>(string Command) where T : class => Convert.ChangeType(ExecScalar(Command), typeof(T)) as T;

        [MTAThread]
        public object ExecScalar(string Command, params object[] Value)
        {
            string[] arr = Command.ParseParameter().ToArray();
            if (arr.Length != Value.Length) throw new Exception("Specified Paramater and Value count is Incorrect");
            return ExecScalar(Command, arr, Value);
        }

        [MTAThread]
        public T ExecScalar<T>(string Command, params object[] Value) where T : class => Convert.ChangeType(ExecScalar(Command, Value), typeof(T)) as T;

        [MTAThread]
        public object ExecScalar(string Command, string[] Parameter, object[] Value)
        {
            try
            {
                object RetVal = DBNull.Value;
                this.OpenConnection();
                using (this.mCmd = new MySqlCommand())
                {
                    this.mTran = this.mConn.BeginTransaction();
                    this.mCmd.Connection = this.mConn;
                    this.mCmd.Transaction = this.mTran;
                    this.mCmd.CommandText = Command;
                    this.mCmd.CommandTimeout = 3000;
                    this.mCmd.CommandType = CommandType.Text;
                    this.Parameterize(Parameter, Value);
                    RetVal = this.mCmd.ExecuteScalar();
                    this.mTran.Commit();
                }
                return RetVal;
            }
            catch (MySqlException ex)
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
        public T ExecScalar<T>(string Command, string[] Parameter, object[] Value, CommandType type = CommandType.Text) where T : class => Convert.ChangeType(ExecScalar(Command, Parameter, Value, type), typeof(T)) as T;

        public void ExecBatch(string sql, string delimiter = ";", Action OnComplete = null, Action<Exception> OnError = null)
        {
            MySqlScript scrpt = new MySqlScript();
            this.OpenConnection();
            scrpt.StatementExecuted += (o, e) => { };
            scrpt.ScriptCompleted += (o, e) => OnComplete?.Invoke();
            scrpt.Error += (o, e) => OnError?.Invoke(e.Exception);
            scrpt.Connection = this.mConn;
            scrpt.Query = sql;
            scrpt.Delimiter = delimiter;
            scrpt.Execute();
        }

        public IQueryArgs ConnectionParameter
        {
            get { return this.mArgs; }
        }

        public void Import(string filepath, MySqlBackup.importComplete onDone, EventHandler<MySqlBackup.ExceptionEventArgs> onError, MySqlBackup.importProgressChange OnProgress)
        {
            Task.Factory.StartNew(() =>
            {
                try
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = new MySqlConnection(mArgs.GetConnectionString());
                        cmd.Connection.Open();
                        using (MySqlBackup bck = new MySqlBackup(cmd))
                        {
                            bck.ImportProgressChanged += OnProgress;
                            bck.ImportCompleted += onDone;
                            bck.ImportFromFile(filepath);
                        }
                    }
                }
                catch (MySqlException ex)
                {
                    onError(this, new MySqlBackup.ExceptionEventArgs() { ex = ex });
                }
                catch (Exception ex)
                {
                    onError(this, new MySqlBackup.ExceptionEventArgs() { ex = ex });
                }
            });
        }

        public void ImportString(string querystring, MySqlBackup.importComplete onDone, EventHandler<MySqlBackup.ExceptionEventArgs> onError, MySqlBackup.importProgressChange OnProgress)
        {
            Task.Factory.StartNew(() =>
            {
                try
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = new MySqlConnection(mArgs.GetConnectionString());
                        cmd.Connection.Open();
                        using (MySqlBackup bck = new MySqlBackup(cmd))
                        {
                            bck.ImportProgressChanged += OnProgress;
                            bck.ImportCompleted += onDone;
                            bck.ImportFromString(querystring);
                        }
                    }
                }
                catch (MySqlException ex)
                {
                    onError(this, new MySqlBackup.ExceptionEventArgs() { ex = ex });
                }
                catch (Exception ex)
                {
                    onError(this, new MySqlBackup.ExceptionEventArgs() { ex = ex });
                }
            });
        }

        public void Export(string mfile, MySqlBackup.exportComplete OnDone, EventHandler<MySqlBackup.ExceptionEventArgs> onError, MySqlBackup.exportProgressChange onProgress)
        {
            Task.Factory.StartNew(() =>
            {
                try
                {
                    using (MySqlCommand cmd = new MySqlCommand())
                    {
                        cmd.Connection = new MySqlConnection(mArgs.GetConnectionString());
                        cmd.Connection.Open();
                        using (MySqlBackup bck = new MySqlBackup(cmd))
                        {
                            bck.ExportProgressChanged += onProgress;
                            bck.ExportCompleted += OnDone;
                            bck.ExportToFile(mfile);
                        }
                    }
                }
                catch (Exception ex)
                {
                    onError(this, new MySqlBackup.ExceptionEventArgs() { ex = ex });
                }
            });
        }

        ~QueryMy()
        {
            Dispose(true);
        }

        public void Dispose(bool b)
        {
            try
            {
                if (this.mConn != null) this.mConn.Dispose();
                if (this.mTran != null) { this.mTran.Dispose(); }
                if (this.mCmd != null) { this.mCmd.Dispose(); }
                if (this.mAdp != null) { this.mAdp.Dispose(); }
                this.mConn = null;
                this.mTran = null;
                this.mCmd = null;
                this.mAdp = null;
            }
            catch { }
        }

        [MTAThread]
        void IDisposable.Dispose()
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

            public QueryArgs()
            {
                this.Port = 3306;
            }

            public QueryArgs(string ConnectionString)
            {
                this.GetConnection(ConnectionString);
            }

            public QueryArgs(string Server, string User, string pass, string Dbase, int Port = 3306)
            {
                this.Server = Server;
                this.UserName = User;
                this.Password = pass;
                this.Database = Dbase;
                this.Port = Port;
            }

            public string GetConnectionString(bool IncludeDB = true)
            {
                StringBuilder sb;

                try
                {
                    sb = new StringBuilder();
                    sb.AppendFormat("Server={0};", this.Server);
                    sb.AppendFormat("Port={0};", this.Port);
                    sb.AppendFormat("Uid={0};", this.UserName);
                    sb.AppendFormat("Pwd={0};", this.Password);

                    if (this.Database != null && IncludeDB)
                    {
                        sb.AppendFormat("Database={0};", this.Database);
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

            public string GetConnectionString()
            {
                return GetConnectionString(true);
            }

            void GetConnection(string strConnection)
            {
                string[] s = strConnection.Split(new string[] { ";" }, StringSplitOptions.RemoveEmptyEntries);
                foreach (string ss in s)
                {
                    string[] h = ss.Split(new string[] { "=" }, StringSplitOptions.RemoveEmptyEntries);
                    switch (h[0].Trim())
                    {
                        case "Pwd": this.Password = h[1].Trim(); break;
                        case "Uid": this.UserName = h[1].Trim(); break;
                        case "Database": this.Database = h[1].Trim(); break;
                        case "Server": this.Server = h[1].Trim(); break;
                        case "Port": this.Port = Convert.ToInt32(h[1].Trim()); break;
                    }
                }
            }

            void IDisposable.Dispose()
            {
                this.Dispose(true);
            }

            public void Dispose(bool b)
            {
                GC.Collect();
                GC.SuppressFinalize(this);
            }
            
        }

        #region Common

        public static bool ParseBoolean(object value)
        {
            if (value == DBNull.Value || value.ToString() == "")
            {
                return false;
            }
            else
            {
                return Convert.ToBoolean(Convert.ToInt32(value.ToString().Replace("b", "").Replace("'", "")));
            }
        }

        public DataTable FillSchema(string TableName)
        {
            try
            {
                mConn.Open();
                using (DataTable dtSchema = mConn.GetSchema("columns"))
                {
                    return (from k in dtSchema.AsEnumerable()
                            where k["TABLE_NAME"].ToString().ToLower() == TableName.ToLower()
                            select k).CopyToDataTable();
                }
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

        #endregion

    }
}
