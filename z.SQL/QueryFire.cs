using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace z.SQL
{
    /// <summary>
    /// @LJGomez 20161104
    /// </summary>
    public class QueryFire : IDisposable
    {
        private SqlConnection Conn { get; set; }

        public delegate void MessageHandler(string Status, int Number);
        public event MessageHandler Message;

        private CancellationToken CancellationToken;

        public QueryFire(SqlConnectionStringBuilder args)
        {
            Conn = new SqlConnection(args.ConnectionString);
            this.CancellationToken = new CancellationTokenSource().Token;
        }

        public QueryFire(SqlConnectionStringBuilder args, CancellationToken cancellationToken) : this(args)
        {
            this.CancellationToken = cancellationToken;
        }

        public void Execute(string CommandText, string[] Parameter, object[] Value)
        {
            try
            {
                Conn.InfoMessage += (s, e) => Message?.Invoke(e.Errors[0].Message, e.Errors[0].Number);
                Conn.FireInfoMessageEventOnUserErrors = true;
                Conn.Open();
                using (var cmd = new SqlCommand())
                {
                    cmd.Parameterize(Parameter, Value);
                    cmd.CommandText = CommandText;
                    cmd.CommandTimeout = 0;
                    cmd.CommandType = System.Data.CommandType.Text;
                    cmd.Connection = Conn;
                    cmd.ExecuteNonQueryAsync().Wait(CancellationToken);
                }
            }
            catch (Exception ex)
            {
                Conn?.Close();
                GC.Collect();
                throw ex;
            }
        }

        public void Execute(string CommandText, params object[] Value)
        {
            string[] arr = CommandText.ParseParameter().ToArray();
            if (arr.Length != Value.Length) throw new Exception("Specified Paramater and Value count is Incorrect");
            Execute(CommandText, arr, Value);
        }

        public void Dispose()
        {
            Conn?.Dispose();
            GC.Collect();
            GC.SuppressFinalize(this);
        }

    }
}
