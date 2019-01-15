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

        public QueryFire(IQueryArgs args)
        {
            Conn = new SqlConnection(args.GetConnectionString());
        }

        public QueryFire(IQueryArgs args, CancellationToken cancellationToken) : this(args)
        {
            this.CancellationToken = cancellationToken;
        }

        public async void Execute(string CommandText, string[] Parameter, object[] Value)
        {
            await Task.Run(() =>
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
                        CancellationToken.Register(() => cmd.Cancel());
                        cmd.ExecuteNonQuery();
                    }
                }
                catch (Exception ex)
                {
                    Conn?.Close();
                    GC.Collect();
                    throw ex;
                }
            }, CancellationToken);
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
