using System;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using Xunit;
using z.SQL;
using z.SQL.Data;

namespace SQLTest
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {
            var gg = QueryCom.SQLFormat("asdasdsd");
            var okj = QueryCom.SQLFormat(DateTime.Now);
            var ng = Extensions.EncryptA("asd", 41);
        }


        [Fact]
        public void TestTable()
        {

            var args = new Query.QueryArgs("Server=192.168.100.12;Initial Catalog=InSysProMaxLive20181126;User ID=sa;Password=dev123sql$%^;");

            using (var dfg = new ZTable(args, "tSellerHRIS", false))
            {
                dfg.Load(100);
            }
        }


        [Fact] //for testing
        public void QueryFired()
        {
            var args = new Query.QueryArgs("Server=tcp:jollibeesvr.database.windows.net,1433;Initial Catalog=InSysJFCDirect_Test;Persist Security Info=False;User ID=jollibeeadmin;Password=P@ssw0rd12345;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;");

            var cts = new CancellationTokenSource();
            using (var sql = new QueryFire(args, cts.Token))
            {
                sql.Message += (s, e) => { };

                Task.Run(() =>
                {
                    Task.Delay(3000);
                    cts.Cancel();
                });

                sql.Execute("pComputeHours @ID, 1", 220);
            }
        }
    }
}
