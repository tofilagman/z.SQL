using System;
using System.Data.SqlClient;
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
    }
}
