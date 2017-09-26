using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace z.SQL
{
    public interface IQueryArgs
    {
        String Server { get; set; }
        String UserName { get; set; }
        String Password { get; set; }
        String Database { get; set; }
        int Port { get; set; }

        string GetConnectionString();
    }
}
