using System;
using System.Data;
using z.Data;

namespace z.SQL
{

    public interface IQuery
    {
        //[MTAThread]
        //void OpenConnection();

        //[MTAThread]
        //void Parameterize(string[] Parameter, object[] Value);

        [MTAThread]
        DataSet ExecQuery(string Command);
        
        [MTAThread]
        DataSet ExecQuery(string Command, params object[] Value);

        //[MTAThread]
        //DataSet ExecQuery(string Command, string[] Parameter, object[] Value, CommandType type = CommandType.Text);

        [MTAThread]
        void ExecNonQuery(string Command);

        [MTAThread]
       void ExecNonQuery(string Command, string[] Parameter, object[] Value, CommandType type = CommandType.Text);

        [MTAThread]
        void ExecNonQuery(string Command, params object[] args);

        [MTAThread]
        object ExecScalar(string Command);
        
        [MTAThread]
        object ExecScalar(string Command, params object[] Value);
        
        IQueryArgs ConnectionParameter { get; }
    }
}
