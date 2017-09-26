using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using z.Data;

namespace z.SQL
{
    public class ScriptParameters
    {

        private Pair Parameters { get; set; }

        public ScriptParameters()
        {
            this.Parameters = new Pair();
        }

        public void AddParameter(string Name, object Value)
        {
            Parameters.Add(Name, Value);
        }
         
    }
}
