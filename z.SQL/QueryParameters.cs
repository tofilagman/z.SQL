using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace z.SQL
{
    public class QueryParameters
    {
        public int ORDINAL_POSITION { get; set; }
        public string PARAMETER_NAME { get; set; }
        public string DATA_TYPE { get; set; }
    }

    public class SchemaIndexes
    {
        public int Index { get; set; }
        public int ID_Orig { get; set; }
        public int ID_Table { get; set; }
    }
}
