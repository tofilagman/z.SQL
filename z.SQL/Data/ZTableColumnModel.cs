using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace z.SQL.Data
{
    public class ZTableColumnModel
    {
        public IEnumerable<ColData> ColString { get; set; }

        public IEnumerable<string> SchemaString { get; set; }
    }

    public class ColData
    {
        public string Insert { get; set; }
        public string OutInsert { get; set; }
        public string TargetInsert { get; set; }
        public string Update { get; set; }
    }
}
