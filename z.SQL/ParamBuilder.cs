using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace z.SQL
{
   public class ParamBuilder : List<string>
    {

       public void AddFormat(string data, params object[] args){
           this.Add(string.Format(data, args));
       }

       public string ToString(string separator = ",")
       {
           return string.Join(separator, this.ToArray());
       }

    }
}
