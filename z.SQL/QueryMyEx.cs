using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;

namespace z.SQL
{
    /// <summary>
    /// LJ 20150806
    /// MySQL Extension
    /// </summary>
    public class QueryMyEx : QueryMy
    {

        public void Save(DataTable dt, string TableName, string Key = "ID")
        {
            try
            {
                foreach (DataRow dr in dt.Rows)
                {

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }


        public void Save(DataRow dr, string TableName, string Key = "ID")
        {
            try
            {
                string qry = string.Format("Select count(1) from {0}");
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
