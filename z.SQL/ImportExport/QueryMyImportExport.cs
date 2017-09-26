using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data;
using MySql.Data.MySqlClient;
using System.IO;
using System.Threading;
using System.Diagnostics;

namespace z.SQL.ImportExport
{
    public class QueryMyImportExport
    {
        private QueryMy.QueryArgs sqlargs;
       // private MySqlScript import;

        public delegate void ExecuteStatement(int line, int position, string message);
        public delegate void ErrorStatement(Exception ex);
        public delegate void ProgressChange(string Percent, int i, int max);


       // public event ExecuteStatement onStatementExec;
        public event ErrorStatement onStatementError;
        public event EventHandler onScriptCompleted;
        // public event ProgressChange onProgressChange;

        MySqlScript scrpt = new MySqlScript();
        private ManualResetEvent reset = new ManualResetEvent(false);

        public QueryMyImportExport(QueryMy.QueryArgs sqlargs) { this.sqlargs = sqlargs; }

        public void Import(string filepath)
        {
            try
            {
                ////if (onStatementExec != null) scrpt.StatementExecuted += (a, b) => onStatementExec(b.Line, b.Position, b.StatementText);
                ////if (onStatementError != null) scrpt.Error += (a, b) => onStatementError(b.Exception);
                ////if (onScriptCompleted != null) scrpt.ScriptCompleted += onScriptCompleted;
                //System.Threading.Tasks.Task.Factory.StartNew(() =>
                //{
                //    try
                //    {

                //        using (QueryMy sql = new QueryMy(this.sqlargs))
                //        {
                //            scrpt.StatementExecuted += (a, b) =>
                //            {
                //                reset.Set();
                //            };

                //            int i = 0;
                //            List<Tuple<string, string>> scr = ParseDumpMySQL(Command);
                //            foreach (Tuple<string, string> str in scr)
                //            {
                //                int percent = i * 100 / scr.Count;
                //                this.onProgressChange?.Invoke(string.Format("{0}%", percent), i, scr.Count);
                //                switch (str.Item1)
                //                {
                //                    case ";":
                //                        sql.ExecNonQuery(str.Item2);
                //                        break;
                //                    case ";;":
                //                        sql.OpenConnection();
                //                        scrpt.Connection = sql.mConn;
                //                        scrpt.Query = str.Item2;
                //                        scrpt.Execute();
                //                        reset.WaitOne();
                //                        break;
                //                }
                //                i++;
                //            }
                //        }
                //        if (onScriptCompleted != null) this.onScriptCompleted(this, EventArgs.Empty);
                //    }
                //    catch (Exception ex)
                //    {
                //        if (onStatementError != null) onStatementError(ex);
                //    }
                //});

                using (MySqlCommand cmd = new MySqlCommand())
                {
                    cmd.Connection = new MySqlConnection(sqlargs.GetConnectionString());
                    cmd.Connection.Open();
                    using (MySqlBackup bck = new MySqlBackup(cmd))
                    {
                        //bck.ExportCompleted += (o, e) =>
                        //{
                        //    if (e.HasError) onStatementError?.Invoke(e.LastError);
                        //    onScriptCompleted?.Invoke(this, EventArgs.Empty);
                        //};
                        //bck.ExportToFile(mfile);
                        bck.ImportCompleted += (o, e) =>
                        {
                            if (e.HasErrors) onStatementError?.Invoke(e.LastError);
                            onScriptCompleted?.Invoke(this, EventArgs.Empty);
                        };
                        bck.ImportFromFile(filepath);
                    }
                }
            }
            catch (MySqlException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Export(string mfile)
        {
            try
            {
                using (MySqlCommand cmd = new MySqlCommand())
                {
                    cmd.Connection = new MySqlConnection(sqlargs.GetConnectionString());
                    cmd.Connection.Open();
                    using (MySqlBackup bck = new MySqlBackup(cmd))
                    {
                        bck.ExportCompleted += (o, e) =>
                        {
                            if (e.HasError) onStatementError?.Invoke(e.LastError);
                            onScriptCompleted?.Invoke(this, EventArgs.Empty);
                        };
                        bck.ExportToFile(mfile);
                    }
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public List<Tuple<string, string>> ParseDumpMySQL(string dumpfile)
        {
            try
            {
                List<Tuple<string, string>> command = new List<Tuple<string, string>>();
                List<string> commc = new List<string>();
                string[] dump = File.ReadAllLines(dumpfile);

                string delimiter = ";";
                bool blocked = false;
                string curparam = "";
                bool proc = false;
                foreach (string str in dump)
                {
                    if (str.Trim() == "") continue;
                    if (str.Substring(0, "--".Length) == "--") continue; //remove comments
                    if (str.Substring(0, "/*".Length) == "/*") { blocked = false; curparam = "/*"; }
                    if (str.Substring(0, "*/".Length) == "*/") { blocked = true; }

                    switch (str.Trim())
                    {
                        case "DELIMITER ;;": proc = true; break;
                        case "DELIMITER ;":
                            commc.Add(str.Trim());
                            proc = false;
                            break;
                    }

                    if (blocked == true && curparam != "/*")
                    {
                        commc.Add(str.Trim());
                    }

                    if (proc)
                        commc.Add(str.Trim());

                    if (blocked) curparam = "";

                    if (str.Trim().LastIndexOf(";") != -1 && str.Trim().LastIndexOf(";;") == -1)
                    {
                        if (!proc)
                        {
                            delimiter = (str.Trim() == "DELIMITER ;") ? ";;" : ";";
                            if (str.Trim() != "" && commc.Count == 0) commc.Add(str.Trim());
                            command.Add(new Tuple<string, string>(delimiter, string.Join("\r\n", commc.ToArray())));
                            commc.Clear();
                        }
                    }
                }

                return command;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void Dispose()
        {
            GC.Collect();
            GC.SuppressFinalize(this);
        }
    }
}
