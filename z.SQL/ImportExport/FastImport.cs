using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using MySql.Data.MySqlClient;

using System.Threading;
using z.Data;

namespace z.SQL.ImportExport
{
    /// <summary>
    /// LJ 20151203
    /// Fast Importing of SQL Dump File
    /// </summary>
    public class FastImport : IDisposable
    {
        List<Thread> mtsf = new List<Thread>();
        private System.Timers.Timer tmr;
        private bool erroroccur = false;
        private QueryMy.QueryArgs args;

        public delegate void LogHandler(string Message);
        public delegate void DoneHandler(DoneEventArgs e);

        public event LogHandler Log;
        public event DoneHandler Done;

        public int RunningThread { get; private set; }
        private Exception LastException { get; set; }

        public FastImport(QueryMy.QueryArgs args)
        {
            this.args = args;
        }

        public void Start(string MFile)
        {
            try
            {
                erroroccur = false;
                Log?.Invoke("Import Started: " + DateTime.Now.ToString("HH:mm:ss"));
                string[] mfile = File.ReadAllLines(MFile);
                tmr = new System.Timers.Timer();
                tmr.Interval = 100;
                tmr.Elapsed += Tmr_Elapsed;
                var j = mfile.Where(x => !x.Trim().ToLower().StartsWith("insert")).ToArray();

                var sql = new QueryMy(this.args);

                Semaphore sm = new Semaphore(10, 10);

                sql.ImportString(string.Join("\r\n", j), (o, p) =>
                {
                    //if (p.HasErrors)
                    //{
                    //    Done(new DoneEventArgs() { HasFaulted = true, Exception = p.LastError });
                    //    return;
                    //}
                    mtsf.Clear();
                    mfile.Where(x => x.Trim().ToLower().StartsWith("insert")).Batch(300).Each(x =>
                    {
                        var mt = new Thread(() =>
                        {
                            using (var mk = new QueryMy(args))
                            {
                                try
                                {
                                    mk.ExecNonQuery(string.Join("\r\n", x.ToArray()));
                                }
                                catch (Exception ex)
                                {
                                    erroroccur = true;
                                    this.LastException = ex;
                                    Log?.Invoke(ex.Message);
                                }
                            }
                        });
                        mtsf.Add(mt);
                        mt.Priority = ThreadPriority.Lowest;
                        mt.SetApartmentState(ApartmentState.MTA);
                        mt.IsBackground = true;
                        mt.Start();
                    });
                    tmr.Start();
                }, (o, p) =>
                {
                    // erroroccur = true;
                    Done(new DoneEventArgs() { HasFaulted = true, Exception = p.ex });
                    Log?.Invoke(p.ex.Message);
                }, (o, p) => { });
            }
            catch (Exception ex)
            {
                Done(new DoneEventArgs() { HasFaulted = true, Exception = ex });
            }
        }

        private void Tmr_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            this.RunningThread = mtsf.Count();
            mtsf.Where(x => x.ThreadState == ThreadState.Stopped).Each(x => mtsf.Remove(x));
            if (mtsf.Count() == 0)
            {
                tmr.Stop();
                if (erroroccur)
                    Done(new DoneEventArgs() { HasFaulted = true, Exception = this.LastException });
                else
                    Done(new DoneEventArgs());
                Log?.Invoke("Import Completed: " + DateTime.Now.ToString("HH:mm:ss"));
            }
        }

        public void Dispose()
        {
            GC.Collect();
            GC.SuppressFinalize(this);
        }

        public class DoneEventArgs : EventArgs
        {
            public bool HasFaulted { get; set; }
            public Exception Exception { get; set; }
        }
    }
}
