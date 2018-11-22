using System;
using System.Text;
using System.Management.Automation;
using Config;

namespace TSI_PerformanceTesting
{
	interface ITest
	{
		void SetPath();
		bool IsRunning();
		void Run();
		void Stop();
	}
	interface IAdapter
	{
		int GetStaticReadCount();
		void SetDomainName(string domain);
		void InitWriteTest(int v);
		void InitReadTest(int v);
		void InitMixTest(int f);
		void PrepareWriteData(int v);
		void PrepareWriteDataWithTime(int v, DateTime ts);
		void Send(ref System.Diagnostics.Stopwatch sw);
		bool CheckInsert();

		void SetStartTime(DateTime time);
		void SetNowTime(DateTime time);
		void PrepareReadOne(TimeSpan ts);
		void ReadOne(TimeSpan ts);

		void PrepareReadMultiple(int v, TimeSpan ts);
		void PrepareReadMultipleLatest(int v, TimeSpan ts);
		void ReadMultiple(int v, TimeSpan ts);

		void ReadStatic(int v, TimeSpan ts);
		void ReadStaticC(int v, TimeSpan ts);
		bool CheckClientConnections(int v);

	}

	class Telemetry
	{
		private PowerShell ps;
		private string VMName;
		private string Path;
		private string ID;
		private int Variables;
		private IAdapter adapter;
		public volatile bool mRunning;
		private static System.Threading.Thread t;

		public Telemetry(string vm, string path, int v, IAdapter a)
		{
			ps = PowerShell.Create();
			VMName = vm;
			Path = path;
			Variables = v;
			adapter = a;
			ID = "";
		}

		public void SetID(string id) { ID = "_"+id;}

		public void InitTelemetryCollection(string id)
		{
			ps.AddScript("Enable-VMResourceMetering -VMName '" + VMName + "'");
			ps.Invoke();
			mCheckError();
			ps.AddScript("Reset-VMResourceMetering -VMName '" + VMName + "'");
			ps.Invoke();
			mCheckError();
			ps.AddScript("$UtilizationReport = Get-VM '" + VMName + "' | Measure-VM");
			ps.Invoke();
			mCheckError();
			ps.AddScript("$UtilizationReport | Format-List | Out-FIle -filepath " + Path + "\\" + id + "telemetry_initial.txt");
			ps.Invoke();
			mCheckError();
			ps.AddScript("$UtilizationReport.HardDiskMetrics | Format-List | Out-FIle -filepath " + Path + "\\"+id+"telemetry_initial_hd.txt");
			ps.Invoke();
			mCheckError();
			ps.AddScript("Reset-VMResourceMetering -VMName '" + VMName + "'");
			ps.Invoke();
			mCheckError();
			mConnectToVM();
		}
		public void CollectVariableRunTelemetry(int count)
		{
			var doc = "$UtilizationReport"+count;
			ps.AddScript(doc+" = Get-VM '" + VMName + "' | Measure-VM");
			ps.Invoke();
			mCheckError();
			ps.AddScript(doc+" | Format-List | Out-FIle -filepath " + Path + "\\telemetry" + ID+"_"+count + "_variables.txt");
			ps.Invoke();
			mCheckError();
			ps.AddScript(doc+".HardDiskMetrics | Format-List | Out-FIle -filepath " + Path + "\\telemetry" + ID + "_" + count + "_variables_hardisk.txt");
			ps.Invoke();
			mCheckError();
			ps.AddScript(doc+".NetworkMeteredTrafficReport | Format-List | Out-FIle -filepath " + Path + "\\telemetry" + ID + "_" + count + "_variables_network.txt");
			ps.Invoke();
			mCheckError();
		}
		public void CloseTelemetryCollecting()
		{
			
			ps.AddScript("$UtilizationReport = Get-VM '" + VMName + "' | Measure-VM");
			ps.Invoke();
			mCheckError();
			ps.AddScript("$UtilizationReport | Format-List | Out-FIle -filepath " + Path + "\\telemetry" + ID + "_" + Variables + "_variables.txt");
			ps.Invoke();
			ps.AddScript("$UtilizationReport.HardDiskMetrics | Format-List | Out-FIle -filepath " + Path + "\\telemetry" + ID + "_" + Variables + "_variables_hardisk.txt");
			ps.Invoke();
			mCheckError();
			ps.AddScript("$UtilizationReport.NetworkMeteredTrafficReport | Format-List | Out-FIle -filepath " + Path + "\\telemetry" + ID + "_" + Variables + "_variables_network.txt");
			ps.Invoke();
			mCheckError();
			ps.AddScript("Reset-VMResourceMetering -VMName '" + VMName + "'");
			ps.Invoke();
			mCheckError();
			ps.AddScript("Disable-VMResourceMetering -VMName '" + VMName + "'");
			ps.Invoke();
			mCheckError();
			CloseVMConnection();

		}
		public void CloseVMConnection() { mRunning = false; t.Join(); }

		private void mCheckError()
		{
			if (ps.Streams.Error.Count > 0) { foreach (var e in ps.Streams.Error.ReadAll()) { System.Console.WriteLine("Powershell error: " + e.ToString()); } }
			ps.Commands = new PSCommand();
		}
		private void mConnectToVM()
		{
			mRunning = true;
			t = new System.Threading.Thread(mCollectMetrics) { IsBackground = true };
			t.Start();
		}
		private void mCollectMetrics()
		{
			using (PowerShell ps1 = PowerShell.Create())
			{
				while (mRunning)
				{ 

					ps1.AddScript("$line = (Get-Counter -Counter '\\Hyper-V Hypervisor Virtual Processor("+VMName.ToLower()+":hv vp *)\\% Guest Run Time').Readings.Split([Environment]::NewLine)");
					ps1.Invoke();
					if (ps.Streams.Error.Count > 0) { foreach (var e in ps.Streams.Error.ReadAll()) { System.Console.WriteLine("Powershell error: " + e.ToString()); } }
					ps1.AddScript("$line[1]+\"`t\"+$line[4]+\"`t\"+$line[7]+\"`t\"+$line[10] | Out-File " + Path+"\\cpu_metrics.txt -Append");
					ps1.Invoke();
					if (ps.Streams.Error.Count > 0) { foreach (var e in ps.Streams.Error.ReadAll()) { System.Console.WriteLine("Powershell error: " + e.ToString()); } }
					System.Threading.Thread.Sleep(5000);

				}
			}
		}
	}

	class WriteTest : ITest
	{
		private static ConfigFile conf;
		private static IAdapter adapter;
		private static int frequency = 1;
		private static int variables = 0;
		private static int iterations = 0;
		private static int increament = 10000;
		private static int stop_value = 300000;
		private static TimeSpan inititerations;
		private static int initvariables = 0;
		private static Telemetry telemetry;
		private static bool mRunning=false;
		private static string Path;
		//Measurements file
		public static System.IO.StreamWriter outputfile;
		//Public stopwatch for measuring delay to read newly written value back(when data is available?)
		public static System.Diagnostics.Stopwatch onevariable_watch = new System.Diagnostics.Stopwatch();

		#region Common Write-Testcase functions
		//Common Write-Testcase functions
		public static void PrepareData(int v)
		{
			adapter.PrepareWriteData(v);
		}
		public static void Send(ref System.Diagnostics.Stopwatch sw)
		{
			adapter.Send(ref sw);
		}
		public static bool CheckInsert()
		{
			return adapter.CheckInsert();
		}
		#endregion

		public WriteTest(ConfigFile c, IAdapter a)
		{
			conf = c;
			adapter = a;
			frequency = Int32.Parse(conf.ReadValue("WriteTest", "UpdateFrequency"));
			variables = Int32.Parse(conf.ReadValue("WriteTest", "Variables"));
			iterations = Int32.Parse(conf.ReadValue("WriteTest", "Iterations"));
			increament = Int32.Parse(conf.ReadValue("WriteTest", "Increament"));
			stop_value = Int32.Parse(conf.ReadValue("WriteTest", "StopValue"));
			inititerations = TimeSpan.Parse(conf.ReadValue("WriteTest", "InitIterations"));
			initvariables = Int32.Parse(conf.ReadValue("WriteTest", "InitVariables"));
			adapter.InitWriteTest(variables);
		}
		public void SetPath() { Path = Path = Program.Path + "\\WriteTest"; System.IO.Directory.CreateDirectory(Path); }
		public bool IsRunning() { return mRunning; }
		public void Run()
		{
			mRunning = true;
			int sleep = 1000 / frequency;
			using (outputfile = new System.IO.StreamWriter(Path + "\\write_test.txt"))
			{
				outputfile.WriteLine("Database: " + Program.Database + "\t Variables: " + variables + "\t Iterations: " + iterations+"\t WriteFrequency: "+frequency);
				outputfile.WriteLine("Insert duration\tFinal duration\tRead back\tSpeed");

				//Write-TestCase
				System.Console.WriteLine("Start Write-TestCase");
				telemetry = new Telemetry(Program.VmName, Path, variables, adapter);
				telemetry.InitTelemetryCollection("write");
				var w_mock = new System.Diagnostics.Stopwatch();
				var endinittime = System.DateTime.Now.Add(inititerations);
				while(endinittime.Subtract(System.DateTime.Now).TotalSeconds>0)
				{
					if (!mRunning) break;
					var st = System.Diagnostics.Stopwatch.StartNew();
					PrepareData(initvariables);
					Send(ref w_mock);
					st.Stop();
					double elap = st.Elapsed.TotalSeconds;
					int wait = sleep - (int)(elap * 1000);
					if (wait > 0) System.Threading.Thread.Sleep(wait);
				}
				while (mRunning)
				{
					for (int j = 0; j < iterations; j++)
					{
						if (!mRunning) break;
						var st = System.Diagnostics.Stopwatch.StartNew();
						if (frequency > 1)
						{
							for (int i = 1; i < frequency; i++)
							{
								var st0 = System.Diagnostics.Stopwatch.StartNew();
								PrepareData(variables);
								Send(ref w_mock);
								double elap0 = st0.Elapsed.TotalSeconds;
								int wait0 = sleep - (int)(elap0 * 1000);
								if (wait0 > 0) System.Threading.Thread.Sleep(wait0);
							}
						}
						var check00 = st.Elapsed.TotalSeconds;
						PrepareData(variables);
						var check0 = st.Elapsed.TotalSeconds;
						Send(ref onevariable_watch);
						var check1 = st.Elapsed.TotalSeconds;
						while (true)
						{
							bool x = CheckInsert();
							if (x) break;
							System.Threading.Thread.Sleep(100);
						}
						onevariable_watch.Stop();
						st.Stop();
						double elap = st.Elapsed.TotalSeconds;
						double d=check0-check00;
						string logtext = "Insert duration: " + check1 + " Final duration: " + elap + " Read back: " + onevariable_watch.Elapsed.TotalSeconds+" PrepareData: "+d;
						System.Console.WriteLine(logtext);
						float speed = (float)(variables / elap);
						System.Console.WriteLine("Speed: " + speed);
						
						try
						{
							outputfile.WriteLine(check1 + "\t" + elap + "\t" + onevariable_watch.Elapsed.TotalSeconds + "\t" + speed);
						}
						catch (System.ObjectDisposedException) { }
						onevariable_watch.Reset();
						int wait = 1000 - (int)(elap * 1000);
						if (wait > 0) System.Threading.Thread.Sleep(wait);
					}
					if (mRunning)
					{
						variables += increament; //Increase updated variable count
						telemetry.CollectVariableRunTelemetry(variables);
						if (variables >= stop_value) { mRunning = false; break; }
						try { outputfile.WriteLine("New Variable count: " + variables + "\t"); }
						catch (System.ObjectDisposedException) { }
					}
				}
				telemetry.CloseVMConnection();
			}
		}
		public void Stop()
		{
			mRunning = false;
			outputfile.Flush();
			outputfile.Close();
			outputfile.Dispose();
			telemetry.CloseTelemetryCollecting();
		}
	}

	class ReadTest : ITest
	{
		public static volatile bool Running;
		public static int Frequency;

		private static ConfigFile conf;
		private static IAdapter adapter;
		private static int writefrequency;
		private static TimeSpan filltime;
		private static string timespaninitvalue;
		private static int timespaniterations;
		private static string maxtimespan;
		private static string timespaninc;
		private static int variablesinitvalue;
		private static int variableiterations;
		private static int maxvariables;
		private static int variableinc;
		private static string variabletimespan;
		private static System.Collections.Generic.List<System.Threading.Thread> clientlist = new System.Collections.Generic.List<System.Threading.Thread>();
		private static int clientsinitvalue;
		private static int clientiterations;
		private static int maxclients;
		private static int clientinc;
		private static string clienttimespan;
		private static int clientvariables;
		private static Telemetry telemetry;
		private static bool mRunning = false;
		private static string Path;
		//Measurements file
		public static System.IO.StreamWriter outputfile;

		#region Common Read-Testcase functions
		struct Params { public int values;public TimeSpan ts; public Params(int v, TimeSpan t) { values = v; ts = t; } }
		public void PrepareReadOne(TimeSpan ts) { adapter.PrepareReadOne(ts); }
		public void ReadOne(TimeSpan ts) { adapter.ReadOne(ts); }
		public void PrepareReadMultiple(int v, TimeSpan ts) { adapter.PrepareReadMultiple(v, ts); }
		public void ReadMultiple(int v, TimeSpan ts) { adapter.ReadMultiple(v, ts); }
		public void ReadStatic(int v, TimeSpan ts) { adapter.ReadStatic(v, ts); }
		public void ReadStaticC(object arg) { Params args = (Params)arg; adapter.ReadStaticC(args.values, args.ts); }
		#endregion

		public ReadTest(ConfigFile c, IAdapter a)
		{
			conf = c;
			adapter = a;
			writefrequency = Int32.Parse(conf.ReadValue("ReadTest", "WriteFrequency"));
			filltime = TimeSpan.Parse(conf.ReadValue("ReadTest", "FillTime"));
			Frequency = writefrequency;
			timespaninitvalue = conf.ReadValue("ReadTest", "TimeSpanInitValue");
			timespaniterations = Int32.Parse(conf.ReadValue("ReadTest", "TimeSpanIterations"));
			maxtimespan = conf.ReadValue("ReadTest", "MaxTimeSpan");
			timespaninc = conf.ReadValue("ReadTest", "TimeSpanIncreament");
			variablesinitvalue = Int32.Parse(conf.ReadValue("ReadTest", "VariablesInitValue"));
			variableinc = Int32.Parse(conf.ReadValue("ReadTest", "VariableIncreament"));
			maxvariables = Int32.Parse(conf.ReadValue("ReadTest", "MaxVariables"));
			variableiterations = Int32.Parse(conf.ReadValue("ReadTest", "VariableIterations"));
			variabletimespan = conf.ReadValue("ReadTest", "VariableTimeSpan");
			clientsinitvalue = Int32.Parse(conf.ReadValue("ReadTest", "ClientsInitValue"));
			maxclients = Int32.Parse(conf.ReadValue("ReadTest", "MaxClients"));
			clientinc = Int32.Parse(conf.ReadValue("ReadTest", "ClientIncreament"));
			clientiterations = Int32.Parse(conf.ReadValue("ReadTest", "ClientIterations"));
			clienttimespan = conf.ReadValue("ReadTest", "ClientTimeSpan");
			clientvariables = Int32.Parse(conf.ReadValue("ReadTest", "ClientVariables"));
			var maxv = Math.Max(maxvariables, clientvariables);
			adapter.InitReadTest(maxv);
		}
		private void WriteDataSet()
		{
			TimeSpan ts = TimeSpan.Parse(maxtimespan);
			TimeSpan ts2 = TimeSpan.Parse(variabletimespan);
			TimeSpan ts3 = TimeSpan.Parse(clienttimespan);
			double max_ts = Math.Max(ts.TotalSeconds, Math.Max(ts2.TotalSeconds, ts3.TotalSeconds));
			double t_add = (double)1 / writefrequency;
			int iterat = (int)(max_ts / t_add);
			int max_v = Math.Max(maxvariables, clientvariables);
			var sw_one = new System.Diagnostics.Stopwatch();
			DateTime time = DateTime.UtcNow;
			time = time.Subtract(new TimeSpan(0, 0, (int)max_ts));
			var org_time = time;
			adapter.SetStartTime(time);
			System.Console.WriteLine("Write data for: " + max_v + " over " + max_ts + " seconds");
			for (int i = 0; i < iterat; i++)
			{
				try
				{
					adapter.PrepareWriteDataWithTime(max_v, time);
					adapter.Send(ref sw_one);
				}
				catch (System.Exception e) { System.Console.WriteLine(e.Message); i--; continue; }
				if (i != (iterat - 1)) time = time.AddSeconds(t_add);
			}
			System.Console.WriteLine("Data written");
			DateTime endtime = DateTime.UtcNow;
			adapter.SetNowTime(endtime);
			var t = System.DateTime.Now.Add(filltime);
			while (t.Subtract(System.DateTime.Now).TotalSeconds > 0)
			{
				try
				{
					adapter.PrepareWriteData(max_v);
					adapter.Send(ref sw_one);
					System.Threading.Thread.Sleep(900);
				}
				catch (System.Exception e) { continue; }
			}
			if (filltime.TotalSeconds == 0) { 
				System.Console.WriteLine("Start waiting");
				adapter.PrepareWriteDataWithTime(max_v, time);
				System.Threading.Thread.Sleep(60000);
				while (true)
				{
					bool x = adapter.CheckInsert();
					if (x) break;
					System.Threading.Thread.Sleep(1);
				}
			}
		}

		public void SetPath() { Path = Program.Path + "\\ReadTest"; System.IO.Directory.CreateDirectory(Path); }
		public bool IsRunning() { return mRunning; }
		public void Run()
		{
			mRunning = true;
			#region WriteDataSet
			WriteDataSet();
			if (System.Threading.ThreadPool.SetMinThreads(300, 300)) System.Console.WriteLine("Set minimum was successful");
			System.Threading.Thread.Sleep(5000);
			#endregion
			#region TimeSpan Test
			using (outputfile = new System.IO.StreamWriter(Path + "\\timespan_read_test.txt"))
			{
				outputfile.WriteLine("Database: " + Program.Database + "\t MaxTimeSpan: " + maxtimespan + "\t Iterations: " + timespaniterations +"\t WriteFrequency: "+writefrequency);
				outputfile.WriteLine("Timespan\tDuration(ms)");

				//TimepanRead-TestCase
				System.Console.WriteLine("Start TimepanRead-TestCase");
				TimeSpan timespanTS = TimeSpan.Parse(timespaninitvalue);
				TimeSpan maxtimespanTS = TimeSpan.Parse(maxtimespan);
				telemetry = new Telemetry(Program.VmName, Path, (int)timespanTS.TotalMinutes, adapter);
				telemetry.SetID("timespan");
				telemetry.InitTelemetryCollection("timespan");
				while (mRunning)
				{
					
					for(int i = 0; i < timespaniterations; i++)
					{
						PrepareReadOne(timespanTS);
						var sw = System.Diagnostics.Stopwatch.StartNew();
						ReadOne(timespanTS);
						sw.Stop();
						double elap = sw.Elapsed.TotalMilliseconds;
						string logtext = "Timespan: "+timespanTS+" Duration: " +elap;
						System.Console.WriteLine(logtext);
						try
						{
							outputfile.WriteLine(timespanTS + "\t" + elap);
						}
						catch (System.ObjectDisposedException) { }
					}
					telemetry.CollectVariableRunTelemetry((int)timespanTS.TotalMinutes);
					timespanTS += TimeSpan.Parse(timespaninc);					
					if (timespanTS > maxtimespanTS) break;
					try { outputfile.WriteLine("New TimeSpan: " + timespanTS + "\t"); }
					catch (System.ObjectDisposedException) { }

				}
				telemetry.CloseVMConnection();
			}
			#endregion
			#region Variable Test
			using(outputfile = new System.IO.StreamWriter(Path + "\\variable_read_test.txt"))
			{
				outputfile.WriteLine("Database: " + Program.Database + "\t MaxVariables: " + maxvariables + "\t Iterations: " + variableiterations+"\t Timespan: "+variabletimespan + "\t WriteFrequency: " + writefrequency);
				outputfile.WriteLine("Variables\tDuration(ms)");

				//VariableRead-TestCase
				System.Console.WriteLine("Start VariableRead-TestCase");
				int variables = variablesinitvalue;
				TimeSpan variabletimespanTS = TimeSpan.Parse(variabletimespan);
				telemetry = new Telemetry(Program.VmName, Path, variables, adapter);
				telemetry.SetID("variable");
				telemetry.InitTelemetryCollection("variable");
				while (mRunning)
				{
					for(int i = 0; i < variableiterations; i++)
					{
						PrepareReadMultiple(variables, variabletimespanTS);
						var sw = System.Diagnostics.Stopwatch.StartNew();
						ReadMultiple(variables, variabletimespanTS);
						sw.Stop();
						double elap = sw.Elapsed.TotalMilliseconds;
						string logtext = "Variables: " + variables + " Duration: " + elap;
						System.Console.WriteLine(logtext);
						try
						{
							outputfile.WriteLine(variables + "\t" + elap);
						}
						catch (System.ObjectDisposedException) { }
					}
					telemetry.CollectVariableRunTelemetry(variables);
					variables += variableinc;
					if (variables > maxvariables) break;
					try { outputfile.WriteLine("New Variable count: " + variables + "\t"); }
					catch (System.ObjectDisposedException) { }
				}
				telemetry.CloseVMConnection();
			}
			#endregion
			#region Client Test
			using(outputfile = new System.IO.StreamWriter(Path + "\\client_read_test.txt"))
			{
				outputfile.WriteLine("Database: " + Program.Database + "\t MaxClient: " + maxclients + "\t Iterations: " + clientiterations+"\t Timespan: "+clienttimespan+"\t Variables: "+clientvariables + "\t WriteFrequency: " + writefrequency);
				outputfile.WriteLine("Clients\tDuration(ms)");

				//ClientRead-TestCase
				System.Console.WriteLine("Start ClientRead-TestCase");
				int clients = clientsinitvalue;
				TimeSpan clienttimespanTS = TimeSpan.Parse(clienttimespan);
				Running = true;
				for (int i = 0; i < clients; i++)
				{
					System.Threading.Thread t = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ReadStaticC)) { IsBackground = false } ;
					Params a = new Params(clientvariables, clienttimespanTS);
					t.Start(a);
					clientlist.Add(t);
				}
				System.Threading.Thread.Sleep(2000);
				PrepareReadMultiple(clientvariables, clienttimespanTS);
				telemetry = new Telemetry(Program.VmName, Path, clients, adapter);
				telemetry.SetID("client");
				telemetry.InitTelemetryCollection("client");
				while (mRunning)
				{
					for(int i = 0; i < clientiterations; i++)
					{
						var sw = System.Diagnostics.Stopwatch.StartNew();
						ReadStatic(clientvariables, clienttimespanTS);
						sw.Stop();
						double elap = sw.Elapsed.TotalMilliseconds;
						string logtext = "Clients: " + clients + " Duration: " + elap;
						System.Console.WriteLine(logtext);
						try
						{
							outputfile.WriteLine(clients + "\t" + elap);
						}
						catch (System.ObjectDisposedException) { }
						
					}
				GC.Collect();
				GC.WaitForPendingFinalizers();
				GC.Collect();
				telemetry.CollectVariableRunTelemetry(clients);
				clients += clientinc;
				if (clients > maxclients) break;
				System.Threading.ThreadPool.GetAvailableThreads(out int awt, out int acpt);
				System.Console.WriteLine("Available: " + awt + " IO: " + acpt);
				while (clientlist.Count <= clients)
				{
					System.Threading.Thread t = new System.Threading.Thread(new System.Threading.ParameterizedThreadStart(ReadStaticC)) { IsBackground = false } ;
					Params a = new Params(clientvariables, clienttimespanTS);
					t.Start(a);
					clientlist.Add(t);
					//System.Threading.Thread.Sleep(500);
				}
				System.Threading.ThreadPool.GetAvailableThreads(out int awt2, out int acpt2);
				System.Console.WriteLine("After increase Available: " + awt2 + " IO: " + acpt2);
				System.Threading.Thread.Sleep(2000);
					try { outputfile.WriteLine("New Client count: " + clients + "\t"); }
					catch (System.ObjectDisposedException) { }
					System.Threading.Thread.Sleep(5000);
				}
				System.Console.WriteLine("ClientList.Count: " + clientlist.Count);
				foreach(var th in clientlist) { if (!th.IsAlive) System.Console.WriteLine("Client not alive"); }
				Running = false;
				foreach(var t in clientlist) { t.Join(); }
				telemetry.CloseVMConnection();
			}
			#endregion
		}
		public void Stop()
		{
			mRunning = false;
			//if (Running) { Running = false; foreach (var t in clientlist) { t.Join(); } }
			outputfile.Flush();
			outputfile.Close();
			outputfile.Dispose();
			telemetry.CloseTelemetryCollecting();
		}
	}

	class MixTest : ITest
	{
		private static ConfigFile conf;
		private static IAdapter adapter;
		private static Telemetry telemetry_p;
		private static Telemetry telemetry_c;
		public volatile bool c_mRunning = false;
		public volatile bool p_mRunning = false;
		public volatile bool p_done = false;
		private static string Path;
		private static int inititerations;
		private static int writefrequency;
		private static int increament;
		private static int max_var;
		private static int writevariables;
		private static int readvariables;
		private static int iterations;
		private static TimeSpan timespan;
		System.Threading.Thread producer;
		System.Threading.Thread consumer;
		//Measurements file
		public static System.IO.StreamWriter produce_outputfile;
		public static System.IO.StreamWriter consume_outputfile;

		#region Common Mix-Test function
		public struct Params { public string Path; public int Variables; public int Iterations; public TimeSpan timeSpan; public int inititerations; public int writefrequency; public Params(string p, int v, int i, TimeSpan ts, int ii, int wf) { Path = p; Variables = v;Iterations = i; timeSpan = ts; inititerations = ii; writefrequency = wf; } }
		public void Produce(object arg)
		{
			Params p = (Params)arg;
			using (produce_outputfile = new System.IO.StreamWriter(p.Path + "\\produce.txt"))
			{
				produce_outputfile.WriteLine("Database: " + Program.Database + "\t Variables: " + p.Variables + "\t Iterations: " + p.Iterations+"\t WriteFrequency: "+p.writefrequency+"\t InitIterations: "+p.inititerations);
				produce_outputfile.WriteLine("Insert duration\tFinal duration\tRead back\tSpeed");

				telemetry_p = new Telemetry(Program.VmName, p.Path, p.Variables, adapter);
				telemetry_p.InitTelemetryCollection("produce");
				System.Diagnostics.Stopwatch onevariable_watch = new System.Diagnostics.Stopwatch();
				var w_mock = new System.Diagnostics.Stopwatch();
				int sleep = (int)((1.0f / (double)p.writefrequency)*1000);
				while (p_mRunning)
				{
					System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
					var startime = System.DateTime.Now;
					var endtime = startime.Add(p.timeSpan);
					while(System.DateTime.Now.Subtract(endtime).TotalSeconds<=0)
					{
						if (!p_mRunning) { break; }
						sw.Restart();
						adapter.PrepareWriteData(p.Variables);
						adapter.Send(ref onevariable_watch);
						sw.Stop();
						int wait = sleep - (int)sw.ElapsedMilliseconds;
						if(wait>0) System.Threading.Thread.Sleep(wait);
					}
					while (p.Variables < max_var)
					{
						var percent = p.Iterations / 100;
						for (int j = 0; j < p.Iterations; j++)
						{
							if (!p_mRunning) { break; }
							sw.Restart();
							if (p.writefrequency > 1)
							{
								for (int i = 1; i < p.writefrequency; i++)
								{
									var st0 = System.Diagnostics.Stopwatch.StartNew();
									adapter.PrepareWriteData(p.Variables);
									adapter.Send(ref w_mock);
									double elap0 = st0.Elapsed.TotalSeconds;
									int wait0 = sleep - (int)(elap0 * 1000);
									if (wait0 > 0) System.Threading.Thread.Sleep(wait0);
								}
							}
							adapter.PrepareWriteData(p.Variables);
							
							adapter.Send(ref onevariable_watch);
							var check1 = sw.Elapsed.TotalSeconds;
							while (true)
							{
								bool x = adapter.CheckInsert();
								if (x) break;
								if (!p_mRunning) break;
							}
							onevariable_watch.Stop();
							sw.Stop();
							double elap = sw.Elapsed.TotalSeconds;
							float speed = (float)(p.Variables / elap);
							try
							{
								produce_outputfile.WriteLine(check1 + "\t" + elap + "\t" + onevariable_watch.Elapsed.TotalSeconds + "\t" + speed);
							}
							catch (System.ObjectDisposedException) { }
							onevariable_watch.Reset();
							if (j % percent == 0) System.Console.WriteLine("Producer: " + j / percent + "% progress");
							int wait = sleep - (int)sw.ElapsedMilliseconds;
							if (wait > 0) System.Threading.Thread.Sleep(wait);
						}
						if (p_mRunning)
						{
							p.Variables += increament; //Increase updated variable count
							telemetry_p.CollectVariableRunTelemetry(p.Variables);
							if (p.Variables >= max_var) { p_mRunning = false; break; }
							try { produce_outputfile.WriteLine("New Variable count: " + p.Variables + "\t"); }
							catch (System.ObjectDisposedException) { }
						}
					}
					p_done = true;
					System.Console.WriteLine("Producer iterations finished");
					telemetry_p.CollectVariableRunTelemetry(p.Variables);
					telemetry_p.mRunning = false;
					produce_outputfile.Flush();
					produce_outputfile.Close();
					produce_outputfile.Dispose();
					System.Console.WriteLine("Producer start fill");
					while (p_mRunning)
					{
						sw.Restart();
						adapter.PrepareWriteData(p.Variables);
						adapter.Send(ref onevariable_watch);
						sw.Stop();
						int wait = sleep - (int)(sw.ElapsedMilliseconds);
						if (wait > 0) System.Threading.Thread.Sleep(wait);
					}
					System.Console.WriteLine("Producer fill finished");
					break;
				}
				System.Console.WriteLine("Producer ending");

			}
		}
		public void Consume(object arg)
		{
			Params p = (Params)arg;
			using (consume_outputfile = new System.IO.StreamWriter(p.Path + "\\consume.txt"))
			{
				consume_outputfile.WriteLine("Database: " + Program.Database + "\t Variables: " + p.Variables + "\t Iterations: " + p.Iterations + "\t Timespan: " + p.timeSpan + "\t WriteFrequency: " + p.writefrequency + "\t InitIterations: " + p.inititerations);
				consume_outputfile.WriteLine("Variables\tDuration(ms)\tReadCount");

				int variables = p.Variables;
				TimeSpan variabletimespanTS = p.timeSpan;
				telemetry_c = new Telemetry(Program.VmName, p.Path, variables, adapter);
				telemetry_c.SetID("consume");
				telemetry_c.InitTelemetryCollection("consume");
				bool ex = false;
				while (c_mRunning)
				{
					System.Diagnostics.Stopwatch sww = new System.Diagnostics.Stopwatch();
					for(int ii = 0; ii < p.inititerations; ii++)
					{
						if (!c_mRunning) { break; }
						sww.Restart();
						adapter.PrepareReadMultipleLatest(variables, variabletimespanTS);
						adapter.ReadStatic(variables, variabletimespanTS);
						sww.Stop();
						int wait = 1000 - (int)sww.ElapsedMilliseconds;
						if (wait > 0) System.Threading.Thread.Sleep(wait);
					}
					var percent = p.Iterations / 100;
					for (int i = 0; i < p.Iterations; i++)
					{
						if (!c_mRunning) { break; }
						adapter.PrepareReadMultipleLatest(variables, variabletimespanTS);
						var sw = System.Diagnostics.Stopwatch.StartNew();
						try
						{
							adapter.ReadStatic(variables, variabletimespanTS);
						}catch(System.NullReferenceException e) { ex = true; continue; }
						
						sw.Stop();
						if (ex) { System.Console.WriteLine("Consumer: OK"); ex = false; }
						double elap = sw.Elapsed.TotalMilliseconds;
						try
						{
							consume_outputfile.WriteLine(variables + "\t" + elap+"\t"+adapter.GetStaticReadCount());
						}
						catch (System.ObjectDisposedException) { }
						if (i % percent == 0) System.Console.WriteLine("Consumer: "+i / percent + "% progress");
						int sleep = 1000 - (int)elap;
						if(sleep>0)System.Threading.Thread.Sleep(sleep);
					}
					while(!p_done)
					{
						if (!c_mRunning) { break; }
						adapter.PrepareReadMultipleLatest(variables, variabletimespanTS);
						try
						{
							adapter.ReadStatic(variables, variabletimespanTS);
						}
						catch (System.NullReferenceException e) { ex = true; continue; }
					}

					break;
				}
				System.Console.WriteLine("Consumer ending");
				telemetry_c.CollectVariableRunTelemetry(variables);
				telemetry_c.mRunning = false;
				consume_outputfile.Flush();
				consume_outputfile.Close();
				consume_outputfile.Dispose();
			}
		}
		#endregion

		public MixTest(ConfigFile c, IAdapter a)
		{
			conf = c;
			adapter = a;
			inititerations = Int32.Parse(conf.ReadValue("MixTest", "InitIterations"));
			writefrequency = Int32.Parse(conf.ReadValue("MixTest", "WriteFrequency"));
			writevariables = Int32.Parse(conf.ReadValue("MixTest", "WriteVariables"));
			increament = Int32.Parse(conf.ReadValue("MixTest", "WriteIncreament"));
			max_var = Int32.Parse(conf.ReadValue("MixTest", "WriteMaxVar"));
			readvariables = Int32.Parse(conf.ReadValue("MixTest", "ReadVariables"));
			iterations = Int32.Parse(conf.ReadValue("MixTest", "Iterations"));
			timespan = TimeSpan.Parse(conf.ReadValue("MixTest", "TimeSpan"));
			adapter.InitWriteTest(writevariables);
			adapter.InitMixTest(writefrequency);
		}

		public void SetPath() { Path = Program.Path + "\\MixTest"; System.IO.Directory.CreateDirectory(Path); }
		public bool IsRunning() { return p_mRunning || c_mRunning ? true:false; }
		public void Run()
		{
			System.Console.WriteLine("MixTest starting");
			p_mRunning = true;
			c_mRunning = true;
			producer = new System.Threading.Thread(Produce);
			consumer = new System.Threading.Thread(Consume);
			Params p1 = new Params(Path, writevariables, iterations, timespan, inititerations, writefrequency);
			Params p2 = new Params(Path, readvariables, iterations, timespan, inititerations, writefrequency);
			var starttime = System.DateTime.UtcNow;
			producer.Start(p1);
			if(inititerations==0) System.Threading.Thread.Sleep((int)timespan.TotalMilliseconds);
			consumer.Start(p2);
			while(consumer.IsAlive) 
			{
				if (!c_mRunning) { consumer.Join(); break; }
				System.Threading.Thread.Sleep(1000); 
			}
			p_mRunning = false;
			producer.Join();
			var endtime = System.DateTime.UtcNow;
			var duration = endtime - starttime;
			System.Console.WriteLine("Test duration: " + duration.TotalMinutes + " min");
		}
		public void Stop()
		{
			c_mRunning = false;
			consumer.Join();
			System.Console.WriteLine("Consumer exited");
			p_mRunning = false;
			producer.Join();
			System.Console.WriteLine("Producer exited");
			produce_outputfile.Flush();
			produce_outputfile.Close();
			produce_outputfile.Dispose();
			consume_outputfile.Flush();
			consume_outputfile.Close();
			consume_outputfile.Dispose();
			telemetry_p.CloseTelemetryCollecting();
			telemetry_c.CloseTelemetryCollecting();
		}
	}

	class Program
	{

		//Default test configuration variables
		public static System.Collections.Generic.List<ITest> tests = new System.Collections.Generic.List<ITest>();
		public static string VmName = "";
		public static string Path = "";
		public static string Database = "";

		//Common Database apadter
		public static IAdapter adapter;
		public static void Usage()
		{
			System.Console.WriteLine("Usage:\n");
			System.Console.WriteLine("TSI_PerformanceTesting.exe config_file");
		}
		protected static void myHandler(object sender, ConsoleCancelEventArgs args)
		{
			System.Console.WriteLine("Ctrl+C received");
			foreach(var t in tests) { if(t.IsRunning())t.Stop(); }
			System.Console.WriteLine("Terminated gracefully");
		}
		static int Main(string[] args)
		{
			if(args.Length < 1)
			{
				Usage();
				return 1;
			}
			ConfigFile conf = new ConfigFile(args[0]);
			VmName = conf.ReadValue("Default", "VMName");
			Database = conf.ReadValue("Default", "Database").ToLower();
			switch (Database)
			{
				case "influxdb":
					adapter = new InfluxDB();
					break;
				case "rtdb":
					//adapter = new RTDB();
					break;
				case "vertica":
					adapter = new Vertica();
					break;
				case "timescale":
					adapter = new Timescale();
					break;
				case "cratedb":
					adapter = new CrateDB();
					break;
				default:
					System.Console.WriteLine("Not supported database type\n\nSupported types:\n\tInfluxDB\n\tRTDB\n\tVertica\n\tTimescale\n\tCrateDB");
					return 1;
			}
			adapter.SetDomainName(conf.ReadValue("Default", "DomainName"));
			string[] tt = conf.ReadValue("Default", "Tests").ToLower().Split('|');
			foreach (var test in tt) {
				switch (test.Trim())
				{
					case "write":
						WriteTest wt = new WriteTest(conf, adapter);
						tests.Add(wt);
						break;
					case "read":
						ReadTest rt = new ReadTest(conf, adapter);
						tests.Add(rt);
						break;
					case "mix":
						MixTest mt = new MixTest(conf, adapter);
						tests.Add(mt);
						break;
					case "all":
						WriteTest wt2 = new WriteTest(conf, adapter);
						ReadTest rt2 = new ReadTest(conf, adapter);
						MixTest mt2 = new MixTest(conf, adapter);
						tests.Add(wt2);
						tests.Add(rt2);
						tests.Add(mt2);
						break;
					default:
						System.Console.WriteLine("Error: Test type not recognized!\nPossible values for \'Tests\' key: Write, Read, Mix, All");
						return 1;
				}
			}
			//Create output directory
			DateTime ts = System.DateTime.Now;
			string date = ts.Year + "_"+ts.Month+"_"+ts.Day + "_" +ts.Hour+ts.Minute+ts.Second;
			System.Console.CancelKeyPress += new ConsoleCancelEventHandler(myHandler);
			Path = System.Environment.GetFolderPath(System.Environment.SpecialFolder.Personal) + "\\TSI\\Run_" + Database.Replace(" ",string.Empty) + "_" + date;
			System.IO.FileInfo fi = new System.IO.FileInfo(Path+ "\\ini.txt");
			if (!fi.Directory.Exists) { System.IO.Directory.CreateDirectory(fi.DirectoryName); }
			System.Console.WriteLine("\nTarget database: "+Database);
			System.Console.WriteLine("Virtual Machine: " + VmName);
			System.Console.WriteLine("\nRunning tests:");
			foreach(var t in tests) { System.Console.WriteLine("\t" + t.GetType().ToString().Split('.')[1]); }
			System.Console.WriteLine("");
			foreach (var t in tests) { t.SetPath(); t.Run(); }
			System.Console.WriteLine("\nTestbench run completed!\n");
			return 0;

		}
	}
}
