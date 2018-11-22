using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Vertica.Data.VerticaClient;

namespace TSI_PerformanceTesting
{
	class Vertica : IAdapter
	{
		public string Name = "Vertica";
		public string DomainName = "mikael-virtual-machine-vertica";
		private static VerticaConnection client;
		private static DateTime ts_max;
		private static int variables = 0;
		private int max_var = 0;
		private StringBuilder w_content = new StringBuilder();
		private StringBuilder r_content = new StringBuilder();
		System.Globalization.DateTimeFormatInfo dtfi;
		private string Now;
		private System.Collections.Generic.List<VerticaCommand> coms = new System.Collections.Generic.List<VerticaCommand>();
		private VerticaCommand checkcom = null;
		private VerticaCommand readonecom;
		private VerticaCommand readmulcom;
		private int frequency;
		private int staticreadcount;


		public Vertica()
		{
			System.Globalization.CultureInfo culture = System.Globalization.CultureInfo.CreateSpecificCulture("en-US");
			dtfi = culture.DateTimeFormat;
		}
		public void SetDomainName(string domain)
		{
			DomainName = domain;
			VerticaConnectionStringBuilder builder = new VerticaConnectionStringBuilder();
			builder.Database = "mydb";
			builder.User = "testuser";
			builder.Password = "password";
			builder.Host = "10.58.44.163";
			builder.Pooling = true;
			client = new VerticaConnection(builder.ToString());
			client.Open();
		}
		public int GetStaticReadCount() { return staticreadcount; }
		public void InitWriteTest(int v) { variables = v; max_var = v; }
		public void InitReadTest(int v) { variables = v; max_var = v; }
		public void InitMixTest(int f) { frequency = f; }
		public void PrepareWriteData(int v)
		{
			if (v > variables) checkcom = null;
			variables = v;
			coms.Clear();
			System.Random r = new Random();
			StringBuilder con = new StringBuilder();
			con.Append("insert /*+ AUTO */ into mydata\n\t");
			VerticaCommand command;
			for (int i = 1; i <= variables; i++)
			{
				if (i >= 1000 && (i % 1000 == 0))
				{
					command = client.CreateCommand();
					command.CommandText = con.ToString();
					coms.Add(command);
					con.Clear();
					con.Append("insert /*+ AUTO */ into mydata\n\t");
				}
				DateTime ts = DateTime.UtcNow;
				int upVal = r.Next(1, 1000000);
				if (i == max_var) { ts_max = ts; }
				con.Append(" select 'testinstance_" + i + "','OK', TIMESTAMP '" + ts.ToString("yyyy-MM-dd hh:mm:ss.ffffff", dtfi) + "', " + upVal.ToString());
				if (i != variables && ((i + 1) % 1000 != 0)) con.Append(" UNION ");
			}
			command = client.CreateCommand();
			command.CommandText = con.ToString();
			command.Prepare();
			coms.Add(command);
		}
		public void PrepareWriteDataWithTime(int v, DateTime ts)
		{
			if (v > variables) checkcom = null;
			variables = v;
			coms.Clear();
			System.Random r = new Random();
			StringBuilder con = new StringBuilder();
			con.Append("insert /*+ AUTO */ into mydata\n\t");
			VerticaCommand command;
			for (int i = 1; i <= variables; i++)
			{
				if (i >= 1000 && (i % 1000 == 0))
				{
					command = client.CreateCommand();
					command.CommandText = con.ToString();
					coms.Add(command);
					con.Clear();
					con.Append("insert into mydata\n\t");
				}
				int upVal = r.Next(1, 1000000);
				if (i == max_var) { ts_max = ts; }
				con.Append(" select " + i + ",'OK', TIMESTAMP '" + ts.ToString("yyyy-MM-dd hh:mm:ss.ffffff", dtfi) + "', " + upVal.ToString());
				if (i != variables && ((i + 1) % 1000 != 0)) con.Append(" UNION ");
			}
			command = client.CreateCommand();
			command.CommandText = con.ToString();
			command.Prepare();
			coms.Add(command);
		}
		public void Send(ref System.Diagnostics.Stopwatch sw)
		{
			try
			{
				sw.Start();
				VerticaTransaction txn = client.BeginTransaction();
				foreach (VerticaCommand c in coms)
				{
					c.Transaction = txn;
					c.ExecuteNonQueryAsync();
				}
				txn.Commit();

			}
			catch (Exception e) { Console.WriteLine(e.Message); }
		}
		private void mCheckInsert()
		{
			if (checkcom != null) return;
			checkcom = new VerticaCommand("select value from mydata where variable_id = 'testinstance_" + max_var + "' AND timestamp = '" + ts_max.ToString("yyyy-MM-dd hh:mm:ss.ffffff", dtfi) + "'");
			checkcom.Connection = client;
		}
		public bool CheckInsert()
		{
			mCheckInsert();
			VerticaDataReader r = checkcom.ExecuteReader();
			if (r.HasRows) { r.Close(); return true; }
			r.Close();
			return false;
		}
		public void SetStartTime(DateTime time) { }
		public void SetNowTime(DateTime time) { Now = time.ToString("yyyy-MM-dd hh:mm:ss.ffffff", dtfi); }
		public void PrepareReadOne(TimeSpan ts)
		{
			readonecom = new VerticaCommand("select * from mydata where variable_id = 'testinstance_1' and timestamp >= timestamp '" + Now + "' - interval '" + ts.TotalSeconds + "'");
			readonecom.Connection = client;
		}
		public void ReadOne(TimeSpan ts)
		{
			VerticaDataReader r = readonecom.ExecuteReader();
			if (!r.HasRows) System.Console.WriteLine("Error: Vertica ReadOne received empty");
			r.Close();
		}
		public void PrepareReadMultiple(int v, TimeSpan ts)
		{
			StringBuilder con = new StringBuilder();
			con.Append("select * from mydata where variable_id in (");
			for (int i = 1; i <= v; i++) { con.Append("'testinstance_" + i + "'"); if (i != v) con.Append(","); }
			con.Append(") and timestamp >= timestamp '" + Now + "' - interval '" + ts.TotalSeconds + "'");
			readmulcom = new VerticaCommand(con.ToString());
			readmulcom.Connection = client;
		}
		public string PrepareReadMultiple(int v, TimeSpan ts, StringBuilder c)
		{

			c.Append("select * from mydata where variable_id in (");
			Random r = new Random();
			for (int i = 1; i <= v; i++) { c.Append("'testinstance_" + r.Next(1, v * 3) + "'"); if (i != v) c.Append(","); }
			c.Append(") and timestamp >= timestamp '" + Now + "' - interval '" + ts.TotalSeconds + "'");
			return c.ToString();
		}
		public void PrepareReadMultipleLatest(int v, TimeSpan ts)
		{
			StringBuilder con = new StringBuilder();
			con.Append("select * from mydata where variable_id in (");
			for (int i = 1; i <= v; i++) { con.Append("'testinstance_" + i + "'"); if (i != v) con.Append(","); }
			con.Append(") and timestamp >= now() - interval '" + ts.TotalSeconds + "'");
			readmulcom = new VerticaCommand(con.ToString());
			readmulcom.Connection = client;
		}
		public void ReadMultiple(int v, TimeSpan ts)
		{
			VerticaDataReader r = readmulcom.ExecuteReader();
			if (!r.HasRows) System.Console.WriteLine("Error: Vertica ReadMultiple received empty");
			r.Close();
		}
		public void ReadStatic(int v, TimeSpan ts)
		{
			VerticaDataReader r = readmulcom.ExecuteReader();
			if (!r.HasRows) System.Console.WriteLine("Error: Vertica ReadMultiple received empty");
			r.Close();
		}
		public void ReadStaticC(int v, TimeSpan ts)
		{
			VerticaConnectionStringBuilder builder = new VerticaConnectionStringBuilder();
			builder.Database = "mydb";
			builder.User = "testuser";
			builder.Password = "password";
			builder.Host = "10.58.44.163";
			VerticaConnection con = new VerticaConnection(builder.ToString());
			con.Open();
			StringBuilder c = new StringBuilder();
			string content = PrepareReadMultiple(v, ts, c);
			VerticaCommand com = new VerticaCommand(content);
			com.Connection = con;
			while (ReadTest.Running)
			{
				VerticaDataReader r = com.ExecuteReader();
				if (!r.HasRows) System.Console.WriteLine("Error: Vertica ReadMultiple received empty");
				r.Close();
				System.Threading.Thread.Sleep(100);
			}
		}
		public bool CheckClientConnections(int v) { return true; }
	}
}
