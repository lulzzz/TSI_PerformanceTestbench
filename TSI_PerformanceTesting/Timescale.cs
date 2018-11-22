using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;

namespace TSI_PerformanceTesting
{
	class Timescale : IAdapter
	{
		public string Name = "TimescaleDB";
		public string DomainName = "";
		private static NpgsqlConnection client;
		private static NpgsqlConnection client2;
		private static DateTime ts_max;
		private static int variables = 0;
		private int max_var = 0;
		private StringBuilder w_content = new StringBuilder();
		private StringBuilder r_content = new StringBuilder();
		System.Globalization.DateTimeFormatInfo dtfi;
		private string Now;
		private string StartTime;
		private NpgsqlCommand checkcom = null;
		private NpgsqlCommand sendcommand;
		private NpgsqlCommand readonecom;
		private NpgsqlCommand readmulcom;
		private int frequency;
		private int staticreadcount;

		public Timescale()
		{
			System.Globalization.CultureInfo culture = System.Globalization.CultureInfo.CreateSpecificCulture("en-US");
			dtfi = culture.DateTimeFormat;
		}

		public void SetDomainName(string domain)
		{
			DomainName = domain;
			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Database = "tutorial";
			builder.Username = "rtdbadmin";
			builder.Password = "mypw";
			builder.Host = DomainName;
			builder.Pooling = true;
			client = new NpgsqlConnection(builder.ToString());
			client2 = new NpgsqlConnection(builder.ToString());
			client.Open();
			client2.Open();
		}
		public void InitWriteTest(int v) { variables = v; max_var = v; }
		public void InitReadTest(int v) { variables = v; max_var = v; }
		public void InitMixTest(int f) { frequency = f; }
		public int GetStaticReadCount() { return staticreadcount; }
		public void PrepareWriteData(int v)
		{
			if (v > variables) checkcom = null;
			client.UnprepareAll();
			variables = v;
			max_var = v;
			System.Random r = new Random();
			StringBuilder con = new StringBuilder();
			con.Append("insert into data values");
			for (int i = 1; i <= variables; i++)
			{
				DateTime ts = DateTime.UtcNow;
				int upVal = r.Next(1, 1000000);
				if (i == max_var) { ts_max = ts; }
				con.Append("('" + ts.ToString("yyyy-MM-dd hh:mm:ss.ffffff", dtfi) + "','testinstance_" + i + "','OK', " + upVal.ToString() + ")");
				if (i != variables) con.Append(",");
			}
			sendcommand = new NpgsqlCommand(con.ToString(), client);
			con = null;
			sendcommand.Prepare();
		}
		public void PrepareWriteDataWithTime(int v, DateTime ts)
		{
			if (v > variables) checkcom = null;
			client.UnprepareAll();
			variables = v;
			max_var = v;
			System.Random r = new Random();
			StringBuilder con = new StringBuilder();
			con.Append("insert into data values");
			for (int i = 1; i <= variables; i++)
			{
				int upVal = r.Next(1, 1000000);
				if (i == max_var) { ts_max = ts; }
				con.Append("('" + ts.ToString("yyyy-MM-dd hh:mm:ss.ffffff", dtfi) + "','testinstance_" + i + "','OK', " + upVal.ToString() + ")");
				if (i != variables) con.Append(",");
			}
			sendcommand = new NpgsqlCommand(con.ToString(), client);
			con = null;
			sendcommand.Prepare();
		}
		public void Send(ref System.Diagnostics.Stopwatch sw)
		{
			try
			{
				sw.Start();
				NpgsqlTransaction txn = client.BeginTransaction();
				sendcommand.Transaction = txn;
				sendcommand.ExecuteNonQuery();
				txn.Commit();

			}
			catch (Exception e) { Console.WriteLine(e.Message); }
			sendcommand.Dispose();
			sendcommand = null;

		}
		private void mCheckInsert()
		{
			if (checkcom != null) return;
			checkcom = new NpgsqlCommand("select value from data where name = 'testinstance_" + max_var + "' AND time = '" + ts_max.ToString("yyyy-MM-dd hh:mm:ss.ffffff", dtfi) + "'", client);
		}
		public bool CheckInsert()
		{
			mCheckInsert();
			var r = checkcom.ExecuteReader();
			if (r.HasRows) { r.Close(); return true; }
			r.Close();
			return false;
		}
		public void SetStartTime(DateTime time) { StartTime = time.ToString("yyyy-MM-dd hh:mm:ss.ffffff", dtfi); }
		public void SetNowTime(DateTime time) { Now = time.ToString("yyyy-MM-dd hh:mm:ss.ffffff", dtfi); }
		public void PrepareReadOne(TimeSpan ts)
		{
			client.UnprepareAll();
			readonecom = new NpgsqlCommand("select * from data where name = 'testinstance_1' AND time >= timestamp '" + StartTime + "' AND time < timestamp '" + StartTime + "' + interval '" + ts.TotalSeconds + " seconds'", client);
			readonecom.Prepare();
		}
		public void ReadOne(TimeSpan ts)
		{
			var r = readonecom.ExecuteReader();
			if (!r.HasRows) System.Console.WriteLine("Error: Timescale ReadOne received empty");
			else { int i = 0; while (r.Read()) { i++; } if (i < (frequency * ts.TotalSeconds)) System.Console.WriteLine("Error: Timescale ReadMultiple didn't receive all. Got only " + i); }
			r.Close();
			readonecom.Dispose();
		}

		public void PrepareReadMultiple(int v, TimeSpan ts)
		{
			client.UnprepareAll();
			StringBuilder con = new StringBuilder();
			con.Append("select * from data where name in (");
			for (int i = 1; i <= v; i++) { con.Append("'testinstance_" + i + "'"); if (i != v) con.Append(","); }
			con.Append(") AND time >= timestamp '" + StartTime + "'  and time < timestamp '" + StartTime + "' + interval '" + ts.TotalSeconds + "'");
			readmulcom = new NpgsqlCommand(con.ToString(), client);
			readmulcom.Prepare();
		}
		public string PrepareReadMultiple(int v, TimeSpan ts, StringBuilder c)
		{

			c.Append("select * from data where name in (");
			Random r = new Random();
			for (int i = 1; i <= v; i++) { c.Append("'testinstance_" + r.Next(1, v * 3) + "'"); if (i != v) c.Append(","); }
			c.Append(") AND time >= timestamp '" + StartTime + "'  and time < timestamp '" + StartTime + "' + interval '" + ts.TotalSeconds + "'");
			return c.ToString();
		}
		public void PrepareReadMultipleLatest(int v, TimeSpan ts)
		{
			client2.UnprepareAll();
			StringBuilder con = new StringBuilder();
			con.Append("select * from data where name in (");
			for (int i = 1; i <= v; i++) { con.Append("'testinstance_" + i + "'"); if (i != v) con.Append(","); }
			con.Append(") and time >= now() - interval '" + ts.TotalSeconds + "'");
			readmulcom = new NpgsqlCommand(con.ToString(), client2);
			readmulcom.Prepare();
		}
		public void ReadMultiple(int v, TimeSpan ts)
		{
			var r = readmulcom.ExecuteReader();
			if (!r.HasRows) System.Console.WriteLine("Error: Timescale ReadMultiple received empty");
			else { int i = 0; while (r.Read()) { i++; } if (i < (frequency * ts.TotalSeconds * v) - 3) System.Console.WriteLine("Error: Timescale ReadMultiple didn't receive all. Got only " + i); }
			r.Close();
			readmulcom.Dispose();
		}

		public void ReadStatic(int v, TimeSpan ts)
		{
			var r = readmulcom.ExecuteReader();
			if (!r.HasRows) System.Console.WriteLine("Error: Timescale ReadMultiple received empty");
			else
			{
				int i = 0;
				while (r.Read())
				{ i++; }
				staticreadcount = i;
				if (i > (ts.TotalSeconds - 60) * frequency && i < (frequency * ts.TotalSeconds * v)) System.Console.WriteLine("Error: Timescale ReadStatic didn't receive all. Got only " + i);
			}
			r.Close();
		}
		public void ReadStaticC(int v, TimeSpan ts)
		{
			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Database = "tutorial";
			builder.Username = "rtdbadmin";
			builder.Password = "mypw";
			builder.Host = DomainName;
			NpgsqlConnection con = new NpgsqlConnection(builder.ToString());
			while (true) { try { con.Open(); break; } catch (System.Exception e) { continue; } }

			StringBuilder c = new StringBuilder();
			string content = PrepareReadMultiple(v, ts, c);
			NpgsqlCommand com = new NpgsqlCommand(content, con);
			com.Prepare();
			while (ReadTest.Running)
			{
				try
				{
					var r = com.ExecuteReader();
					if (!r.HasRows) System.Console.WriteLine("Error: Timescale ReadMultiple received empty");
					r.Close();
				}
				catch (System.Exception e) { continue; }
				System.Threading.Thread.Sleep(100);
			}
		}
		public bool CheckClientConnections(int v) { return true; }
	}
}
