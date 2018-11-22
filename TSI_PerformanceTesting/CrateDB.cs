using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Npgsql;
using System.Net.Http;

namespace TSI_PerformanceTesting
{
	class CrateDB : IAdapter
	{
		public string Database = "CrateDB";
		private string DomainName = "";
		private static NpgsqlConnection client;
		private static NpgsqlConnection client2;
		private int variables = 0;
		private int max_var = 0;
		private int frequency = 0;
		private int staticreadcount = 0;
		private static DateTime ts_max;
		private StringBuilder w_content = new StringBuilder();
		private StringBuilder r_content = new StringBuilder();
		System.Globalization.DateTimeFormatInfo dtfi;
		private string Now;
		private string StartTime;
		private DateTime StartTimeD;
		private NpgsqlCommand checkcom = null;
		private NpgsqlCommand sendcommand;
		private NpgsqlCommand readonecom;
		private NpgsqlCommand readmulcom;

		private static HttpClientHandler handler = new HttpClientHandler();
		private static HttpClientHandler handler2 = new HttpClientHandler();
		private static HttpClient hclient = new HttpClient(handler);
		private static HttpClient hclient2 = new HttpClient(handler2);
		private static System.Threading.CancellationTokenSource cs = new System.Threading.CancellationTokenSource();

		public CrateDB()
		{
			System.Globalization.CultureInfo culture = System.Globalization.CultureInfo.CreateSpecificCulture("en-US");
			dtfi = culture.DateTimeFormat;
		}
		public void SetDomainName(string domain)
		{
			DomainName = domain;
			NpgsqlDatabaseInfo.RegisterFactory(new Npgsql.CrateDb.CrateDbDatabaseInfoFactory());
			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Database = "doc";
			builder.Username = "admin";
			builder.Password = "admin";
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
			variables = v;
			max_var = v;
			w_content.Clear();
			System.Random r = new Random();
			w_content.Append("{\"stmt\": \"INSERT INTO data VALUES (?, ?, ?, ?)\", \"bulk_args\": [");
			for (int i = 1; i <= variables; i++)
			{
				DateTime ts = DateTime.UtcNow;
				int upVal = r.Next(1, Int32.MaxValue);
				var id = r.Next(1, variables);
				if (i == max_var) { ts_max = ts; max_var = id; }
				w_content.Append("[\"testinstance_" + id.ToString() + "\",\"OK\",\""+ ts.ToString("yyyy-MM-ddThh:mm:ss.ffffff", dtfi)+"\","+ upVal.ToString() +"]");
				if (i != variables) w_content.Append(",");
			}
			w_content.Append("]}");

		}
		public void PrepareWriteDataWithTime(int v, DateTime ts)
		{
			variables = v;
			max_var = v;
			w_content.Clear();
			System.Random r = new Random();
			w_content.Append("{\"stmt\": \"INSERT INTO data VALUES (?, ?, ?, ?)\", \"bulk_args\": [");
			for (int i = 1; i <= variables; i++)
			{
				int upVal = r.Next(1, Int32.MaxValue);
				var id = r.Next(1, variables);
				if (i == max_var) { ts_max = ts; max_var = id; }
				w_content.Append("[\"testinstance_" + id.ToString() + "\",\"OK\",\"" + ts.ToString("yyyy-MM-ddThh:mm:ss.ffffff", dtfi) + "\"," + upVal.ToString() + "]");
				if (i != variables) w_content.Append(",");
			}
			w_content.Append("]}");
		}

		public void Send(ref System.Diagnostics.Stopwatch sw)
		{
			sw.Start();
			Task.WaitAll(mSend());
		}
		private async Task mSend()
		{

			StringContent co = new StringContent(w_content.ToString(), Encoding.UTF8, "application/json");
			var response = await hclient.PostAsync("http://" + DomainName + ":4200/_sql", co, cs.Token);
			if (!response.IsSuccessStatusCode)
			{
				System.Console.WriteLine("Data write error!" + response.StatusCode);
			}
		}

		private void mCheckInsert()
		{
			if (checkcom != null) return;
			checkcom = new NpgsqlCommand("select valuee from data where name = 'testinstance_" + max_var + "' AND ts = '" + ts_max.ToString("yyyy-MM-ddThh:mm:ss.ffffff", dtfi) + "'", client);
		}
		public bool CheckInsert()
		{
			mCheckInsert();
			try
			{
				var r = checkcom.ExecuteReader();
				if (r.HasRows) { r.Close(); return true; }
				r.Close();
			}
			catch (Exception e) { }
			return false;
		}
		public void SetStartTime(DateTime time) { StartTimeD = time; StartTime = time.ToString("yyyy-MM-ddThh:mm:ss.ffffff", dtfi); }
		public void SetNowTime(DateTime time) { Now = time.ToString("yyyy-MM-ddThh:mm:ss.ffffff", dtfi); }

		public void PrepareReadOne(TimeSpan ts)
		{
			DateTime endt = StartTimeD.Add(ts);
			readonecom = new NpgsqlCommand("select * from data where name = 'testinstance_1' AND ts >= '" + StartTime + "' AND ts <  '" +endt.ToString("yyyy-MM-ddThh:mm:ss.ffffff", dtfi) + "'" , client);
		}
		public void ReadOne(TimeSpan ts)
		{
			var r = readonecom.ExecuteReader();
			if (!r.HasRows) System.Console.WriteLine("Error: CrateDB ReadOne received empty");
			else { int i = 0; while (r.Read()) { i++; } if (i < (frequency * ts.TotalSeconds)) System.Console.WriteLine("Error: CrateDB ReadOne didn't receive all. Got only " + i); }
			r.Close();
			readonecom.Dispose();
		}
		public void PrepareReadMultiple(int v, TimeSpan ts)
		{
			StringBuilder con = new StringBuilder();
			DateTime endt = StartTimeD.Add(ts);
			con.Append("select * from data where name in (");
			for (int i = 1; i <= v; i++) { con.Append("'testinstance_" + i + "'"); if (i != v) con.Append(","); }
			con.Append(") AND ts >= '" + StartTime + "'  and ts < '" + endt.ToString("yyyy-MM-ddThh:mm:ss.ffffff", dtfi) + "'");
			readmulcom = new NpgsqlCommand(con.ToString(), client);
		}
		public string PrepareReadMultiple(int v, TimeSpan ts, StringBuilder c)
		{
			c.Append("select * from data where name in (");
			DateTime endt = StartTimeD.Add(ts);
			Random r = new Random();
			for (int i = 1; i <= v; i++) { c.Append("'testinstance_" + r.Next(1, v * 3) + "'"); if (i != v) c.Append(","); }
			c.Append(") AND ts >= '" + StartTime + "'  and ts < '" + endt.ToString("yyyy-MM-ddThh:mm:ss.ffffff", dtfi) + "'");
			return c.ToString();
		}
		public void PrepareReadMultipleLatest(int v, TimeSpan ts)
		{
			StringBuilder con = new StringBuilder();
			DateTime n = DateTime.UtcNow;
			n.Subtract(ts);
			con.Append("select * from data where name in (");
			for (int i = 1; i <= v; i++) { con.Append("'testinstance_" + i + "'"); if (i != v) con.Append(","); }
			con.Append(") and ts >= '" + n.ToString("yyyy-MM-ddThh:mm:ss.ffffff", dtfi) + "'");
			readmulcom = new NpgsqlCommand(con.ToString(), client2);
		}
		public void ReadMultiple(int v, TimeSpan ts)
		{
			var r = readmulcom.ExecuteReader();
			if (!r.HasRows) System.Console.WriteLine("Error: CrateDB ReadMultiple received empty");
			else { int i = 0; while (r.Read()) { i++; } if (i < (frequency * ts.TotalSeconds * v) - 3) System.Console.WriteLine("Error: CrateDB ReadMultiple didn't receive all. Got only " + i); }
			r.Close();
			readmulcom.Dispose();
		}
		public void ReadStatic(int v, TimeSpan ts)
		{
			var r = readmulcom.ExecuteReader();
			if (!r.HasRows) System.Console.WriteLine("Error: CrateDB ReadMultiple received empty");
			else
			{
				int i = 0;
				while (r.Read())
				{ i++; }
				staticreadcount = i;
				if (i > (ts.TotalSeconds - 60) * frequency && i < (frequency * ts.TotalSeconds * v)) System.Console.WriteLine("Error: CrateDB ReadStatic didn't receive all. Got only " + i);
			}
			r.Close();
		}
		public void ReadStaticC(int v, TimeSpan ts)
		{
			NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder();
			builder.Database = "doc";
			builder.Username = "admin";
			builder.Password = "admin";
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
					if (!r.HasRows) System.Console.WriteLine("Error: CrateDB ReadMultiple received empty");
					r.Close();
				}
				catch (System.Exception e) { continue; }
				System.Threading.Thread.Sleep(100);
			}
		}
		public bool CheckClientConnections(int n) { return true; }


	}
}
