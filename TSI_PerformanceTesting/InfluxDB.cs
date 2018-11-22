using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net.Http;

namespace TSI_PerformanceTesting
{
	class InfluxDB : IAdapter
	{
		public string Name = "InfluxDB";
		public string DomainName = "";
		private static HttpClientHandler handler = new HttpClientHandler();
		private static HttpClientHandler handler2 = new HttpClientHandler();
		private static HttpClient client = new HttpClient(handler);
		private static HttpClient client2 = new HttpClient(handler2);
		private static System.Threading.CancellationTokenSource cs = new System.Threading.CancellationTokenSource();
		private static long ts_max = 0;
		private static int variables = 0;
		private int max_var = 0;
		private StringBuilder w_content = new StringBuilder();
		private StringBuilder r_content = new StringBuilder();
		private string tss = "";
		private long Now;
		private long StartTime;
		private int frequency;
		public int staticreadcount;

		public InfluxDB()
		{
			System.Net.ServicePointManager.SecurityProtocol = System.Net.SecurityProtocolType.Tls12 | System.Net.SecurityProtocolType.Tls11 | System.Net.SecurityProtocolType.Tls;
			System.Net.ServicePointManager.ServerCertificateValidationCallback += new System.Net.Security.RemoteCertificateValidationCallback(myValidator);
			client.Timeout = System.Threading.Timeout.InfiniteTimeSpan;
			client2.Timeout = System.Threading.Timeout.InfiniteTimeSpan;
		}
		private static bool myValidator(object sender, System.Security.Cryptography.X509Certificates.X509Certificate cert, System.Security.Cryptography.X509Certificates.X509Chain chain, System.Net.Security.SslPolicyErrors sslErrors)
		{
			if (sslErrors == System.Net.Security.SslPolicyErrors.RemoteCertificateChainErrors)
			{
				return true;
			}
			return false;
		}
		public void InitWriteTest(int v) { variables = v; max_var = variables; }
		public void InitReadTest(int v) { variables = v; max_var = v; frequency = ReadTest.Frequency; }
		public void InitMixTest(int f) { frequency = f; }
		public void SetDomainName(string domain) { DomainName = domain; }
		public int GetStaticReadCount() { return staticreadcount; }

		private async Task mSend()
		{

			StringContent co = new StringContent(w_content.ToString(), Encoding.UTF8);
			var response = await client.PostAsync("https://" + DomainName + ":8086/write?db=mydb&u=admin2&p=admin2&precision=u", co, cs.Token);
			if (!response.IsSuccessStatusCode) System.Console.WriteLine("Data write error!");
		}

		private bool mCheckInsert()
		{
			byte[] r = client.GetByteArrayAsync("https://" + DomainName + ":8086/query?u=admin&p=admin&db=mydb&q=SELECT%20%2A%20FROM%20data%20WHERE%20%22name%22%3D'testinstance_" + max_var.ToString() + "'%20AND%20time=" + ts_max.ToString() + "%20").Result;
			if (r.Length > 167) return true;
			else return false;
		}
		public void PrepareWriteData(int v)
		{
			variables = v;
			max_var = v;
			w_content.Clear();
			System.Random r = new Random();
			for (int i = 1; i <= variables; i++)
			{
				long ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000;
				int upVal = r.Next(1, Int32.MaxValue);
				var id = r.Next(1, variables);
				if (i == max_var) { ts_max = ts * 1000; max_var = id; }
				w_content.Append("data,name=testinstance_" + id.ToString() + ",status=ok value=" + upVal.ToString() + " " + ts.ToString() + "\n");
			}
		}
		public void PrepareWriteDataWithTime(int v, DateTime time)
		{
			variables = v;
			max_var = v;
			w_content.Clear();
			System.Random r = new Random();
			long ts = (long)time.Subtract(new DateTime(1970, 1, 1)).TotalMilliseconds * 1000;
			for (int i = 1; i <= variables; i++)
			{
				int upVal = r.Next(1, 1000000);
				if (i == max_var) ts_max = ts * 1000;
				w_content.Append("data,name=testinstance_" + i.ToString() + ",status=ok value=" + upVal.ToString() + " " + ts.ToString() + " \n");
			}
		}
		public void Send(ref System.Diagnostics.Stopwatch sw)
		{
			sw.Start();
			Task.WaitAll(mSend());
		}

		public bool CheckInsert()
		{
			return ((bool)mCheckInsert());
		}
		public void SetStartTime(DateTime time) { StartTime = (long)time.Subtract(new DateTime(1970, 1, 1)).TotalMilliseconds * 1000 * 1000; }
		public void SetNowTime(DateTime time) { Now = (long)time.Subtract(new DateTime(1970, 1, 1)).TotalMilliseconds * 1000; }
		public void PrepareReadOne(TimeSpan ts)
		{
			tss = "";
			if (ts.Days > 0) tss = ts.Days + "d";
			if (ts.Hours > 0) tss += ts.Hours + "h";
			if (ts.Minutes > 0) tss += ts.Minutes + "m";
			if (ts.Seconds > 0) tss += ts.Seconds + "s";
			if (ts.Milliseconds > 0) tss += ts.Milliseconds + "ms";
		}
		public void ReadOne(TimeSpan ts)
		{
			try
			{
				byte[] r = client.GetByteArrayAsync("https://" + DomainName + ":8086/query?u=admin&p=admin&db=mydb&q=SELECT%20%2A%20FROM%20data%20WHERE%20%22name%22%3D'testinstance_1'%20AND%20time >= " + StartTime + " and time < " + StartTime + " %2B " + tss + "%20").Result;
				if (r.Length <= 77) System.Console.WriteLine("Error: InluxDB ReadOne received empty");
				//var str = System.Text.Encoding.Default.GetString(r);
				//if (System.Text.RegularExpressions.Regex.Matches(str, "testinstance_1").Count != (ts.TotalSeconds * ReadTest.Frequency)) System.Console.WriteLine("Error: InfluxDB ReadOne didn't receive all. Got only " + System.Text.RegularExpressions.Regex.Matches(str, "testinstance_1").Count);
			}
			catch (SystemException e) { System.Console.WriteLine("Error while reading InfluxDB ReadOne: " + e.Message); throw e; }
		}

		public void PrepareReadMultiple(int v, TimeSpan ts)
		{
			tss = "";
			if (ts.Days > 0) tss = ts.Days + "d";
			if (ts.Hours > 0) tss += ts.Hours + "h";
			if (ts.Minutes > 0) tss += ts.Minutes + "m";
			if (ts.Seconds > 0) tss += ts.Seconds + "s";
			if (ts.Milliseconds > 0) tss += ts.Milliseconds + "ms";
			r_content.Clear();
			r_content.Append("SELECT * FROM data WHERE ");
			for (int i = 1; i <= v; i++)
			{
				r_content.Append("\"name\"='testinstance_" + i + "'");
				if (i <= (v - 1)) r_content.Append(" OR ");
			}
			r_content.Append(" AND time >= " + StartTime + " AND time < " + StartTime + " + " + tss);
		}
		public void PrepareReadMultipleLatest(int v, TimeSpan ts)
		{
			tss = "";
			if (ts.Days > 0) tss = ts.Days + "d";
			if (ts.Hours > 0) tss += ts.Hours + "h";
			if (ts.Minutes > 0) tss += ts.Minutes + "m";
			if (ts.Seconds > 0) tss += ts.Seconds + "s";
			if (ts.Milliseconds > 0) tss += ts.Milliseconds + "ms";
			r_content.Clear();
			r_content.Append("SELECT * FROM data WHERE ");
			for (int i = 1; i <= v; i++)
			{
				r_content.Append("\"name\"='testinstance_" + i + "'");
				if (i <= (v - 1)) r_content.Append(" OR ");
			}
			r_content.Append(" AND time > now() - " + tss);
		}
		public StringBuilder PrepareReadMultiple(int v, TimeSpan ts, StringBuilder c)
		{
			string tsss = "";
			if (ts.Days > 0) tsss = ts.Days + "d";
			if (ts.Hours > 0) tsss += ts.Hours + "h";
			if (ts.Minutes > 0) tsss += ts.Minutes + "m";
			if (ts.Seconds > 0) tsss += ts.Seconds + "s";
			if (ts.Milliseconds > 0) tsss += ts.Milliseconds + "ms";
			c.Clear();
			c.Append("SELECT * FROM data WHERE ");

			int[] items = new int[v];
			Random r = new Random();
			for (int j = 0; j < v; j++)
			{
				items[j] = r.Next(1, max_var);
			}
			for (int i = 0; i < v; i++)
			{
				c.Append("\"name\"='testinstance_" + items[i] + "'");
				if (i < (v - 1)) c.Append(" OR ");
			}
			c.Append(" AND time > " + Now + " - " + tss);
			return c;
		}
		public void ReadMultiple(int v, TimeSpan ts)
		{
			try
			{
				var content = new StringContent("db=mydb&q=" + System.Web.HttpUtility.UrlEncode(r_content.ToString()), Encoding.UTF8, "application/x-www-form-urlencoded");
				byte[] r = client.PostAsync("https://" + DomainName + ":8086/query?u=admin&p=admin", content).Result.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult();
				if (r.Length <= 77) { System.Console.WriteLine("Error: InluxDB ReadMultiple received empty"); throw new System.NullReferenceException("InluxDB ReadMultiple received empty"); }
				//var str = System.Text.Encoding.Default.GetString(r);
				//if (System.Text.RegularExpressions.Regex.Matches(str, "testinstance_"+v).Count != (ts.TotalSeconds * ReadTest.Frequency)) System.Console.WriteLine("Error: InfluxDB ReadMultiple didn't receive all. Got only " + System.Text.RegularExpressions.Regex.Matches(str, "testinstance_"+v).Count);
			}
			catch (SystemException e) { System.Console.WriteLine("Error while reading InfluxDB ReadMultiple: " + e.Message); throw e; }
		}

		public void ReadStatic(int v, TimeSpan ts)
		{
			try
			{
				var content = new StringContent("db=mydb&q=" + System.Web.HttpUtility.UrlEncode(r_content.ToString()), Encoding.UTF8, "application/x-www-form-urlencoded");
				//r_content.Clear();
				byte[] r = client2.PostAsync("https://" + DomainName + ":8086/query?u=admin&p=admin", content, cs.Token).Result.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult();
				if (r.Length <= 77) System.Console.WriteLine("Error: InluxDB ReadStatic received empty");
				var str = System.Text.Encoding.Default.GetString(r);
				staticreadcount = System.Text.RegularExpressions.Regex.Matches(str, "testinstance_" + v).Count;
				if (((ts.TotalSeconds - 60) * frequency) < staticreadcount && staticreadcount < (ts.TotalSeconds * frequency)) System.Console.WriteLine("Error: InfluxDB ReadStatic didn't receive all. Got only " + staticreadcount);
			}
			catch (SystemException e) { System.Console.WriteLine("Error while reading InfluxDB ReadStatic: " + e.Message); throw e; }
		}

		public void ReadStaticC(int v, TimeSpan ts)
		{
			HttpClient clientC = new HttpClient();
			StringBuilder content = new StringBuilder();
			content = PrepareReadMultiple(v, ts, content);
			while (ReadTest.Running)
			{
				try
				{
					var c = new StringContent("db=mydb&q=" + System.Web.HttpUtility.UrlEncode(content.ToString()), Encoding.UTF8, "application/x-www-form-urlencoded");
					byte[] r = client.PostAsync("https://" + DomainName + ":8086/query?u=admin&p=admin", c).Result.Content.ReadAsByteArrayAsync().GetAwaiter().GetResult();
					if (r.Length <= 77) { System.Console.WriteLine("Error: InluxDB ReadStaticC received empty"); System.Console.WriteLine(System.Text.Encoding.Default.GetString(r)); }
				}
				catch (SystemException e) { System.Console.WriteLine("Error while reading InfluxDB ReadStaticC: " + e.Message); throw e; }
				System.Threading.Thread.Sleep(100);
			}
		}
		public bool CheckClientConnections(int v) { return true; }

	}
}
