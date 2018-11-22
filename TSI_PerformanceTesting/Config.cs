using System;
using System.Text;
using System.Runtime.InteropServices;

namespace Config
{
	public class ConfigFile
	{
		public static string Path;

		[DllImport("kernel32")]
		private static extern long WritePrivateProfileString(string section,
			 string key, string val, string filePath);
		[DllImport("kernel32")]
		private static extern int GetPrivateProfileString(string section,
					string key, string def, StringBuilder retVal,
			 int size, string filePath);

		public ConfigFile(string path)
		{
			Path = path;
			Console.WriteLine(System.IO.File.Exists(Path) ? "Configuration file exists." : "\nERROR:\tConfiguration file does not exist.\n");
		}
		public string ReadValue(string Section, string Key)
		{
			StringBuilder temp = new StringBuilder(255);
			int i = GetPrivateProfileString(Section, Key, "", temp,
													  255, Path);
			var t = temp.ToString();
			char[] c = { ';'};
			var tt = t.Split(c)[0].Trim('\"').TrimEnd('\t').Trim().Trim('\"');
			return tt;

		}
	}
}
