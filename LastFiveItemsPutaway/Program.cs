using Dapper;
using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection.Emit;
using System.Text;
using System.Threading.Tasks;

namespace LastFiveItemsPutaway
{
    class Program
    {
        static void Main(string[] args)
        {
            using (IDbConnection cnn = new OracleConnection("DATA SOURCE=ashprdwmrb-scan.gspt.net:1521/ASH_MTV2WMS_RPT;USER ID=MTV2_READONLY_USER;PASSWORD=mtv2r0user"))
            {
                string query = "select d.lodnum, d.prtnum, k.alt_prtnum, d.tostol, a.first_name, a.last_name, d.usr_id, d.trndte from MTV2WMSPRD.dlytrn d, MTV2WMSPRD.adrmst a, MTV2WMSPRD.alt_prtmst k where d.usr_id = a.adrnam" +
                    " and d.frstol like 'WAV%' and d.lodnum like 'L%' and d.prtnum = k.prtnum and d.prt_client_id in ('100001', '100002') and k.alt_prt_typ = 'VPN' and d.trndte >= to_date(to_char(current_timestamp, 'yyyymmdd'), 'yyyymmdd')" +
                    " and a.adrtyp = 'USR' and d.to_arecod like 'PCKCS%' and d.oprcod <> 'PCKREP' and d.frstol like 'WAV%'";

                IEnumerable<Record> results = cnn.Query<Record>(query);
                var users = new List<string>();

                foreach(var result in results.Select(i => i.usr_id))
                {
                    if (!users.Contains(result))
                        users.Add(result);
                }

                List<Record> records = new List<Record>(); 

                foreach(var user in users)
                {
                    var userRecords = results.Where(i => i.usr_id.Equals(user)).ToList();
                    var dates = userRecords.Select(i => i.trndte).OrderBy(i => i).ToArray();

                    for(int i = dates.Length-1; i >= dates.Length - 5; i--)
                    {
                        try
                        {
                            records.Add(userRecords.Where(b => b.trndte.Equals(dates[i])).First());
                        }
                        catch { }
                    }
                }

                File.WriteAllText(@"C:\Users\nbrowning\Desktop\Last_Five_Items_Putaway.csv", ToCSV(records));
                Console.ReadLine();
            }
        }

        static string ToCSV<T>(IEnumerable<T> records)
        {
            try
            {
                var returnString = typeof(T).GetMethod("ToPropertyString").Invoke(null, null) as String;
                foreach(var record in records)
                {
                    returnString += record.ToString();
                }

                return returnString;
            }
            catch
            {
                Console.WriteLine("Fail");
                var s = typeof(T).GetProperties().Select(i => i.Name);

                var r = s.Aggregate((l, p) =>
                {
                    return l + ", " + p;
                }) + "\n";

                foreach (var record in records)
                {
                    r += record.ToString();
                }

                return r;
            }

        }

        class Record
        {
            public DateTime trndte { get; set; }
            public string lodnum { get; set; }
            public string prtnum { get; set; }
            public string alt_prtnum { get; set; }
            public string tostol { get; set; }
            public string first_name { get; set; }
            public string last_name { get; set; }
            public string usr_id { get; set; }

            public static string ToPropertyString()
            {
                var properties = typeof(Record).GetProperties().Select(i => i.Name);
                var returnString = properties.Aggregate((l, r) =>
                {
                    return l + ", " + r;
                }) + "\n";

                return returnString;
            }

            public override string ToString()
            {
                var props = this.GetType().GetProperties();
                string returnString = "";

                foreach(var prop in props)
                {
                    if (!prop.Equals(props.Last()))
                        returnString += prop.GetValue(this) + ", ";
                    else returnString += prop.GetValue(this);
                }

                returnString += "\n";

                return returnString;
            }
        }
    }
}
