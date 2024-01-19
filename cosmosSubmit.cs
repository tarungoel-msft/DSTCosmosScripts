using Microsoft.Rest;
using ScopeRuntime;
using System;
using System.Data;
using System.Reflection;
using System.Security.Cryptography;
using System.Text.RegularExpressions;
using System.Threading;
using VcClient;
using static System.Net.Mime.MediaTypeNames;

namespace CosmosSamples.ExportToCSV
{
    class Program
    {

        static void Main(string[] args)
        {
            var exporter = new Exporter();
            string stream = "https://cosmos14.osdinfra.net/cosmos/spo.adhoc/local/tarungoel/dbWiseStats.ss";

            exporter.Export(stream);

        }
    }

    public class Exporter
    {
        public string Separator = ",";
        public string Terminator = "\r\n";
        public bool ExcludeHeaders;
        public int Top = -1;
        public string[] Columns;

        public string date = DateTime.Now.ToString("yyyy-dd-M--HH-mm-ss");
        public string LocalFolder = "D:/cps/cosmos_" + DateTime.Now.ToString("yyyy-dd-M--HH-mm-ss");

        public Exporter()
        {

        }

        public void Export(string stream)
        {
            System.IO.Directory.CreateDirectory(LocalFolder);
            string filename = LocalFolder + @"dbWiseCount.txt";
            var settings = new Microsoft.Cosmos.ExportClient.ScopeExportSettings();
            settings.Path = stream;

            // Authenticate using AAD
            // See AadCredentialHelper class in CosmosSamples/VcClient/VcClient.Sample for sample usage and some other ways to get AAD credentials
            settings.ServiceClientCredentials = AadCredentialHelper.GetCredentialFromPrompt();
            settings.EnableClientMode = true;

            // Authenticate using Certificate
            // See Utility class in CosmosSamples/Cosmos.VNext/SubmitScope for sample usage to retrieve certificate from local user's store
            //settings.ClientCertificate = Utility.GetCertificateFromStore();

            var exportClient = new Microsoft.Cosmos.ExportClient.ExportClient(settings);

            // Top
            if (this.Top > 0)
            {
                settings.Top = this.Top;
            }


            // Columns
            settings.ColumnFilters = this.Columns;


            // Partitions
            // if PartitionIndices is set to null, or not set, default to get all partitions
            settings.PartitionIndices = exportClient.GetAllPartitionIndices(null).Result;

            try
            {
                var task = exportClient.Export(null, new System.Threading.CancellationToken());

                task.Wait();

                using (System.Data.IDataReader dataReader = task.Result.DataReader)
                {
                    OutputToFile(dataReader, filename, this.Separator, this.Terminator, settings.ServiceClientCredentials);
                }
            }
            catch (System.AggregateException ae)
            {
                System.Console.WriteLine(ae.ToString());
            }

        }


        private void OutputToFile(System.Data.IDataReader reader, string fileName, string separator, string terminator, ServiceClientCredentials creds)
        {
            using (var s = System.IO.File.OpenWrite(fileName))
            {
                using (var writer = new System.IO.StreamWriter(s))
                {

                    var schematable = reader.GetSchemaTable();

                    var schemacol_colname = schematable.Columns["ColumnName"];
                    var schemacol_ordinal = schematable.Columns["ColumnOrdinal"];
                    var schemacol_datatypename = schematable.Columns["DataTypeName"];
                    var schemacol_providertype = schematable.Columns["ProviderType"];
                    var schemacol_allowdbnull = schematable.Columns["AllowDBNull"];

                    var column_names = new string[schematable.Rows.Count];
                    if (!this.ExcludeHeaders)
                    {
                        for (int i = 0; i < schematable.Rows.Count; i++)
                        {
                            var row = schematable.Rows[i];
                            string colname = (string)row[schemacol_colname.Ordinal];
                            string datatypename = (string)row[schemacol_datatypename.Ordinal];
                            System.Type providertype = (System.Type)row[schemacol_providertype.Ordinal];

                            column_names[i] = colname;

                            if (i > 0)
                            {
                                writer.Write(this.Separator);
                            }
                            writer.Write("{0}", colname);
                        }
                        writer.Write(this.Terminator);
                    }

                    long numRecords = 0, startRecord = 0, n = 0;
                    Console.Write("Enter start index: ");
                    startRecord = 410;
                    object[] values = new object[reader.FieldCount];
                    while (reader.Read())
                    {
                        if (numRecords >= startRecord)
                        {
                            int num_fields = reader.GetValues(values);
                            for (int i = 0; i < num_fields; i++)
                            {
                                if (i > 0)
                                {
                                    writer.Write(this.Separator);
                                }
                                writer.Write(values[i]);
                            }

                            writer.Write(numRecords);
                            Console.Write(numRecords);
                            Console.Write(this.Terminator);

                            writer.Write(this.Terminator);

                            CreateStream(values[1], creds, startRecord, numRecords);
                            n++;
                        }
                        else
                        {
                            Console.Write("Skipping record {0}", numRecords);
                        }
                        numRecords++;
                    }
                }
            }
        }

        private void CreateStream(Object dbGuid, ServiceClientCredentials creds, long startRecord, long numRec)
        {
            string script = @"
MODULE ""/shares/exchange.storage.prod/local/PrivacyAnnotation/PrivacyAnnotation.module"" AS DataMapCodeAnnotation;
USING Microsoft.DataMap.CodeAnnotation.Cosmos;

dstErrors =
    SSTREAM ""shares/spo.adhoc/local/tarungoel/errorsWithDb.ss"";

errorsForthisDb = SELECT * FROM dstErrors WHERE ContentDBId == Guid.Parse(""<DEADDEAD>"");

[Privacy.Asset.NonPersonal]
OUTPUT errorsForthisDb 
    TO SSTREAM ""shares/spo.adhoc/local/tarungoel/test<DATEDATE>_<STARTSTART>/dstErrors_<DEADDEAD>.ss""
    WITH STREAMEXPIRY ""300"";";

            string modifiedScript = Regex.Replace(script, "<DEADDEAD>", dbGuid.ToString());
            modifiedScript = Regex.Replace(modifiedScript, "<DATEDATE>", date);
            modifiedScript = Regex.Replace(modifiedScript, "<STARTSTART>", startRecord.ToString());
            string vc = "https://cosmos14.osdinfra.net/cosmos/office.adhoc/";
            string temp_folder = "D:/cps/cosmos_" + date;
            string fileName = string.Format("test_{0}_{1}.script", dbGuid.ToString(), numRec);
            string script_filename = System.IO.Path.Combine(temp_folder, fileName);
            VcClient.VC.SetupAadCredentials(vc, VcClient.VC.NoProxy, creds);
            System.IO.File.WriteAllText(script_filename, modifiedScript);

            // For VcClient that setup with AAD Credential
            // subParams.NebulaCommandLineArgs = "-on useaadauthentication -u tarungoel_debug@prdtrs01.prod.outlook.com";

            ScopeClient.ScopeEnvironment.Instance.WorkingRoot = temp_folder;

            ScopeClient.SubmitParameters subParams = new ScopeClient.SubmitParameters(script_filename)
            {
                NebulaCommandLineArgs = "-on useaadauthentication -u tarungoel_debug@prdtrs01.prod.outlook.com",
            };

            ScopeClient.Scope.VCSettings vcSettings = new ScopeClient.Scope.VCSettings();
            JobInfo jobinfo = ScopeClient.Scope.Submit(vc, null, null, subParams);
            //WaitUntilJobFinished(jobinfo);

            System.Console.WriteLine("Completed job\n");
        }

        private static void WaitUntilJobFinished(JobInfo jobinfo)
        {
            // The submission is done. Now we wait until the job is done
            bool use_compression = true;
            int seconds_to_sleep = 5;
            var wait_time = new System.TimeSpan(0, 0, 0, seconds_to_sleep);
            while (true)
            {
                jobinfo = VcClient.VC.GetJobInfo(jobinfo.ID, use_compression);
                Console.WriteLine("Job State = {0}", jobinfo.State);
                if (jobinfo.State == VcClient.JobInfo.JobState.Cancelled || jobinfo.State == VcClient.JobInfo.JobState.Completed
                    || jobinfo.State == VcClient.JobInfo.JobState.CompletedFailure
                    || jobinfo.State == VcClient.JobInfo.JobState.CompletedSuccess)
                {
                    Console.WriteLine("Job Stopped Running");
                    break;
                }

                System.Threading.Thread.Sleep(wait_time);
            }
        }
    }
}
