using Microsoft.Data.OData;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.Protocol;
using System;

namespace TestStorageSASUrl
{
    class Program
    {
        private static string tableName;

        static void Main(string[] args)
        {
            var p = new Program();
            p.TestTableCreation();
        }

        void TestTableCreation()
        {
            string sasUrl = @"YOUR SAS URL HERE!";

            CloudTableClient tc = GetCloudTableClient(sasUrl);

            CloudTable cloudTable = tc.GetTableReference(tableName);


            try
            {
                Console.WriteLine("Exists: " + cloudTable.Exists());
                
                cloudTable.CreateIfNotExists();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }

        private CloudTableClient GetCloudTableClient(string sasUrl)
        {
            int parseIndex = sasUrl.IndexOf('?');
            if (parseIndex > 0)
            {
                string tableAddress = sasUrl.Substring(0, parseIndex);

                int tableParseIndex = tableAddress.LastIndexOf('/');
                if (tableParseIndex > 0)
                {
                    tableName = tableAddress.Substring(tableParseIndex + 1);

                    string endpointAddress = tableAddress.Substring(0, tableParseIndex);
                    string sasSignature = sasUrl.Substring(parseIndex);

                    var tableClient = new CloudTableClient(new Uri(endpointAddress), new StorageCredentials(sasSignature));

                    // This is a hack for the Azure Storage SDK to make it work with version 2012 SAS urls as long as we support them.
                    // Apply hack only if the SAS url is version 2012.
                    if (sasSignature.IndexOf("sv=2012-02-12", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        tableClient.DefaultRequestOptions.PayloadFormat = TablePayloadFormat.AtomPub;
                        var type = typeof(TableConstants);
                        var field = type.GetField("ODataProtocolVersion", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
                        field.SetValue(null, ODataVersion.V2);
                    }

                    return tableClient;
                }
            }

            return null;
        }
    }
}
