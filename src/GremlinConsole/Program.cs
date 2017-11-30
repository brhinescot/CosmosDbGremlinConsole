#region Using Directives

using System;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Graphs;
using Newtonsoft.Json;

using static System.Console;

#endregion

namespace GremlinConsole
{
    /// <summary>
    ///     Sample program that shows how to get started with the Graph (Gremlin) APIs for Azure Cosmos DB.
    /// </summary>
    public class Program
    {
        private readonly JsonSerializerSettings settings = new JsonSerializerSettings
        {
            DateFormatHandling = DateFormatHandling.IsoDateFormat,
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            DateParseHandling = DateParseHandling.DateTime,
        };

        private string partition;

        /// <summary>
        ///     Runs some Gremlin commands on the console.
        /// </summary>
        /// <param name="args">command-line arguments</param>
        public static async Task Main(string[] args)
        {
            Title = "CosmosDB Gremlin Development Console";
            
            Program p = new Program();
            await p.RunAsync();
        }

        /// <summary>
        ///     Run the get started application.
        /// </summary>
        /// <returns>A Task for asynchronous execution.</returns>
        private async Task RunAsync()
        {
            var inputBuffer = new byte[2048];
            Stream inputStream = OpenStandardInput(inputBuffer.Length);
            SetIn(new StreamReader(inputStream, InputEncoding, false, inputBuffer.Length));

            string endpoint = ConfigurationManager.AppSettings["Endpoint"];
            string authKey = ConfigurationManager.AppSettings["AuthKey"];
            string databaseId = ConfigurationManager.AppSettings["Database"];
            string collectionId = ConfigurationManager.AppSettings["Collection"];
            string partitionKey = ConfigurationManager.AppSettings["PartitionKey"];

            using (DocumentClient client = new DocumentClient(new Uri(endpoint), authKey))
            {
                bool retry = true;
                while (retry)
                {
                    try
                    {
                        await client.CreateDatabaseIfNotExistsAsync(new Database {Id = databaseId});
                        DocumentCollection collection = new DocumentCollection {Id = collectionId};
                        collection.PartitionKey.Paths.Add(partitionKey);

                        DocumentCollection graph = await client.CreateDocumentCollectionIfNotExistsAsync(
                            UriFactory.CreateDatabaseUri(databaseId),
                            collection);

                        //Fetch the resource to be updated
                        Offer offer = client.CreateOfferQuery()
                            .Where(r => r.ResourceLink == graph.SelfLink)
                            .AsEnumerable()
                            .SingleOrDefault();

                        // Set the throughput to the new value, for example 12,000 request units per second
                        offer = new OfferV2(offer, 10000);

                        //Now persist these changes to the database by replacing the original resource
                        await client.ReplaceOfferAsync(offer);

                        Clear();
                        WriteLine($"Connected to database {databaseId} and collection {graph.Id} at {client.ReadEndpoint}");

                        await DoStuff(databaseId, client, graph);
                    }
                    catch (System.Net.Http.HttpRequestException ex)
                    {

                        Clear();
                        ForegroundColor = ConsoleColor.Red;
                        WriteLine($"Error connecting to database {databaseId} at {client.ReadEndpoint}. {ex.InnerException?.Message}");
                        ResetColor();

                        WriteLine("Press Enter to retry, any other key exit.");
                        ConsoleKeyInfo info = ReadKey(true);
                        retry = info.Key == ConsoleKey.Enter;
                        if (!retry)
                            continue;

                        Clear();
                        WriteLine("Retrying connection ...");
                    }
                    catch (Exception ex)
                    {
                        WriteLine();
                        ForegroundColor = ConsoleColor.Red;
                        WriteLine(ex.Message);
                        ResetColor();
                        
                        WriteLine("Press Enter to retry, any other key exit.");
                        ConsoleKeyInfo info = ReadKey(true);
                        retry = info.Key == ConsoleKey.Enter;
                        if (!retry)
                            continue;

                        Clear();
                        WriteLine("Retrying connection ...");
                    }
                }
            }
        }

        private async Task DoStuff(string database, DocumentClient client, DocumentCollection graph)
        {
            WriteLine();
            WriteGremlinPrompt();

            string userInput;
            while ((userInput = ReadLongLine()) != null)
            {
                var strings = userInput.Split(' ');
                switch (strings[0])
                {
                    case "cls":
                        Clear();
                        ResetColor();
                        WriteLine($"Connected to database {database} and collection {graph.Id} at {client.ReadEndpoint}");
                        WriteLine();
                        WriteGremlinPrompt();
                        continue;
                    case "exit":
                        Environment.Exit(0);
                        break;
                    case "run-script":
                        string scriptPath = strings[1].Trim('"');
                        await RunScript(client, graph, scriptPath);
                        break;
                    case "set-partition":
                        partition = strings[1];
                        ForegroundColor = ConsoleColor.Green;
                        WriteLine($"==> Partition is now {partition}");
                        WriteGremlinPrompt();
                        break;
                    case "connect":
                        Clear();
                        client.Dispose();
                        (client, graph) = await Connect(); 
                        WriteLine();
                        WriteGremlinPrompt();
                        break;
                    default:
                        await RunUserQuery(client, graph, userInput);
                        WriteGremlinPrompt();
                        break;
                }
            }
        }

        private async Task RunScript(DocumentClient client, DocumentCollection graph, string scriptPath)
        {
            if (string.IsNullOrWhiteSpace(scriptPath))
                throw new ArgumentNullException(nameof(scriptPath));

            WriteGremlinPrompt();

            using (FileStream stream = File.OpenRead(scriptPath))
            using (StreamReader reader = new StreamReader(stream))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if(string.IsNullOrWhiteSpace(line) || line.StartsWith("//"))
                        continue;

                    WriteLine(line);
                    await RunUserQuery(client, graph, line);
                    WriteGremlinPrompt();
                }
            }
        }

        private static async Task<(DocumentClient client, DocumentCollection graph)> Connect()
        {
            ResetColor();
            WriteLine("Connect to database");
            WriteLine("************************************************************\r\n");

            DocumentClient client;
            DocumentCollection graph;

            while (true)
            {
                try
                {
                    Write("Endpoint (leave blank for local emulator): ");
                    string endpoint = ReadLine();
                    string authKey;
                    if (!string.IsNullOrEmpty(endpoint))
                    {
                        Write("Auth Key");
                        authKey = ReadLine();
                    }
                    else
                    {
                        endpoint = "https://localhost:8081/";
                        authKey = "C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==";
                    }

                    Write("Database: ");
                    string database = ReadLine();
                    Write("Collection: ");
                    string collection = ReadLine();

                    client = new DocumentClient(new Uri(endpoint), authKey, new ConnectionPolicy {EnableEndpointDiscovery = false});
                    await client.CreateDatabaseIfNotExistsAsync(new Database {Id = database});

                    graph = await client.CreateDocumentCollectionIfNotExistsAsync(
                        UriFactory.CreateDatabaseUri(database),
                        new DocumentCollection {Id = collection},
                        new RequestOptions {OfferThroughput = 1000});

                    Clear();
                    WriteLine($"Connected to database {database} and collection {graph.Id} at {client.ReadEndpoint}");
                    break;
                }
                catch (Exception e)
                {
                    Clear();
                    WriteLine("Connect to database");
                    WriteLine("************************************************************\r\n");
                    
                    ForegroundColor = ConsoleColor.Red;
                    WriteLine(e.Message);
                    ResetColor();
                }
            }

            return (client, graph);
        }

        private async Task RunUserQuery(DocumentClient client, DocumentCollection graph, string input)
        {
            FeedOptions feedOptions = new FeedOptions
            {
                EnableCrossPartitionQuery = true,
                EnableLowPrecisionOrderBy = true,
                MaxDegreeOfParallelism = 8,
                PopulateQueryMetrics = true
            };

            if (!string.IsNullOrWhiteSpace(partition))
                feedOptions.PartitionKey = new PartitionKey(partition);

            try
            {
                var readQuery = client.CreateGremlinQuery<dynamic>(graph, input, feedOptions, GraphSONMode.Normal);
               
                while (readQuery.HasMoreResults)
                {
                    var response = await readQuery.ExecuteNextAsync();

                    foreach (var next in response)
                    {
                        ForegroundColor = ConsoleColor.Yellow;
                        Write("==> ");
                        ForegroundColor = ConsoleColor.Gray;
                        WriteLine($"{JsonConvert.SerializeObject(next, Formatting.Indented, settings)}");
                        ResetColor();
                    }

                    ForegroundColor = ConsoleColor.Green;
                    WriteLine($"==> Request Charge: {response.RequestCharge}");
                    ForegroundColor = ConsoleColor.Gray;
                }
            }
            catch (Exception e)
            {
                WriteLine();
                ForegroundColor = ConsoleColor.Red;
                WriteLine(e.Message);
                ResetColor();
            }
        }

        private static void WriteGremlinPrompt()
        {
            ForegroundColor = ConsoleColor.Yellow;
            Write("gremlin> ");
            ForegroundColor = ConsoleColor.White;
        }

        private static string ReadLongLine(int maxLength = 2048)
        {
//            var inputBuffer = new byte[maxLength];
//            Stream inputStream = OpenStandardInput(inputBuffer.Length);
//            SetIn(new StreamReader(inputStream, InputEncoding, false, inputBuffer.Length));
            return ReadLine();
        }
    }
}