using System;
using System.Collections.Generic;
using System.Configuration;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Graphs;
using Microsoft.Azure.Graphs.Elements;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;

namespace FunctionAppGraph
{
    public class NortonGraphHttpTrigger2
    {
        [FunctionName("NortonGraphHttpTrigger2")]
        public static async Task RunAsync(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)]
            HttpRequestMessage req,
            TraceWriter log)
        {
            log.Info($"C# function processing: NortonGraphHttpTrigger2");

            string documentsEndpoint = ConfigurationManager.AppSettings["documentsendpoint"];
            string graphDbKey = ConfigurationManager.AppSettings["graphdbkey"];
            string database = ConfigurationManager.AppSettings["database"];
            string documentCollection = ConfigurationManager.AppSettings["collection"];

            log.Info($"   documentsEndpoint:{documentsEndpoint}");
            log.Info($"   graphDbKey:{graphDbKey}");
            log.Info($"   database:{database}");
            log.Info($"   documentCollection:{documentCollection}");

            var client = new DocumentClient(new Uri(documentsEndpoint), graphDbKey);
            var data = new Dictionary<string, dynamic>()
            {
                {"id", Guid.NewGuid()},
                {"array", new List<string> {"blah"}},
                {"firstName", "Justin"},
                {"lastName", "Bieber"},
                {"male", true},
                {"age", 32},
            };
            var data2 = new User
            {
                id = Guid.NewGuid(),
                array = new List<string> {"blah2"},
                firstName = "Larry",
                lastName = "Gowan",
                male = true,
                age = 32
            };
            var query = QueryAddVertex("user", data);
            log.Info($"   query:{query}");
            var query2 = QueryAddVertex("user", data2);
            log.Info($"   query2:{query2}");
            var uri = UriFactory.CreateDatabaseUri(database);
           // log.Info($" databaseUri: {uri.AbsolutePath}");

            var collection = await client.CreateDocumentCollectionIfNotExistsAsync(uri, new DocumentCollection { Id = documentCollection });

            var resp = await client.CreateGremlinQuery<Vertex>(collection, query).ExecuteNextAsync();
            log.Info($"C# function processed: {resp.ActivityId}");

            var resp2 = await client.CreateGremlinQuery<Vertex>(collection, query2).ExecuteNextAsync();
            log.Info($"C# function processed: {resp2.ActivityId}");
            log.Info($"C# function processed: NortonGraphHttpTrigger2");
        }
        public static string QueryAddVertex<T>(string label, T properties)
        {
            Dictionary<string, dynamic> props = JsonConvert.DeserializeObject<Dictionary<string, dynamic>>(JsonConvert.SerializeObject(properties));

            var query = new StringBuilder($"g.addV('{label}')");

            foreach (var p in props)
            {
                query.Append($".property('{p.Key}', ");

                var t = (Type)p.Value.GetType();

                if (p.Value is string || p.Value is Guid)
                {
                    query.Append($"'{p.Value}'");
                }
                else if (p.Value is bool)
                {
                    query.Append($"{p.Value.ToString().ToLower()}");
                }
                else if (p.Value is int || p.Value is long || p.Value is decimal || p.Value is double)
                {
                    query.Append($"{p.Value}");
                }
                else
                {
                    query.Append($"'{JsonConvert.SerializeObject(p.Value)}'");
                }

                query.Append(")");
            }

            return query.ToString();
        }
    }
}