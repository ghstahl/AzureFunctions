using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Graphs;
using Microsoft.Azure.Graphs.Elements;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

namespace ConsoleApp.AzureGremlin
{
    class Program
    {
        static Dictionary<string, string> GremlinQueries = new Dictionary<string, string>
        {
            { "Cleanup",        "g.V().drop()" },

            { "AddVertex user 1",    "g.addV('person').property('id', 'fran').property('firstName', 'Fran').property('age', 44)" },
            { "AddVertex user 2",    "g.addV('person').property('id', 'spencer').property('firstName', 'Spencer').property('age', 44)" },

            { "AddVertex entitlement 1",    "g.addV('entitlement').property('id', 'F-entitlement-1234').property('template', 'Security Level One').property('seats', 3)" },
            { "AddVertex entitlement 2",    "g.addV('entitlement').property('id', 'S-entitlement-1234').property('template', 'Security Level One').property('seats', 3)" },

            { "AddVertex entitlement 3",    "g.addV('entitlement').property('id', 'F-entitlement-2345').property('template', 'Security Level two').property('seats', 3)" },
            { "AddVertex entitlement 4",    "g.addV('entitlement').property('id', 'S-entitlement-2345').property('template', 'Security Level two').property('seats', 3)" },

            { "AddEdge ownership 1",      "g.V('fran').addE('owns').to(g.V('F-entitlement-1234'))" },
            { "AddEdge ownership 2",      "g.V('fran').addE('owns').to(g.V('F-entitlement-2345'))" },
            { "AddEdge ownership 3",      "g.V('spencer').addE('owns').to(g.V('S-entitlement-1234'))" },
            { "AddEdge ownership 4",      "g.V('spencer').addE('owns').to(g.V('S-entitlement-2345'))" },



            { "AddVertex seat 1",    "g.addV('seat').property('id', 'F-Seat-1234').property('productInstanceId', 'F-productInstanceId-1234')" },
            { "AddVertex seat 2",    "g.addV('seat').property('id', 'F-Seat-2345').property('productInstanceId', 'F-productInstanceId-2345')" },
            { "AddVertex seat 3",    "g.addV('seat').property('id', 'F-Seat-3456').property('productInstanceId', 'F-productInstanceId-3456')" },
            { "AddVertex seat 4",    "g.addV('seat').property('id', 'S-Seat-1234').property('productInstanceId', 'S-productInstanceId-1234')" },
            { "AddVertex seat 5",    "g.addV('seat').property('id', 'S-Seat-2345').property('productInstanceId', 'S-productInstanceId-2345')" },
            { "AddVertex seat 6",    "g.addV('seat').property('id', 'S-Seat-3456').property('productInstanceId', 'S-productInstanceId-3456')" },

            { "AddEdge ownership 5",      "g.V('F-entitlement-1234').addE('owns').to(g.V('F-Seat-1234'))" },
            { "AddEdge ownership 6",      "g.V('F-entitlement-1234').addE('owns').to(g.V('F-Seat-2345'))" },
            { "AddEdge ownership 7",      "g.V('F-entitlement-1234').addE('owns').to(g.V('F-Seat-3456'))" },

            { "AddEdge ownership 8",      "g.V('S-entitlement-2345').addE('owns').to(g.V('S-Seat-1234'))" },
            { "AddEdge ownership 9",      "g.V('S-entitlement-2345').addE('owns').to(g.V('S-Seat-2345'))" },
            { "AddEdge ownership 10",     "g.V('S-entitlement-2345').addE('owns').to(g.V('S-Seat-3456'))" },

            { "AddVertex 1",    "g.addV('person').property('id', 'thomas').property('firstName', 'Thomas').property('age', 44)" },
            { "AddVertex 2",    "g.addV('person').property('id', 'mary').property('firstName', 'Mary').property('lastName', 'Andersen').property('age', 39)" },
            { "AddVertex 3",    "g.addV('person').property('id', 'ben').property('firstName', 'Ben').property('lastName', 'Miller')" },
            { "AddVertex 4",    "g.addV('person').property('id', 'robin').property('firstName', 'Robin').property('lastName', 'Wakefield')" },

            { "AddVertex 5",    "g.addV('person').property('id', 'bill').property('firstName', 'Bill').property('lastName', 'Smith')" },
            { "AddVertex 6",    "g.addV('person').property('id', 'debbie').property('firstName', 'Debbie').property('lastName', 'Smith')" },

            { "AddVertex 7",    "g.addV('person').property('id', 'luke').property('firstName', 'Luke').property('lastName', 'Smith')" },
            { "AddVertex 8",    "g.addV('person').property('id', 'sally').property('firstName', 'Sally').property('lastName', 'Smith')" },

            { "AddEdge 1",      "g.V('thomas').addE('knows').to(g.V('mary'))" },
            { "AddEdge 2",      "g.V('thomas').addE('knows').to(g.V('ben'))" },
            { "AddEdge 3",      "g.V('ben').addE('knows').to(g.V('robin'))" },

            { "AddEdge 4",      "g.V('bill').addE('married to').to(g.V('debbie'))" },
            { "AddEdge 5",      "g.V('debbie').addE('married to').to(g.V('bill'))" },

            { "AddEdge 6",      "g.V('bill').addE('parent of').to(g.V('luke'))" },
            { "AddEdge 7",      "g.V('debbie').addE('parent of').to(g.V('luke'))" },

            { "AddEdge 8",      "g.V('luke').addE('married to').to(g.V('sally'))" },
            { "AddEdge 9",      "g.V('sally').addE('married to').to(g.V('luke'))" },

            { "UpdateVertex",   "g.V('thomas').property('age', 44)" },
            { "CountVertices",  "g.V().count()" },
            { "Filter Range",   "g.V().hasLabel('person').has('age', gt(40))" },
            { "Project",        "g.V().hasLabel('person').values('firstName')" },
            { "Sort",           "g.V().hasLabel('person').order().by('firstName', decr)" },
            { "Traverse",       "g.V('thomas').out('knows').hasLabel('person')" },
            { "Traverse 2x",    "g.V('thomas').out('knows').hasLabel('person').out('knows').hasLabel('person')" },
            { "Loop",           "g.V('thomas').repeat(out()).until(has('id', 'robin')).path()" },
            { "DropEdge",       "g.V('thomas').outE('knows').where(inV().has('id', 'mary')).drop()" },
            { "CountEdges",     "g.E().count()" },
            { "DropVertex",     "g.V('thomas').drop()" },
            { "Seat to owner",  "g.V('S-Seat-3456').repeat(__.in()).until(has('label', 'person')).emit().path()" },
        };
  

        static async Task MainAsync(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);
            IConfigurationRoot configuration = builder.Build();


            string documentsEndpoint = configuration["azure:documentsendpoint"];
            string graphDbKey = configuration["azure:graphdbkey"];
            string databaseName = configuration["azure:database"];
            string documentCollection = configuration["azure:collection"];

            Console.WriteLine($"documentsEndpoint:{documentsEndpoint}");
            Console.WriteLine($"graphDbKey:{graphDbKey}");
            Console.WriteLine($"databaseName:{databaseName}");
            Console.WriteLine($"documentCollection:{documentCollection}");
            Console.WriteLine("------------------------------------");

            var client = new DocumentClient(new Uri(documentsEndpoint), graphDbKey);
            Database database = await client.CreateDatabaseIfNotExistsAsync(new Database { Id = databaseName });
            DocumentCollection graph = await client.CreateDocumentCollectionIfNotExistsAsync(
                UriFactory.CreateDatabaseUri(databaseName),
                new DocumentCollection { Id = documentCollection },
                new RequestOptions { OfferThroughput = 1000 });

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
                array = new List<string> { "blah2" },
                firstName = "Larry",
                lastName = "Gowan",
                male = true,
                age = 32
            };
            var query1 = QueryAddVertex("user", data);
            var query2 = QueryAddVertex("user", data2);
            var resp = await client.CreateGremlinQuery<Vertex>(graph, query1).ExecuteNextAsync();
            Console.WriteLine($"C# function processed: {resp.ActivityId}");

            var resp2 = await client.CreateGremlinQuery<Vertex>(graph, query2).ExecuteNextAsync();
            Console.WriteLine($"C# function processed: {resp2.ActivityId}");

            foreach (KeyValuePair<string, string> gremlinQuery in GremlinQueries)
            {
                Console.WriteLine($"Running {gremlinQuery.Key}: {gremlinQuery.Value}");

                // The CreateGremlinQuery method extensions allow you to execute Gremlin queries and iterate
                // results asychronously
                IDocumentQuery<dynamic> query = client.CreateGremlinQuery<dynamic>(graph, gremlinQuery.Value);
                while (query.HasMoreResults)
                {
                    foreach (dynamic result in await query.ExecuteNextAsync())
                    {
                        Console.WriteLine($"\t {JsonConvert.SerializeObject(result)}");
                    }
                }
            }

            // Data is returned in GraphSON format, which be deserialized into a strongly-typed vertex, edge or property class
            // The following snippet shows how to do this
            string gremlin = GremlinQueries["AddVertex 1"];
            Console.WriteLine($"Running Add Vertex with deserialization: {gremlin}");

            IDocumentQuery<Vertex> insertVertex = client.CreateGremlinQuery<Vertex>(graph, GremlinQueries["AddVertex 1"]);
            while (insertVertex.HasMoreResults)
            {
                foreach (Vertex vertex in await insertVertex.ExecuteNextAsync<Vertex>())
                {
                    // Since Gremlin is designed for multi-valued properties, the format returns an array. Here we just read
                    // the first value
                    string name = (string)vertex.GetVertexProperties("firstName").First().Value;
                    Console.WriteLine($"\t Id:{vertex.Id}, Name: {name}");
                }
            }


            Console.WriteLine($"C# function processed: NortonGraphHttpTrigger2");

        }

        static void Main(string[] args)
        {
            var task = MainAsync(args);
            task.Wait();

            // Exit program
            Console.WriteLine("Done. Press any key to exit...");
            Console.ReadLine();
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
