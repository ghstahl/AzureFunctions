using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net.Http;
using System.ServiceModel.Channels;
using System.Text;
using System.Threading.Tasks;
using Gremlin.Net;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Azure.ServiceBus;
using Newtonsoft.Json;

namespace FunctionAppGraph
{
    public class MetaData
    {
        public string Category { get; set; }
        public string Version { get; set; }
    }
    public class MessageModel
    {
        public MetaData MetaData { get; set; }
        public object Data { get; set; }
    }

    public class ThunderDomeDirtyHttpTrigger
    {
        [FunctionName("ThunderDomeDirtyHttpTrigger")]
        public static async Task RunAsync(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequestMessage req,
            TraceWriter log)
        {
            IQueueClient queueClient;
            string queueName = "users";
            string serviceBusConnectionString =
                ConfigurationManager.AppSettings["p7graph_RootManageSharedAccessKey_SERVICEBUS"];
            queueClient = new QueueClient(serviceBusConnectionString, queueName);

            // Send messages.
            await SendMessagesAsync(queueClient,1);
        }

        static async Task SendMessagesAsync(IQueueClient queueClient,int numberOfMessagesToSend)
        {
            try
            {
                for (var i = 0; i < numberOfMessagesToSend; i++)
                {
                    // Create a new message to send to the queue.

                    string data = $"Message {i} created at: {DateTime.Now}";

                    var messageModel = new MessageModel
                    {
                        MetaData = new MetaData()
                        {
                            Category = "Test Blaster",
                            Version = "0.0.0.1"
                        },
                        Data = data
                    };

                    var messageBody = JsonConvert.SerializeObject(messageModel);

                    var message = new Microsoft.Azure.ServiceBus.Message(Encoding.UTF8.GetBytes(messageBody));

                    // Write the body of the message to the console.
                    Console.WriteLine($"Sending message: {messageBody}");

                    // Send the message to the queue.
                    await queueClient.SendAsync(message);
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }
    }


    public class NortonGraphHttpTrigger
    {
        private static Dictionary<string, string> GremlinQueries = new Dictionary<string, string>
        {
            {"Cleanup", "g.V().drop()"},

            {"AddVertex user 1", "g.addV('user').property('id', 'na-guid-keith').property('externalIDP', 'Norton')"},
            {"AddVertex user 2", "g.addV('user').property('id', 'na-guid-thomas').property('externalIDP', 'T-Online')"},
            {"AddVertex user 3", "g.addV('user').property('id', 'na-guid-randy').property('externalIDP', 'Comcast')"},

            {
                "AddVertex entitlement 1",
                "g.addV('entitlement').property('id', 'entitlement-1').property('name', 'Norton Security')"
            },
            {
                "AddVertex entitlement 2",
                "g.addV('entitlement').property('id', 'entitlement-2').property('name', 'Norton Uber')"
            },
            {
                "AddVertex entitlement 3",
                "g.addV('entitlement').property('id', 'entitlement-3').property('name', 'Norton DSP')"
            },

            {"addedge user-knows-entitlement 1", "g.v('na-guid-keith').adde('knows').to(g.v('entitlement-1'))"},
            {"addedge user-knows-entitlement 2", "g.v('na-guid-thomas').adde('knows').to(g.v('entitlement-2'))"},
            {"addedge user-knows-entitlement 3", "g.v('na-guid-randy').adde('knows').to(g.v('entitlement-3'))"},

            {"AddVertex Seat 1", "g.addV('entitlement').property('id', 'seat-1')"},
            {"AddVertex Seat 2", "g.addV('entitlement').property('id', 'seat-2')"},
            {"AddVertex Seat 3", "g.addV('entitlement').property('id', 'seat-3')"},

            {"addedge entitlement-knows-seat 1", "g.v('entitlement-1').adde('knows').to(g.v('seat-1'))"},
            {"addedge entitlement-knows-seat 2", "g.v('entitlement-2').adde('knows').to(g.v('seat-2'))"},
            {"addedge entitlement-knows-seat 3", "g.v('entitlement-3').adde('knows').to(g.v('seat-3'))"},



            //{ "AddVertex 1",    "g.addV('person').property('id', 'thomas').property('firstName', 'Thomas').property('age', 44)" },
            //{ "AddVertex 2",    "g.addV('person').property('id', 'mary').property('firstName', 'Mary').property('lastName', 'Andersen').property('age', 39)" },
            //{ "AddVertex 3",    "g.addV('person').property('id', 'ben').property('firstName', 'Ben').property('lastName', 'Miller')" },
            //{ "AddVertex 4",    "g.addV('person').property('id', 'robin').property('firstName', 'Robin').property('lastName', 'Wakefield')" },
            //{ "addedge 1",      "g.v('thomas').adde('knows').to(g.v('mary'))" },
            //{ "addedge 2",      "g.v('thomas').adde('knows').to(g.v('ben'))" },
            //{ "addedge 3",      "g.v('ben').adde('knows').to(g.v('robin'))" },
            //{ "UpdateVertex",   "g.V('thomas').property('age', 44)" },
            //{ "CountVertices",  "g.V().count()" },
            //{ "Filter Range",   "g.V().hasLabel('person').has('age', gt(40))" },
            //{ "Project",        "g.V().hasLabel('person').values('firstName')" },
            //{ "Sort",           "g.V().hasLabel('person').order().by('firstName', decr)" },
            //{ "Traverse",       "g.V('thomas').out('knows').hasLabel('person')" },
            //{ "Traverse 2x",    "g.V('thomas').out('knows').hasLabel('person').out('knows').hasLabel('person')" },
            //{ "Loop",           "g.V('thomas').repeat(out()).until(has('id', 'robin')).path()" },
            //{ "DropEdge",       "g.V('thomas').outE('knows').where(inV().has('id', 'mary')).drop()" },
            //{ "CountEdges",     "g.E().count()" },
            //{ "DropVertex",     "g.V('thomas').drop()" },
        };

        [FunctionName("NortonGraphHttpTrigger")]
        public static async Task RunAsync(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequestMessage req,
            TraceWriter log)
        {
            string graphDbConnectionString = ConfigurationManager.AppSettings["graphdbconnectionstring"];
            string graphHostname = ConfigurationManager.AppSettings["graphhostname"];
            string database = ConfigurationManager.AppSettings["database"];
            string collection = ConfigurationManager.AppSettings["collection"];
            string graphDbKey = ConfigurationManager.AppSettings["graphdbkey"];
            string graphDbUri = ConfigurationManager.AppSettings["graphdburi"];
            int port = Convert.ToInt32(ConfigurationManager.AppSettings["port"]);
            string username = $"/dbs/{database}/colls/{collection}";
            log.Info($"username: {username}");

            var gremlinServer = new GremlinServer(graphHostname, port, enableSsl: true,
                username: "/dbs/" + database + "/colls/" + collection,
                password: graphDbKey);

            string localPath = req.RequestUri.LocalPath;

            log.Info($"C# function processed: {localPath}");



            using (var gremlinClient = new GremlinClient(gremlinServer))
            {
                foreach (var query in GremlinQueries)
                {
                    log.Info(String.Format("Running this query: {0}: {1}", query.Key, query.Value));

                    // Create async task to execute the Gremlin query.
                    var task = gremlinClient.SubmitAsync<dynamic>(query.Value);
                    task.Wait();

                    foreach (var result in task.Result)
                    {
                        // The vertex results are formed as Dictionaries with a nested dictionary for their properties
                        string output = JsonConvert.SerializeObject(result);
                        log.Info(String.Format("\tResult:\n\t{0}", output));
                    }
                 
                }
            }
        }
    }
}
