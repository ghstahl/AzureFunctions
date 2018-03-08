using System;
using System.Collections.Generic;
using System.Configuration;
using System.Threading.Tasks;
using Gremlin.Net;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace FunctionAppGraph
{
    public static class UsersQueueTrigger
    {
        [FunctionName("UsersQueueTrigger")]
        public static async Task RunAsync(
            [ServiceBusTrigger("users", Connection = "p7graph_RootManageSharedAccessKey_SERVICEBUS")] string myQueueItem,
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

            log.Info($"C# function processed: {myQueueItem}");
        }
    }
}