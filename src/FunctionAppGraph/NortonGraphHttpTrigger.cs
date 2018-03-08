using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Gremlin.Net;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Host;

namespace FunctionAppGraph
{
    public class NortonGraphHttpTrigger
    {
        [FunctionName("NortonGraphHttpTrigger")]
        public static async Task RunAsync(
            [HttpTrigger(AuthorizationLevel.Anonymous,new []{"GET"}, WebHookType = "genericJson")] HttpRequestMessage req,
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
        }
    }
}
