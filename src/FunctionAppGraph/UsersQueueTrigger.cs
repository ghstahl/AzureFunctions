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