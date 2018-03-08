using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;

namespace FunctionAppGraph
{
    public static class SimpleExample
    {
        [FunctionName("QueueTrigger")]
        public static void Run(
            [ServiceBusTrigger("users", Connection = "p7graph_RootManageSharedAccessKey_SERVICEBUS")] string myQueueItem,
            TraceWriter log)
        {
            log.Info($"C# function processed: {myQueueItem}");
        }
    }
}
