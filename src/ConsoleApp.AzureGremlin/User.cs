using System;
using System.Collections.Generic;

namespace ConsoleApp.AzureGremlin
{
    public class User
    {
        public Guid id { set; get; }
        public List<string> array { set; get; }
        public string firstName { set; get; }
        public string lastName { set; get; }
        public bool male { set; get; }
        public int age { set; get; }
    }
}