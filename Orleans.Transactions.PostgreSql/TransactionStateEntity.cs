using System;
using Newtonsoft.Json.Linq;

namespace Orleans.Transactions.PostgreSql
{
    public class TransactionStateEntity
    {
        public long SequenceId { get; set; }
        
        public string TransactionId { get; set; }

        //public DateTimeOffset Timestamp { get; set; }

//        public JObject TransactionManager { get; set; }

//        public JObject Value { get; set; }
    }
}