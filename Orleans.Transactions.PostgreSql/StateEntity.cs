using System;
using Newtonsoft.Json.Linq;

namespace Orleans.Transactions.PostgreSql
{
    internal class StateEntity
    {
        public long SequenceId { get; set; }
        
        public string TransactionId { get; set; }

        public DateTime TransactionTimestamp { get; set; }

        public string TransactionManager { get; set; }

        public JToken StateJson { get; set; }
    }
}