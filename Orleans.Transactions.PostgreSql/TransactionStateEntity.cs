using System;
using Newtonsoft.Json.Linq;

namespace Orleans.Transactions.PostgreSql
{
    internal class TransactionStateEntity
    {
        public long SequenceId { get; set; }
        
        public string TransactionId { get; set; }

        public DateTimeOffset TransactionTimestamp { get; set; }

        public JToken TransactionManager { get; set; }

        public JToken Value { get; set; }
    }
}