using Newtonsoft.Json.Linq;

namespace Orleans.Transactions.PostgreSql
{
    internal class TransactionMetadataEntity
    {
        public string ETag { get; set; }
        public long CommittedSequenceId { get; set; }
        public JToken Value { get; set; }
    }
}