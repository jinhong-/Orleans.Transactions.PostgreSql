using Newtonsoft.Json.Linq;

namespace Orleans.Transactions.PostgreSql
{
    internal class KeyEntity
    {
        public string StateId { get; set; }
        public long CommittedSequenceId { get; set; }
        public JToken Metadata { get; set; }
    }
}