using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions.PostgreSql
{
    public interface ITransactionMetadata
    {
        string ETag { get; }
        long CommittedSequenceId { get; }
        TransactionalStateMetaData Value { get; }
    }
}