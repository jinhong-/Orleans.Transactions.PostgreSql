using System;

namespace Orleans.Transactions.PostgreSql
{
    public interface ITransactionState<out TState>
    {
        long SequenceId { get; }
        string TransactionId { get; }
        DateTimeOffset TransactionTimestamp { get; }
        ParticipantId? TransactionManager { get; }
        TState Value { get; }
    }
}