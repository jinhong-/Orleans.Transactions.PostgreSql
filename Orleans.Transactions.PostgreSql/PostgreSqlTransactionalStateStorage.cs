using System;
using System.Linq;
using System.Net;
using System.Reflection.Metadata.Ecma335;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Npgsql;
using Orleans.Transactions.Abstractions;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace Orleans.Transactions.PostgreSql
{
    public class PostgreSqlTransactionalStateStorage<TState> : TransactionalStateStorage<TState>
        where TState : class, new()
    {
        private class TransactionMetadata : ITransactionMetadata
        {
            public string ETag { get; set; }
            public long CommittedSequenceId { get; set; }
            public TransactionalStateMetaData Value { get; set; }
        }

        private class TransactionState : ITransactionState<TState>
        {
            public long SequenceId { get; set; }
            public string TransactionId { get; set; }
            public DateTimeOffset TransactionTimestamp { get; set; }
            public ParticipantId? TransactionManager { get; set; }
            public TState Value { get; set; }
        }

        private readonly string _stateId;
        private readonly PostgreSqlTransactionalStateOptions _options;

        public PostgreSqlTransactionalStateStorage(string stateId, PostgreSqlTransactionalStateOptions options)
        {
            _stateId = stateId;
            _options = options;
        }

        protected override Task<ITransactionMetadata> ReadMetadata()
            => ExecuteQuery<ITransactionMetadata>(async db =>
            {
                var transactionMetadata = await db.Query(_options.MetadataTableName).Where("state_id", _stateId)
                    .FirstOrDefaultAsync<TransactionMetadataEntity>().ConfigureAwait(false);
                if (transactionMetadata == null)
                {
                    transactionMetadata = new TransactionMetadataEntity();
                }

                return new TransactionMetadata
                {
                    ETag = transactionMetadata.ETag,
                    CommittedSequenceId = transactionMetadata.CommittedSequenceId,
                    Value = transactionMetadata.Value.ToObject<TransactionalStateMetaData>(CreateSerializer())
                };
            });

        protected override Task<ITransactionState<TState>[]> ReadStates(long fromSequenceId)
            => ExecuteQuery<ITransactionState<TState>[]>(async db =>
            {
                var results = await db.Query(_options.StateTableName).Where("sequence_id", ">=", fromSequenceId)
                    .GetAsync<TransactionStateEntity>().ConfigureAwait(false);

                // ReSharper disable once CoVariantArrayConversion
                return results.Select(x => new TransactionState
                {
                    SequenceId = x.SequenceId,
                    TransactionId = x.TransactionId,
                    TransactionTimestamp = x.TransactionTimestamp.UtcDateTime,
                    Value = x.Value.ToObject<TState>(CreateSerializer()),
                    TransactionManager = x.TransactionManager.ToObject<ParticipantId>(CreateSerializer())
                }).ToArray();
            });

        protected override Task<ITransactionState<TState>> PersistState(PendingTransactionState<TState> pendingState,
            long? commitUpTo,
            ITransactionState<TState> existingState = null) => ExecuteQuery<ITransactionState<TState>>(async db =>
        {
            var transactionManager = JToken.FromObject(pendingState.TransactionManager, CreateSerializer());
            var stateValue = JToken.FromObject(pendingState.State, CreateSerializer());

            if (existingState == null)
            {
                await db.Query(_options.MetadataTableName).AsInsert(new[]
                    {
                        "state_id", "sequence_id", "transaction_manager", "value", "timestamp", "transaction_id"
                    },
                    new[]
                    {
                        _stateId,
                        pendingState.SequenceId,
                        transactionManager,
                        stateValue,
                        pendingState.TimeStamp,
                        pendingState.TransactionId
                    }).FirstOrDefaultAsync();
            }
            else
            {
                var rowsUpdated = await db.Query(_options.MetadataTableName).Where("state_id", _stateId)
                    .Where("sequence_id", pendingState.SequenceId)
                    .AsUpdate(new[] {"transaction_manager", "value", "timestamp", "transaction_id"}, new[]
                    {
                        transactionManager,
                        stateValue,
                        pendingState.TimeStamp,
                        pendingState.TransactionId
                    }).FirstOrDefaultAsync<int>();

                if (rowsUpdated != 1)
                    throw new InvalidOperationException("Something went wrong while persisting existing state");
            }

            return new TransactionState
            {
                Value = pendingState.State,
                SequenceId = pendingState.SequenceId,
                TransactionId = pendingState.TransactionId,
                TransactionManager = pendingState.TransactionManager,
                TransactionTimestamp = pendingState.TimeStamp
            };
        });

        protected override Task RemoveAbortedState(ITransactionState<TState> state)
            => ExecuteQuery(async db =>
            {
                var rowsDeleted = await db.Query(_options.StateTableName)
                    .Where("state_id", _stateId)
                    .Where("sequence_id", state.SequenceId)
                    .AsDelete()
                    .FirstOrDefaultAsync<int>().ConfigureAwait(false);
                if (rowsDeleted != 1)
                    throw new InvalidOperationException("Something went wrong when trying to delete transaction state");
            });

        protected override Task<ITransactionMetadata> PersistMetadata(TransactionalStateMetaData value,
            long commitSequenceId) => ExecuteQuery<ITransactionMetadata>(async db =>
        {
            var newEtag = Guid.NewGuid().ToString();
            var tableName = _options.MetadataTableName;
            var serializedValue = JToken.FromObject(value, CreateSerializer());

            if (Metadata.ETag == null)
            {
                await db.Query(tableName)
                    .AsInsert(new[] {"state_id", "committed_sequence_id", "etag", "value"},
                        new[]
                        {
                            _stateId, commitSequenceId, newEtag, serializedValue
                        }).FirstOrDefaultAsync().ConfigureAwait(false);
            }
            else
            {
                var rowsUpdated = await db.Query(tableName).Where("state_id", _stateId).Where("etag", Metadata.ETag)
                    .AsUpdate(new[] {"committed_sequence_id", "etag", "value"}, new[]
                    {
                        commitSequenceId, newEtag, serializedValue
                    }).FirstOrDefaultAsync<int>();

                if (rowsUpdated == 0)
                {
                    throw new InvalidOperationException("Could not update metadata. Possible concurrency issue");
                }
            }

            return new TransactionMetadata
            {
                Value = value,
                ETag = newEtag,
                CommittedSequenceId = commitSequenceId
            };
        });

        protected override Task LoadFinalize()
        {
            foreach (var state in States.OfType<TransactionState>())
            {
                state.Value = null;
            }

            return Task.CompletedTask;
        }

        private JsonSerializer CreateSerializer() => JsonSerializer.Create(_options.JsonSerializerSettings);

        private Task ExecuteQuery(Func<QueryFactory, Task> execute) => ExecuteQuery<object>(async db =>
        {
            await execute(db);
            return null;
        });


        private Task<TResult> ExecuteQuery<TResult>(Func<QueryFactory, Task<TResult>> execute)
        {
            using (var connection = new NpgsqlConnection(_options.ConnectionString))
            {
                var compiler = new PostgresCompiler();

                var db = new QueryFactory(connection, compiler);
                return execute(db);
            }
        }
    }
}