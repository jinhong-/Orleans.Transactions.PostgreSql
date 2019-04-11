using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using Orleans.Transactions.Abstractions;
using SqlKata.Compilers;
using SqlKata.Execution;

namespace Orleans.Transactions.PostgreSql
{
    public class PostgreSqlTransactionalStateStorage<TState> : ITransactionalStateStorage<TState>
        where TState : class, new()
    {
        private readonly string _stateId;
        private readonly PostgreSqlTransactionalStateOptions _options;

        private KeyEntity _key;
        private List<StateEntity> _states;

        public PostgreSqlTransactionalStateStorage(string stateId, PostgreSqlTransactionalStateOptions options)
        {
            _stateId = stateId;
            _options = options;
        }

        public Task<TransactionalStorageLoadResponse<TState>> Load()
            => ExecuteQuery<TransactionalStorageLoadResponse<TState>>(async db =>
            {
                _key = await ReadKey(db).ConfigureAwait(false);
                _states = await ReadStates(db, _key.CommittedSequenceId).ConfigureAwait(false);
                throw new NotImplementedException();
            });

        public Task<string> Store(string expectedETag, TransactionalStateMetaData metadata,
            List<PendingTransactionState<TState>> statesToPrepare, long? commitUpTo,
            long? abortAfter)
        {
            throw new NotImplementedException();
        }

        private async Task<List<StateEntity>> ReadStates(QueryFactory db, long committedSequenceId)
        {
            var results = await db.Query(_options.StateTableName).Where("sequence_id", ">=", committedSequenceId)
                .GetAsync<StateEntity>().ConfigureAwait(false);
            return results.ToList();
        }

        private Task<KeyEntity> ReadKey(QueryFactory db)
        {
            return db.Query(_options.KeyTableName).Where("state_id", _stateId)
                .FirstOrDefaultAsync<KeyEntity>();
        }

        private Task<TResult> ExecuteQuery<TResult>(Func<QueryFactory, Task<TResult>> execute)
        {
            using (var connection = new NpgsqlConnection(_options.ConnectionString))
            {
                var compiler = new SqlServerCompiler();

                var db = new QueryFactory(connection, compiler);
                return execute(db);
            }
        }
    }
}