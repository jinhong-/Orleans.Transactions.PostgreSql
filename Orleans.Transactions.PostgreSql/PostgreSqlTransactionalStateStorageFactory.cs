using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions.PostgreSql
{
    public class PostgreSqlTransactionalStateStorageFactory : ITransactionalStateStorageFactory
    {
        private readonly PostgreSqlTransactionalStateOptions _options;

        public PostgreSqlTransactionalStateStorageFactory(PostgreSqlTransactionalStateOptions options)
        {
            _options = options;
        }

        public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainActivationContext context)
            where TState : class, new()
        {
            var stateId = MakeStateId(context, stateName);
            return ActivatorUtilities.CreateInstance<PostgreSqlTransactionalStateStorage<TState>>(
                context.ActivationServices, stateId, _options);
        }

        protected virtual string MakeStateId(IGrainActivationContext context, string stateName)
        {
            string grainKey = context.GrainInstance.GrainReference.ToShortKeyString();
            var key = $"{grainKey}_{stateName}";
            return key;
        }
    }
}