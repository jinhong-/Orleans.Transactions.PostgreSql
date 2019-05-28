using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Newtonsoft.Json;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions.PostgreSql
{
    public class PostgreSqlTransactionalStateStorageFactory : ITransactionalStateStorageFactory
    {
        private readonly string _name;
        private readonly PostgreSqlTransactionalStateOptions _options;
        private readonly JsonSerializerSettings _jsonSettings;

        public PostgreSqlTransactionalStateStorageFactory(string name, PostgreSqlTransactionalStateOptions options,
            ITypeResolver typeResolver, IGrainFactory grainFactory)
        {
            _name = name;
            _options = options;
            _jsonSettings = TransactionalStateFactory.GetJsonSerializerSettings(
                typeResolver,
                grainFactory);
        }

        public static ITransactionalStateStorageFactory Create(IServiceProvider services, string name)
        {
            IOptionsSnapshot<PostgreSqlTransactionalStateOptions> optionsSnapshot =
                services.GetRequiredService<IOptionsSnapshot<PostgreSqlTransactionalStateOptions>>();
            return ActivatorUtilities.CreateInstance<PostgreSqlTransactionalStateStorageFactory>(services, name,
                optionsSnapshot.Get(name));
        }

        public ITransactionalStateStorage<TState> Create<TState>(string stateName, IGrainActivationContext context)
            where TState : class, new()
        {
            var stateRef = new StateReference(context.GrainInstance.GrainReference, stateName);
            return ActivatorUtilities.CreateInstance<PostgreSqlTransactionalStateStorage2<TState>>(
                context.ActivationServices, stateRef, _options, _jsonSettings);
        }
    }
}