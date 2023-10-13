using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Providers;
using Orleans.Runtime;
using Orleans.Transactions.Abstractions;

namespace Orleans.Transactions.PostgreSql
{
    public static class ServiceCollectionExtensions
    {
        public static ISiloHostBuilder AddPostgreSqlTransactionalStateStorageAsDefault(this ISiloHostBuilder builder,
            Action<PostgreSqlTransactionalStateOptions> configureOptions = null)
        {
            return builder.AddPostgreSqlTableTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME,
                configureOptions);
        }

        public static ISiloHostBuilder AddPostgreSqlTableTransactionalStateStorage(this ISiloHostBuilder builder,
            string name, Action<PostgreSqlTransactionalStateOptions> configureOptions = null)
        {
            return builder.ConfigureServices(services =>
                services.AddPostgreSqlTableTransactionalStateStorage(name, ob =>
                {
                    if (configureOptions != null) ob.Configure(configureOptions);
                }));
        }

        public static ISiloBuilder AddPostgreSqlTransactionalStateStorageAsDefault(this ISiloBuilder builder,
            Action<PostgreSqlTransactionalStateOptions> configureOptions = null)
        {
            return builder.AddPostgreSqlTableTransactionalStateStorage(ProviderConstants.DEFAULT_STORAGE_PROVIDER_NAME,
                configureOptions);
        }

        public static ISiloBuilder AddPostgreSqlTableTransactionalStateStorage(this ISiloBuilder builder,
            string name, Action<PostgreSqlTransactionalStateOptions> configureOptions = null)
        {
            return builder.ConfigureServices(services =>
                services.AddPostgreSqlTableTransactionalStateStorage(name, ob =>
                {
                    if (configureOptions != null) ob.Configure(configureOptions);
                }));
        }

        private static IServiceCollection AddPostgreSqlTableTransactionalStateStorage(this IServiceCollection services,
            string name,
            Action<OptionsBuilder<PostgreSqlTransactionalStateOptions>> configureOptions = null)
        {
            configureOptions?.Invoke(services.AddOptions<PostgreSqlTransactionalStateOptions>(name));

            services.TryAddSingleton<ITransactionalStateStorageFactory>(sp =>
                sp.GetServiceByName<ITransactionalStateStorageFactory>(ProviderConstants
                    .DEFAULT_STORAGE_PROVIDER_NAME));
            services.AddSingletonNamedService<ITransactionalStateStorageFactory>(name,
                PostgreSqlTransactionalStateStorageFactory.Create);

            return services;
        }
    }
}