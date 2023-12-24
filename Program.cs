using RecomendationSystemWorkerService;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddHostedService<MoviesMetadataWorker>();
        services.AddHostedService<RatingsSmallWorker>();
        services.AddHostedService<GenresWorker>();
        services.AddHostedService<ProductionCompaniesWorker>();
        services.AddHostedService<ProductionCountriesWorker>();
        services.AddHostedService<MovieLinksSmallWorker>();
        services.AddHostedService<MovieLinksWoeker>();
        services.AddHostedService<ExpandedKeywordsWorker>();
        services.AddHostedService<RatingsWorker>();
        services.AddHostedService<CreditsCrewWorker>();
        services.AddHostedService<SpokenLanguagesWorker>();
        //TODO: Add credits Cast

    })
    .Build();

host.Run();
