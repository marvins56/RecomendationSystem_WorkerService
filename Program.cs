using RecomendationSystemWorkerService;

IHost host = Host.CreateDefaultBuilder(args)
    .ConfigureServices(services =>
    {
        services.AddHostedService<Worker>();
        services.AddHostedService<MoviesMetadataWorker>();
        services.AddHostedService<ExcelToDbTransfer>();


    })
    .Build();

host.Run();
