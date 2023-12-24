using Microsoft.Data.SqlClient;
using Microsoft.VisualBasic.FileIO;
using RecomendationSystemWorkerService.Models.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RecomendationSystemWorkerService
{
    internal class MovieLinksSmallWorker : BackgroundService
    {
        private readonly ILogger<MovieLinksSmallWorker> _logger;
        private readonly string _connectionString;


        public MovieLinksSmallWorker(
            ILogger<MovieLinksSmallWorker> logger,
            IConfiguration configuration)
        {
            _logger = (ILogger<MovieLinksSmallWorker>?)logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var smallsteps = ReadMovieLinksSmallFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_links_small.csv");
                await BulkInsertMovieLinksSmallAsync(smallsteps);


                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }
        private List<MovieLinkSmall> ReadMovieLinksSmallFromCsv(string filePath)
        {
            Helpers helpers = new Helpers();
            var movieLinksSmall = new List<MovieLinkSmall>();

            using (TextFieldParser parser = new TextFieldParser(filePath))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                parser.HasFieldsEnclosedInQuotes = true;

                // Skip header row
                if (!parser.EndOfData) parser.ReadLine();

                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();

                    var link = new MovieLinkSmall();

                    if (!helpers.TryParseInt(fields[0], out int movieId))
                    {
                        _logger.LogError($"Invalid integer value for 'MovieId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    link.MovieId = movieId;

                    if (!helpers.TryParseInt(fields[1], out int ImdbId))
                    {
                        _logger.LogError($"Invalid integer value for 'ImdbId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    link.ImdbId = ImdbId;

                    if (!helpers.TryParseInt(fields[2], out int tmdbId))
                    {
                        _logger.LogError($"Invalid integer value for 'TmdbId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    link.TmdbId = tmdbId;

                    movieLinksSmall.Add(link);
                }
            }

            return movieLinksSmall;
        }
        private async Task BulkInsertMovieLinksSmallAsync(List<MovieLinkSmall> movieLinksSmall)
        {
            DataTable movieLinksSmallTable = ConvertMovieLinksSmallToDataTable(movieLinksSmall);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "MovieLinksSmall"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("ImdbId", "ImdbId");
                    bulkCopy.ColumnMappings.Add("TmdbId", "TmdbId");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(movieLinksSmallTable);
                        transaction.Commit(); // Commit the transaction if no exceptions
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback(); // Rollback on error
                        _logger.LogError($"Bulk insert failed: {ex.Message}", ex);
                    }
                }
            }
        }

        private DataTable ConvertMovieLinksSmallToDataTable(List<MovieLinkSmall> movieLinksSmall)
        {
            DataTable table = new DataTable();

            // Define columns
            table.Columns.Add("MovieId", typeof(int));
            table.Columns.Add("ImdbId", typeof(string));
            table.Columns.Add("TmdbId", typeof(int));

            // Populate the DataTable
            foreach (var link in movieLinksSmall)
            {
                table.Rows.Add(link.MovieId, link.ImdbId, link.TmdbId);
            }

            return table;
        }

    }
}
