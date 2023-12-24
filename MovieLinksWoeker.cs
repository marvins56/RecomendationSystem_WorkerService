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
    internal class MovieLinksWoeker:BackgroundService
    {
        private readonly ILogger<MovieLinksWoeker> _logger;
        private readonly string _connectionString;


        public MovieLinksWoeker(
            ILogger<MovieLinksWoeker> logger,
            IConfiguration configuration)
        {
            _logger = (ILogger<MovieLinksWoeker>?)logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        private DataTable ConvertMovieLinksToDataTable(List<MovieLink> movieLinks)
        {
            DataTable dataTable = new DataTable();

            // Define the columns in the DataTable to match the structure of the MovieLinks table
            dataTable.Columns.Add("MovieId", typeof(float));
            dataTable.Columns.Add("ImdbId", typeof(string));
            dataTable.Columns.Add("TmdbId", typeof(int));

            // Populate the DataTable with data from the MovieLink objects
            foreach (var movieLink in movieLinks)
            {
                var row = dataTable.NewRow();

                row["MovieId"] = movieLink.MovieId;
                row["ImdbId"] = movieLink.ImdbId;
                row["TmdbId"] = movieLink.TmdbId;

                dataTable.Rows.Add(row);
            }

            return dataTable;
        }

        private async Task BulkInsertMovieLinksAsync(List<MovieLink> movieLinks)
        {
            DataTable movieLinksTable = ConvertMovieLinksToDataTable(movieLinks);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "MovieLinks"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("ImdbId", "ImdbId");
                    bulkCopy.ColumnMappings.Add("TmdbId", "TmdbId");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(movieLinksTable);
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

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {

                var links = ReadMovieLinksFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_links.csv");
                await BulkInsertMovieLinksAsync(links);


                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }
        private List<MovieLink> ReadMovieLinksFromCsv(string filePath)
        {
            Helpers helpers = new Helpers();
            var movieLinks = new List<MovieLink>();

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

                    var movieLink = new MovieLink();
                    if (!helpers.TryParseFloat(fields[0], out float movieId))
                    {
                        _logger.LogError($"Invalid float value for 'MovieId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    movieLink.MovieId = movieId;

                    movieLink.ImdbId = fields[1];

                    if (!helpers.TryParseInt(fields[2], out int tmdbId))
                    {
                        _logger.LogError($"Invalid int value for 'TmdbId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    movieLink.TmdbId = tmdbId;

                    movieLinks.Add(movieLink);
                }
            }

            return movieLinks;
        }

    }
}
