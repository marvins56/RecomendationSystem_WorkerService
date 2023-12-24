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
    internal class GenresWorker:BackgroundService
    {
        private readonly ILogger<GenresWorker> _logger;
        private readonly string _connectionString;


        public GenresWorker(
            ILogger<GenresWorker> logger,
            IConfiguration configuration)
        {
            _logger = (ILogger<GenresWorker>?)logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {


                var genres = ReadGenresFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_genres.csv");
                await BulkInsertGenresAsync(genres);

                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }
        private DataTable ConvertGenresToDataTable(List<Genre> genres)
        {
            DataTable dataTable = new DataTable();

            // Define the columns in the DataTable to match the structure of the Genres table
            dataTable.Columns.Add("MovieId", typeof(int));
            dataTable.Columns.Add("GenreId", typeof(int));
            dataTable.Columns.Add("Name", typeof(string));

            // Populate the DataTable with data from the Genre objects
            foreach (var genre in genres)
            {
                var row = dataTable.NewRow();

                row["MovieId"] = genre.MovieId;
                row["GenreId"] = genre.GenreId;
                row["Name"] = genre.Name ?? string.Empty; // Use null-coalescing for nullable fields

                dataTable.Rows.Add(row);
            }

            return dataTable;
        }

        private async Task BulkInsertGenresAsync(List<Genre> genres)
        {
            DataTable genresTable = ConvertGenresToDataTable(genres);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "Genres"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("GenreId", "GenreId");
                    bulkCopy.ColumnMappings.Add("Name", "Name");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(genresTable);
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

        private List<Genre> ReadGenresFromCsv(string filePath)
        {
            var genres = new List<Genre>();

            using (TextFieldParser parser = new TextFieldParser(filePath))
            {
                Helpers helpers = new Helpers();
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                parser.HasFieldsEnclosedInQuotes = true;

                // Skip header row
                if (!parser.EndOfData) parser.ReadLine();

                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();

                    var genre = new Genre();

                    // Assuming the CSV columns are in the order: MovieId, GenreId, Name
                    if (!helpers.TryParseInt(fields[0], out int movieId))
                    {
                        _logger.LogError($"Invalid int value for 'MovieId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    genre.MovieId = movieId;

                    if (!helpers.TryParseInt(fields[1], out int genreId))
                    {
                        _logger.LogError($"Invalid int value for 'GenreId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    genre.GenreId = genreId;

                    genre.Name = fields[2];

                    genres.Add(genre);
                }
            }

            return genres;
        }

    }
}
