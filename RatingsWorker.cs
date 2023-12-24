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
    internal class RatingsWorker:BackgroundService
    {
        private readonly ILogger<RatingsWorker> _logger;
        private readonly string _connectionString;


        public RatingsWorker(
            ILogger<RatingsWorker> logger,
            IConfiguration configuration)
        {
            _logger = (ILogger<RatingsWorker>?)logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {


                var ratingz = ReadRatingsFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_ratings.csv");
                await BulkInsertRatingsAsync(ratingz);

                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }
        private List<Rating> ReadRatingsFromCsv(string filePath)
        {
            var ratings = new List<Rating>();

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

                    var rating = new Rating();

                    // Parse and set the properties here
                    if (!helpers.TryParseInt(fields[0], out int userId))
                    {
                        _logger.LogError($"Invalid float value for 'userid' at row {parser.LineNumber}");
                        continue;
                    }
                    rating.UserId = userId;

                    if (!helpers.TryParseInt(fields[1], out int movieId))
                    {
                        _logger.LogError($"Invalid float value for 'MovieId' at row {parser.LineNumber}");
                        continue;
                    }
                    rating.MovieId = movieId;

                    if (!helpers.TryParseFloat(fields[2], out float ratingValue))
                    {
                        _logger.LogError($"Invalid float value for 'ratingvalue' at row {parser.LineNumber}");
                        continue;
                    }
                    rating.RatingValue = ratingValue;

                    if (!helpers.TryParseLong(fields[3], out long timestamp))
                    {
                        _logger.LogError($"Invalid float value for 'timestamp' at row {parser.LineNumber}");
                        continue;
                    }
                    rating.Timestamp = timestamp;

                    ratings.Add(rating);
                }
            }

            return ratings;
        }
        private async Task BulkInsertRatingsAsync(List<Rating> ratings)
        {
            const int batchSize = 100000; // Adjust the batch size as needed

            DataTable ratingsTable = ConvertRatingsToDataTable(ratings);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "Ratings"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("UserId", "UserId");
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("RatingValue", "RatingValue");
                    bulkCopy.ColumnMappings.Add("Timestamp", "Timestamp");

                    try
                    {
                        int totalRows = ratingsTable.Rows.Count;
                        int startIndex = 0;

                        bulkCopy.BulkCopyTimeout = 600; // Increase the timeout to 10 minutes (in seconds)

                        while (startIndex < totalRows)
                        {
                            int batchRowCount = Math.Min(batchSize, totalRows - startIndex);

                            // Create a new DataTable containing a batch of rows
                            DataTable batchTable = ratingsTable.Clone();
                            for (int i = startIndex; i < startIndex + batchRowCount; i++)
                            {
                                batchTable.ImportRow(ratingsTable.Rows[i]);
                            }

                            await bulkCopy.WriteToServerAsync(batchTable);
                            startIndex += batchRowCount;
                        }

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
        private DataTable ConvertRatingsToDataTable(List<Rating> ratings)
        {
            DataTable table = new DataTable();

            // Define the columns
            table.Columns.Add("UserId", typeof(int));
            table.Columns.Add("MovieId", typeof(int));
            table.Columns.Add("RatingValue", typeof(float));
            table.Columns.Add("Timestamp", typeof(long));

            // Populate the DataTable from the list
            foreach (var rating in ratings)
            {
                table.Rows.Add(rating.UserId, rating.MovieId, rating.RatingValue, rating.Timestamp);
            }

            return table;
        }



    }
}
