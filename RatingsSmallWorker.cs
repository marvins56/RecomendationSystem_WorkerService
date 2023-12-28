
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using ClosedXML.Excel;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.VisualBasic.FileIO;
using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Data.SqlClient;
using RecomendationSystemWorkerService.Models.Helpers;

namespace RecomendationSystemWorkerService
{
    public class RatingsSmallWorker : BackgroundService
    {
        private readonly ILogger<RatingsSmallWorker> _logger;
        private readonly string _connectionString;

        public RatingsSmallWorker(
            ILogger<RatingsSmallWorker> logger,
            IConfiguration configuration)
        {
            _logger = logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var ratingsmall = ReadRatingsSmallFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_ratings_small.csv");
                await BulkInsertRatingsSmallAsync(ratingsmall);

                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }

        private async Task BulkInsertRatingsSmallAsync(List<RatingSmall> ratingsSmall)
        {
            DataTable ratingsSmallTable = ConvertRatingsSmallToDataTable(ratingsSmall);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "RatingsSmall"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("UserId", "UserId");
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("RatingValue", "RatingValue");
                    bulkCopy.ColumnMappings.Add("Timestamp", "Timestamp");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(ratingsSmallTable);
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
        private List<RatingSmall> ReadRatingsSmallFromCsv(string filePath)
        {
            var ratingsSmall = new List<RatingSmall>();

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

                    var ratingSmall = new RatingSmall();

                    // Parse and set the properties here
                    if (!helpers.TryParseInt(fields[0], out int userId))
                    {
                        _logger.LogError($"Invalid float value for 'userId' at row {parser.LineNumber}");
                        continue;
                    }
                    ratingSmall.UserId = userId;

                    if (!helpers.TryParseInt(fields[1], out int movieId))
                    {
                        _logger.LogError($"Invalid float value for 'movieid' at row {parser.LineNumber}");
                        continue;
                    }
                    ratingSmall.MovieId = movieId;

                    if (!helpers.TryParseFloat(fields[2], out float ratingValue))
                    {
                        _logger.LogError($"Invalid float value for 'ratingvalue' at row {parser.LineNumber}");
                        continue;
                    }
                    ratingSmall.RatingValue = ratingValue;

                    if (!helpers.TryParseLong(fields[3], out long timestamp))
                    {
                        _logger.LogError($"Invalid float value for 'timestamp' at row {parser.LineNumber}");
                        continue;
                    }
                    ratingSmall.Timestamp = timestamp;

                    ratingsSmall.Add(ratingSmall);
                }
            }

            return ratingsSmall;
        }
        private DataTable ConvertRatingsSmallToDataTable(List<RatingSmall> ratingsSmall)
        {
            DataTable table = new DataTable();

            // Define the columns
            table.Columns.Add("UserId", typeof(int));
            table.Columns.Add("MovieId", typeof(int));
            table.Columns.Add("RatingValue", typeof(float));
            table.Columns.Add("Timestamp", typeof(long));

            // Populate the DataTable from the list
            foreach (var ratingSmall in ratingsSmall)
            {
                table.Rows.Add(ratingSmall.UserId, ratingSmall.MovieId, ratingSmall.RatingValue, ratingSmall.Timestamp);
            }

            return table;
        }  

    }


}
