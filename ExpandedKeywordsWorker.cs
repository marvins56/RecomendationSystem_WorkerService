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
    internal class ExpandedKeywordsWorker:BackgroundService
    {
        private readonly ILogger<ExpandedKeywordsWorker> _logger;
        private readonly string _connectionString;


        public ExpandedKeywordsWorker(
            ILogger<ExpandedKeywordsWorker> logger,
            IConfiguration configuration)
        {
            _logger = (ILogger<ExpandedKeywordsWorker>?)logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {

                var keywords = ReadExpandedKeywordsFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_expanded_keywords.csv");
                await BulkInsertExpandedKeywordsAsync(keywords);


                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }
        private List<ExpandedKeyword> ReadExpandedKeywordsFromCsv(string filePath)
        {
            var expandedKeywords = new List<ExpandedKeyword>();
            Helpers formatDatatypes = new Helpers();

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

                    var expandedKeyword = new ExpandedKeyword();

                    if (!formatDatatypes.TryParseInt(fields[0], out int movieId))
                    {
                        _logger.LogError($"Invalid int value for 'MovieId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    expandedKeyword.MovieId = movieId;

                    if (!formatDatatypes.TryParseInt(fields[1], out int keywordId))
                    {
                        _logger.LogError($"Invalid int value for 'KeywordId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    expandedKeyword.KeywordId = keywordId;

                    expandedKeyword.Name = fields[2];

                    expandedKeywords.Add(expandedKeyword);
                }
            }

            return expandedKeywords;
        }

        private async Task BulkInsertExpandedKeywordsAsync(List<ExpandedKeyword> expandedKeywords)
        {
            DataTable keywordsTable = ConvertExpandedKeywordsToDataTable(expandedKeywords);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "ExpandedKeywords"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("KeywordId", "KeywordId");
                    bulkCopy.ColumnMappings.Add("Name", "Name");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(keywordsTable);
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
        private DataTable ConvertExpandedKeywordsToDataTable(List<ExpandedKeyword> expandedKeywords)
        {
            DataTable dataTable = new DataTable();

            // Define the columns in the DataTable to match the structure of the ExpandedKeywords table
            dataTable.Columns.Add("MovieId", typeof(int));
            dataTable.Columns.Add("KeywordId", typeof(int));
            dataTable.Columns.Add("Name", typeof(string));

            // Populate the DataTable with data from the ExpandedKeyword objects
            foreach (var keyword in expandedKeywords)
            {
                var row = dataTable.NewRow();

                row["MovieId"] = keyword.MovieId;
                row["KeywordId"] = keyword.KeywordId;
                row["Name"] = keyword.Name ?? string.Empty; // Use null-coalescing for nullable fields

                dataTable.Rows.Add(row);
            }

            return dataTable;
        }

    }
}
