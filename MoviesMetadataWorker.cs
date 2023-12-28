using Microsoft.Data.SqlClient;
using Microsoft.VisualBasic.FileIO;
using RecomendationSystemWorkerService.Models.Helpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RecomendationSystemWorkerService
{
    internal class MoviesMetadataWorker : BackgroundService
    {
        private readonly ILogger<MoviesMetadataWorker> _logger;
        private readonly string _connectionString;
    

        public MoviesMetadataWorker(
            ILogger<MoviesMetadataWorker> logger,
            IConfiguration configuration)
        {
            _logger = (ILogger<MoviesMetadataWorker>?)logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                var movieMetadataList = ReadMoviesMetadataFromCsv("C:\\Users\\DELL\\Desktop\\rr4444444.csv");
                await BulkInsertMoviesMetadataAsync(movieMetadataList);
                await Task.Delay(10000, stoppingToken); 
            }
        }
        
        //read data from cleaned excel / csv file
        private List<MovieMetadata> ReadMoviesMetadataFromCsv(string filePath)
        {
            var moviesMetadata = new List<MovieMetadata>();

            using (TextFieldParser parser = new TextFieldParser(filePath))
            {
                Helpers formatDatatypes = new Helpers();
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(",");
                parser.HasFieldsEnclosedInQuotes = true;

                // Skip header row
                if (!parser.EndOfData) parser.ReadLine();

                while (!parser.EndOfData)
                {
                    string[] fields = parser.ReadFields();

                    var movie = new MovieMetadata();
                    if (!formatDatatypes.TryParseBool(fields[0], out bool adult))
                    {
                        _logger.LogError($"Invalid boolean value for 'Adult' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    movie.Adult = adult;

                    if (!formatDatatypes.TryParseLong(fields[1], out long budget))
                    {
                        _logger.LogError($"Invalid long value for 'Budget' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    movie.Budget = budget;

                    movie.Homepage = fields[2];

                    if (!formatDatatypes.TryParseInt(fields[3], out int movieId))
                    {
                        _logger.LogError($"Invalid int value for 'MovieId' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    movie.MovieId = movieId;

                    movie.ImdbId = fields[4];
                    movie.OriginalLanguage = fields[5];
                    movie.OriginalTitle = fields[6];
                    movie.Overview = fields[7];

                    if (!formatDatatypes.TryParseFloat(fields[8], out float popularity))
                    {
                        _logger.LogError($"Invalid float value for 'Popularity' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    movie.Popularity = popularity;

                    movie.PosterPath = fields[9];

                    if (!formatDatatypes.TryParseDateTime(fields[10], out DateTime releaseDate))
                    {
                        _logger.LogError($"Invalid DateTime value for 'ReleaseDate' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    movie.ReleaseDate = releaseDate;

                    if (!formatDatatypes.TryParseLong(fields[11], out long revenue))
                    {
                        _logger.LogError($"Invalid long value for 'Revenue' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    movie.Revenue = revenue;

                    if (!formatDatatypes.TryParseIntFromMixedFormat(fields[12], out int runtime))
                    {
                        _logger.LogError($"Invalid int value for 'Runtime' at row {parser.LineNumber}");
                        continue; // Skip this row or handle as needed
                    }
                    movie.Runtime = runtime;


                    movie.Status = fields[13];
                    movie.Tagline = fields[14];
                    movie.Title = fields[15];

                    if (!formatDatatypes.TryParseBool(fields[16], out bool video))
                    {
                        _logger.LogError($"Invalid boolean value for 'Video' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    movie.Video = video;

                    if (!formatDatatypes.TryParseFloat(fields[17], out float voteAverage))
                    {
                        _logger.LogError($"Invalid float value for 'VoteAverage' at row {parser.LineNumber}");
                        continue; // Skip this row
                    }
                    movie.VoteAverage = voteAverage;

                    if (!formatDatatypes.TryParseIntFromMixedFormat(fields[18], out int voteCount))
                    {
                        _logger.LogError($"Invalid int value for 'VoteCount' at row {parser.LineNumber}");
                        continue; // Skip this row or handle as needed
                    }
                    movie.VoteCount = voteCount;

                    moviesMetadata.Add(movie);
                }
            }


            return moviesMetadata;
        }

        private async Task BulkInsertMoviesMetadataAsync(List<MovieMetadata> moviesMetadata)
        {
            DataTable moviesTable = ConvertMoviesToDataTable(moviesMetadata);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "MoviesMetadata"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("adult", "Adult");
                    bulkCopy.ColumnMappings.Add("budget", "Budget");
                    bulkCopy.ColumnMappings.Add("homepage", "Homepage");
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("ImdbId", "ImdbId");
                    bulkCopy.ColumnMappings.Add("OriginalLanguage", "OriginalLanguage");
                    bulkCopy.ColumnMappings.Add("OriginalTitle", "OriginalTitle");
                    bulkCopy.ColumnMappings.Add("overview", "Overview");
                    bulkCopy.ColumnMappings.Add("popularity", "Popularity");
                    bulkCopy.ColumnMappings.Add("PosterPath", "PosterPath");
                    bulkCopy.ColumnMappings.Add("ReleaseDate", "ReleaseDate");
                    bulkCopy.ColumnMappings.Add("revenue", "Revenue");
                    bulkCopy.ColumnMappings.Add("runtime", "Runtime");
                    bulkCopy.ColumnMappings.Add("status", "Status");
                    bulkCopy.ColumnMappings.Add("tagline", "Tagline");
                    bulkCopy.ColumnMappings.Add("title", "Title");
                    bulkCopy.ColumnMappings.Add("video", "Video");
                    bulkCopy.ColumnMappings.Add("VoteAverage", "VoteAverage");
                    bulkCopy.ColumnMappings.Add("VoteCount", "VoteCount");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(moviesTable);
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
        private DataTable ConvertMoviesToDataTable(List<MovieMetadata> moviesMetadata)
        {

            DataTable dataTable = new DataTable();

            // Define the columns in the DataTable to match the structure of the MoviesMetadata table
            dataTable.Columns.Add("Adult", typeof(bool));
            dataTable.Columns.Add("Budget", typeof(long));
            dataTable.Columns.Add("Homepage", typeof(string));
            dataTable.Columns.Add("MovieId", typeof(int));
            dataTable.Columns.Add("ImdbId", typeof(string));
            dataTable.Columns.Add("OriginalLanguage", typeof(string));
            dataTable.Columns.Add("OriginalTitle", typeof(string));
            dataTable.Columns.Add("Overview", typeof(string));
            dataTable.Columns.Add("Popularity", typeof(float));
            dataTable.Columns.Add("PosterPath", typeof(string));
            dataTable.Columns.Add("ReleaseDate", typeof(DateTime));
            dataTable.Columns.Add("Revenue", typeof(long));
            dataTable.Columns.Add("Runtime", typeof(int));
            dataTable.Columns.Add("Status", typeof(string));
            dataTable.Columns.Add("Tagline", typeof(string));
            dataTable.Columns.Add("Title", typeof(string));
            dataTable.Columns.Add("Video", typeof(bool));
            dataTable.Columns.Add("VoteAverage", typeof(float));
            dataTable.Columns.Add("VoteCount", typeof(int));

            // Populate the DataTable with data from the MovieMetadata objects
            foreach (var movie in moviesMetadata)
            {
                var row = dataTable.NewRow();

                row["Adult"] = movie.Adult;
                row["Budget"] = movie.Budget;
                row["Homepage"] = movie.Homepage ?? string.Empty; // Use null-coalescing for nullable fields
                row["MovieId"] = movie.MovieId;
                row["ImdbId"] = movie.ImdbId ?? string.Empty;
                row["OriginalLanguage"] = movie.OriginalLanguage ?? string.Empty;
                row["OriginalTitle"] = movie.OriginalTitle ?? string.Empty;
                row["Overview"] = movie.Overview ?? string.Empty;
                row["Popularity"] = movie.Popularity;
                row["PosterPath"] = movie.PosterPath ?? string.Empty;
                row["ReleaseDate"] = movie.ReleaseDate;
                row["Revenue"] = movie.Revenue;
                row["Runtime"] = movie.Runtime;
                row["Status"] = movie.Status ?? string.Empty;
                row["Tagline"] = movie.Tagline ?? string.Empty;
                row["Title"] = movie.Title ?? string.Empty;
                row["Video"] = movie.Video;
                row["VoteAverage"] = movie.VoteAverage;
                row["VoteCount"] = movie.VoteCount;

                dataTable.Rows.Add(row);
            }

            return dataTable;
        }
   
    }
}
