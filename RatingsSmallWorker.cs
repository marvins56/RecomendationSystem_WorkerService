
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

namespace RecomendationSystemWorkerService
{
    public class ExcelToDbTransfer : BackgroundService
    {
        private readonly ILogger<ExcelToDbTransfer> _logger;
        private readonly string _connectionString;

        public ExcelToDbTransfer(
            ILogger<ExcelToDbTransfer> logger,
            IConfiguration configuration)
        {
            _logger = logger;
            _connectionString = configuration.GetConnectionString("DefaultConnection");
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
          

                var lang = ReadSpokenLanguagesFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_spoken_languages.csv");
                await BulkInsertSpokenLanguagesAsync(lang);

                var ratingsmall = ReadRatingsSmallFromCsv("E:\\Marvin\\Movies_final-exam\\CleanedData\\cleaned_ratings_small.csv");
                await BulkInsertRatingsSmallAsync(ratingsmall);

                await Task.Delay(10000, stoppingToken); // Adjust the delay as needed
            }
        }

        private List<SpokenLanguage> ReadSpokenLanguagesFromCsv(string filePath)
        {
            var spokenLanguages = new List<SpokenLanguage>();

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

                    var spokenLanguage = new SpokenLanguage();

                    // Parse and set the properties here
                    if (!TryParseInt(fields[0], out int movieId))
                    {
                        _logger.LogError($"Invalid float value for 'movieId' at row {parser.LineNumber}");
                        continue;
                    }
                    spokenLanguage.MovieId = movieId;

                    spokenLanguage.SpokenLanguagesId = fields[1];
                    spokenLanguage.Name = fields[2];

                    spokenLanguages.Add(spokenLanguage);
                }
            }

            return spokenLanguages;
        }
        private List<RatingSmall> ReadRatingsSmallFromCsv(string filePath)
        {
            var ratingsSmall = new List<RatingSmall>();

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

                    var ratingSmall = new RatingSmall();

                    // Parse and set the properties here
                    if (!TryParseInt(fields[0], out int userId))
                    {
                        _logger.LogError($"Invalid float value for 'userId' at row {parser.LineNumber}");
                        continue;
                    }
                    ratingSmall.UserId = userId;

                    if (!TryParseInt(fields[1], out int movieId))
                    {
                        _logger.LogError($"Invalid float value for 'movieid' at row {parser.LineNumber}");
                        continue;
                    }
                    ratingSmall.MovieId = movieId;

                    if (!TryParseFloat(fields[2], out float ratingValue))
                    {
                        _logger.LogError($"Invalid float value for 'ratingvalue' at row {parser.LineNumber}");
                        continue;
                    }
                    ratingSmall.RatingValue = ratingValue;

                    if (!TryParseLong(fields[3], out long timestamp))
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
        private List<Rating> ReadRatingsFromCsv(string filePath)
        {
            var ratings = new List<Rating>();

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

                    var rating = new Rating();

                    // Parse and set the properties here
                    if (!TryParseInt(fields[0], out int userId))
                    {
                        _logger.LogError($"Invalid float value for 'userid' at row {parser.LineNumber}");
                        continue;
                    }
                    rating.UserId = userId;

                    if (!TryParseInt(fields[1], out int movieId))
                    {
                        _logger.LogError($"Invalid float value for 'MovieId' at row {parser.LineNumber}");
                        continue;
                    }
                    rating.MovieId = movieId;

                    if (!TryParseFloat(fields[2], out float ratingValue))
                    {
                        _logger.LogError($"Invalid float value for 'ratingvalue' at row {parser.LineNumber}");
                        continue;
                    }
                    rating.RatingValue = ratingValue;

                    if (!TryParseLong(fields[3], out long timestamp))
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



        private async Task BulkInsertSpokenLanguagesAsync(List<SpokenLanguage> spokenLanguages)
        {
            DataTable spokenLanguagesTable = ConvertSpokenLanguagesToDataTable(spokenLanguages);

            using (var connection = new SqlConnection(_connectionString))
            {
                await connection.OpenAsync();
                using (var transaction = connection.BeginTransaction())
                using (var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = "SpokenLanguages"; // Set the destination table name

                    // Map columns
                    bulkCopy.ColumnMappings.Add("MovieId", "MovieId");
                    bulkCopy.ColumnMappings.Add("SpokenLanguagesId", "SpokenLanguagesId");
                    bulkCopy.ColumnMappings.Add("Name", "Name");

                    try
                    {
                        await bulkCopy.WriteToServerAsync(spokenLanguagesTable);
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


  
        private DataTable ConvertSpokenLanguagesToDataTable(List<SpokenLanguage> spokenLanguages)
        {
            DataTable table = new DataTable();

            // Define the columns
            table.Columns.Add("MovieId", typeof(int));
            table.Columns.Add("SpokenLanguagesId", typeof(string));
            table.Columns.Add("Name", typeof(string));

            // Populate the DataTable from the list
            foreach (var spokenLanguage in spokenLanguages)
            {
                table.Rows.Add(spokenLanguage.MovieId, spokenLanguage.SpokenLanguagesId, spokenLanguage.Name);
            }

            return table;
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

        private bool TryParseInt(string value, out int result)
        {
            // First, attempt to parse the value directly as an integer.
            if (int.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result))
            {
                return true;
            }

            // If direct parsing fails, try to parse as a float and then convert to int.
            if (float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out float floatValue))
            {
                // Here, you can choose to round or truncate.
                result = (int)Math.Round(floatValue); // Use Math.Round to round to the nearest integer.
                return true;
            }

            // If both attempts fail, set the result to a default value and return false.
            result = 0; // Default value if parsing is unsuccessful.
            return false;
        }
        private bool TryParseLong(string value, out long result)
        {
            return long.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result);
        }
        private bool TryParseFloat(string value, out float result)
        {
            return float.TryParse(value, NumberStyles.Float, CultureInfo.InvariantCulture, out result);
        }
        private bool TryParseBool(string value, out bool result)
        {
            // Define what you consider to be true or false in your data
            var trueValues = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "true", "yes", "1", "t" };
            var falseValues = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "false", "no", "0", "f" };

            if (trueValues.Contains(value))
            {
                result = true;
                return true;
            }
            else if (falseValues.Contains(value))
            {
                result = false;
                return true;
            }
            else
            {
                _logger.LogError($"Invalid boolean value: '{value}'");
                result = false; // Decide on a default value for your scenario
                return false;
            }
        }
        private bool TryParseDateTime(string value, out DateTime result)
        {
            // Handle ISO 8601 date format (yyyy-MM-dd) and year-only format
            string[] formats = { "yyyy-MM-dd", "yyyy" };
            return DateTime.TryParseExact(value, formats, CultureInfo.InvariantCulture, DateTimeStyles.None, out result);
        }
        private bool TryParseIntFromMixedFormat(string value, out int result)
        {
            // First, attempt to parse the value directly as an integer.
            if (int.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out result))
            {
                return true;
            }

            // If direct parsing fails, try to parse as a float and then convert to int.
            if (float.TryParse(value, NumberStyles.Any, CultureInfo.InvariantCulture, out float floatValue))
            {
                // Here, you can choose to round or truncate.
                result = (int)Math.Round(floatValue); // Use Math.Round to round to the nearest integer.
                return true;
            }

            // If both attempts fail, set the result to a default value and return false.
            result = 0; // Default value if parsing is unsuccessful.
            return false;
        }

    }

    // Define a class that mirrors the structure of your MoviesMetadata table
    public class MovieMetadata
    {
        public bool Adult { get; set; }
        public long Budget { get; set; }
        public string Homepage { get; set; }
        public int MovieId { get; set; }
        public string ImdbId { get; set; }
        public string OriginalLanguage { get; set; }
        public string OriginalTitle { get; set; }
        public string Overview { get; set; }
        public double Popularity { get; set; }
        public string PosterPath { get; set; }
        public DateTime ReleaseDate { get; set; }
        public long Revenue { get; set; }
        public int Runtime { get; set; }
        public string Status { get; set; }
        public string Tagline { get; set; }
        public string Title { get; set; }
        public bool Video { get; set; }
        public double VoteAverage { get; set; }
        public double VoteCount { get; set; }

    }
    public class CreditsCrew
    {
        public string CreditId { get; set; }
        public string Department { get; set; }
        public int Gender { get; set; }
        public string Job { get; set; }
        public string Name { get; set; }
        public string ProfilePath { get; set; }
        public int CrewId { get; set; }
        public int MovieId { get; set; }
    }
    public class ExpandedKeyword
    {
        public int MovieId { get; set; }
        public int KeywordId { get; set; }
        public string Name { get; set; }
    }
    public class Genre
    {
        public int MovieId { get; set; }
        public int GenreId { get; set; }
        public string Name { get; set; }
    }
    public class MovieLink
    {
        public float MovieId { get; set; }
        public string ImdbId { get; set; }
        public int TmdbId { get; set; }
    }
    public class MovieLinkSmall
    {
        public int MovieId { get; set; }
        public int ImdbId { get; set; }
        public int TmdbId { get; set; }
    }
    public class ProductionCompany
    {
        public int MovieId { get; set; }
        public int ProductionCompaniesId { get; set; }
        public string Name { get; set; }
    }
    public class ProductionCountry
    {
        public int MovieId { get; set; }
        public string ProductionCountriesId { get; set; }
        public string Name { get; set; }
    }
    public class Rating
    {
        public int UserId { get; set; }
        public int MovieId { get; set; }
        public float RatingValue { get; set; }
        public long Timestamp { get; set; }
    }
    public class SpokenLanguage
    {
        public int MovieId { get; set; }
        public string SpokenLanguagesId { get; set; }
        public string Name { get; set; }
    }
    public class RatingSmall
    {
        public int UserId { get; set; }
        public int MovieId { get; set; }
        public float RatingValue { get; set; }
        public long Timestamp { get; set; }
    }
}
