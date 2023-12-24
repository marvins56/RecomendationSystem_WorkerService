using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RecomendationSystemWorkerService.Models.Helpers
{
   
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
