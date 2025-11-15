%default FILMS '/user/root/input/films_tsv/films.tsv';
%default RATINGS '/user/root/input/films_tsv/ratings.tsv';
%default OUTPUT '/user/root/pigout/films';

-- Schémas TSV issus d'un pré-traitement JSON Lines (jq)
films = LOAD '$FILMS' USING PigStorage('\t') AS (
    movieid:int,
    title:chararray,
    genres:chararray   -- pipe-separated, e.g., "Action|Comedy"
);

ratings = LOAD '$RATINGS' USING PigStorage('\t') AS (
    user_id:int,
    name:chararray,
    gender:chararray,
    age:int,
    occupation:chararray,
    movieid:int,
    rating:int,
    ts:long
);

-- Une ligne par (movieid, titre, genre)
film_genres = FOREACH films GENERATE
    movieid,
    title,
    FLATTEN(TOKENIZE(REPLACE(genres, '\\|', ' '))) AS genre:chararray;

film_genres_clean = FILTER film_genres BY genre IS NOT NULL AND SIZE(genre) > 0;

-- Meilleurs films par note moyenne et nombre d'avis
r_by_movie = GROUP ratings BY movieid;
movie_stats = FOREACH r_by_movie GENERATE group AS movieid, AVG(ratings.rating) AS avg_rating, COUNT(ratings) AS rating_count;
ms_join = JOIN movie_stats BY movieid, films BY movieid;
top_movies = FOREACH ms_join GENERATE films::movieid AS movieid,
                                         films::title AS title,
                                         movie_stats::avg_rating AS avg_rating,
                                         movie_stats::rating_count AS rating_count;
ordered_top = ORDER top_movies BY avg_rating DESC, rating_count DESC, title ASC;
STORE ordered_top INTO '$OUTPUT/top_movies' USING PigStorage('\t');

-- Nombre de films uniques par genre
distinct_movie_genre = DISTINCT (FOREACH film_genres_clean GENERATE genre, movieid);
by_genre_movies = GROUP distinct_movie_genre BY genre;
genre_count = FOREACH by_genre_movies GENERATE group AS genre, COUNT(distinct_movie_genre) AS movie_count;
ordered_genre_count = ORDER genre_count BY movie_count DESC, genre ASC;
STORE ordered_genre_count INTO '$OUTPUT/genre_count' USING PigStorage('\t');

-- Note moyenne par genre
ratings_with_genre = JOIN ratings BY movieid, film_genres_clean BY movieid;
by_genre_r = GROUP ratings_with_genre BY film_genres_clean::genre;
genre_avg = FOREACH by_genre_r GENERATE group AS genre,
                                      AVG(ratings_with_genre.ratings::rating) AS avg_rating,
                                      COUNT(ratings_with_genre) AS rating_count;
ordered_genre_avg = ORDER genre_avg BY avg_rating DESC, rating_count DESC, genre ASC;
STORE ordered_genre_avg INTO '$OUTPUT/genre_avg' USING PigStorage('\t');

-- Note moyenne par (genre, sexe)
by_genre_gender = GROUP ratings_with_genre BY (film_genres_clean::genre, ratings::gender);
genre_gender_avg = FOREACH by_genre_gender GENERATE
    FLATTEN(group) AS (genre, gender),
    AVG(ratings_with_genre.ratings::rating) AS avg_rating,
    COUNT(ratings_with_genre) AS rating_count;
STORE genre_gender_avg INTO '$OUTPUT/genre_gender_avg' USING PigStorage('\t');
