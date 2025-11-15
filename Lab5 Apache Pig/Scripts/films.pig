%default FILMS '/user/root/input/films_tsv/films.tsv';
%default RATINGS '/user/root/input/films_tsv/ratings.tsv';
%default OUTPUT '/user/root/pigout/films';

-- Chargement TSV issus d'une conversion JSON préalable
films_raw = LOAD '$FILMS' USING PigStorage('\t') AS (
    film_id:int,
    titre:chararray,
    genres:chararray  -- "Action|Comedy|..."
);

ratings_raw = LOAD '$RATINGS' USING PigStorage('\t') AS (
    usr_id:int,
    nom:chararray,
    sexe:chararray,
    age:int,
    job:chararray,
    film_id:int,
    note:int,
    ts:long
);

-- Explosion des genres : une ligne par genre
genres_exp = FOREACH films_raw GENERATE
    film_id,
    titre,
    FLATTEN(TOKENIZE(REPLACE(genres,'\\|',' '))) AS genre:chararray;
genres_valid = FILTER genres_exp BY genre IS NOT NULL AND SIZE(genre)>0;

-- Statistiques par film (moyenne / volume d'avis)
grp_film = GROUP ratings_raw BY film_id;
film_stats = FOREACH grp_film GENERATE group AS film_id, AVG(ratings_raw.note) AS note_moy, COUNT(ratings_raw) AS nb_notes;
join_top = JOIN film_stats BY film_id, films_raw BY film_id;
films_rank_base = FOREACH join_top GENERATE films_raw::film_id AS film_id,
                                                                                 films_raw::titre AS titre,
                                                                                 film_stats::note_moy AS note_moy,
                                                                                 film_stats::nb_notes AS nb_notes;
films_best = ORDER films_rank_base BY note_moy DESC, nb_notes DESC, titre ASC;
STORE films_best INTO '$OUTPUT/top_movies' USING PigStorage('\t');

-- Comptage de films uniques par genre
genre_uniques = DISTINCT (FOREACH genres_valid GENERATE genre, film_id);
grp_genre_films = GROUP genre_uniques BY genre;
genre_stats = FOREACH grp_genre_films GENERATE group AS genre, COUNT(genre_uniques) AS nb_films;
genre_stats_ord = ORDER genre_stats BY nb_films DESC, genre ASC;
STORE genre_stats_ord INTO '$OUTPUT/genre_count' USING PigStorage('\t');

-- Moyenne de note par genre
rating_genre_join = JOIN ratings_raw BY film_id, genres_valid BY film_id;
grp_genre_notes = GROUP rating_genre_join BY genres_valid::genre;
genre_notes = FOREACH grp_genre_notes GENERATE group AS genre,
                                                                                 AVG(rating_genre_join.ratings_raw::note) AS note_moy,
                                                                                 COUNT(rating_genre_join) AS nb_entries;
genre_notes_ord = ORDER genre_notes BY note_moy DESC, nb_entries DESC, genre ASC;
STORE genre_notes_ord INTO '$OUTPUT/genre_avg' USING PigStorage('\t');

-- Moyenne par couple (genre, sexe utilisateur)
grp_genre_sexe = GROUP rating_genre_join BY (genres_valid::genre, ratings_raw::sexe);
genre_sexe_stats = FOREACH grp_genre_sexe GENERATE
    FLATTEN(group) AS (genre, sexe),
    AVG(rating_genre_join.ratings_raw::note) AS note_moy,
    COUNT(rating_genre_join) AS nb_entries;
STORE genre_sexe_stats INTO '$OUTPUT/genre_gender_avg' USING PigStorage('\t');
