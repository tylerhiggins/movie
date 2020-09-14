SELECT * FROM ( SELECT movie.title FROM
movies.movie INNER JOIN movies.movies_genres
ON movies_genres.movie_id = movie.movie_id, movies.genres
WHERE movies_genres.genre_id = genres.genre_id AND
genres.genre_name IN ('Science Fiction', 'Mystery')
GROUP BY movie.title HAVING COUNT(movie.title) > 1) AS A
NATURAL JOIN (SELECT genres.genre_name FROM movies.genres) AS B