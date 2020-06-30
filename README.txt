Files used: name.basics.tsv.gz, title.basics.tsv.gz, title.principals.tsv.gz, title.rating.tsv.gz
Names of the tables created: PEOPLE, MOVIE_TV, GENRES, ACTED_IN, DIRECTED, WRITTEN, PRODUCED, HAVE_GENRES

Current progress:-
Using JDBC we have successfully established a connection to the MySQL database service and have created all the tables required for this project.
The data for all these tables have been successfully inserted, the source code for which is included along with this submission.
The file title.akas.tsv.gz was not used for any data insertion as it held no significance to the data required for our recommendation system.
While inserting the data into the tables we had made some assumptions namely,
In the file title.basics.tsv.gz, the titleTypes "short","movie" and "tvMovie" are all considered as TitleType "Movie" and the titleTypes 
"tvEpisode","tvMiniSeries","tvSeries","tvSpecial" and "tvShort" are all considered as TitleType "Tv-Show" for the "MOVIE_TV" Table.
For "ACTED_IN" Table, the PersonId and TitleId include the ncosnt and tconst information for the categories "actor", "actress" and "self" from the file title.principals.tsv.gz.


Future work :-
We will be creating indexes in certain tables in order to reduce the amount of time required for a query to fetch the data.
Next steps in the project will involve focus on normalizing the data and finally clustering (using R) based on certain 
conditions such as specific genres or movies/tv shows with number of votes or ratings higher than a specific value.
