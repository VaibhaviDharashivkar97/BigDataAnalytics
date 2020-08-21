=========================================================================
Code Description:-
=========================================================================

File Name :- project.R

This R script successfully performs K means clustering and provides a visualization for it. 
It uses the data from the IMDB database created in MySQL using the RMySQL package.

The dbConnect() function used in the script requires the following arguments :-
username :- username of the respective database. (root)
password :- password for the username of the respective database.
host :- hostname. (localhost)
dbname :- name of the database to be accessed.

We have performed K means clustering using the kmeans() function in R using K = 5.
In order to determine the optimal value of K we have used the elbow meathod and have also
provided a visualization for the Within cluster sum of squares (wss).

=========================================================================
File Name :- imdb.java

The source code included along with this submission successfully creates a database, cleans/normalizes 
the data from the files insert this data into the respective tables in the database.

It requires the following arguments in the respective order:-
- url:- String representation of the url to the database which is to be connected. (jdbc:mysql://localhost:3306/)
- username:- username of the respective database. (root)
- password:- password for the username of the respective database.
- fPath:- String representation of the location ending with the name of the folder that contains the IMDb data file.
(fName is the name of the file used for inserting data and is provided in the respective functions.)

It contains the following function :-
- connectNcreateDb() :- It establishes the connection and creates the database.
- createTable() :- Creates all the required tables.
- insertPeople() :-  Inserts data into the PEOPLE table.
- insertMovieAndTv() :- Inserts data into the MOVIE_TV table.
- updateMTV() :- Eliminates the records from the MOVIE_TV table that do not satisfy the conditions stated.
- insertActInfo() :- Uses the PEOPLE and MOVIE_TV tables for inserting the PersonId and TitleId of actors into the ACTED_IN table. 
- insertDirInfo() :- Uses the PEOPLE and MOVIE_TV tables for inserting the PersonId and TitleId of directors into the DIRECTED table. 
- insertProdInfo() :- Uses the PEOPLE and MOVIE_TV tables for inserting the PersonId and TitleId of producers into the PRODUCED table. 
- insertWriInfo() :- Uses the PEOPLE and MOVIE_TV tables for inserting the PersonId and TitleId of writers into the WRITTEN table. 
- insertGenre() :- Inserts data into the GENRE table.
- insertHaveG() :-  Uses the MOVIE_TV and GENRE table to insert the information about which movie has which genre into the HAVE_GENRES table.
- createInd() :- Creates the index
- dropInd() :- Drops the index.
- complexQE() :- Used to fire a query which is written.

=========================================================================
Data Description:-
=========================================================================

The IMDb dataset contains five files namely:-
- name.basics.tsv.gz
- title.basics.tsv.gz
- title.principals.tsv.gz
- title.rating.tsv.gz
- title.akas.tsv.gz
From the files listed above, we will be using all the files except the title.akas.tsv.gz since 
it does not hold any significant information to be utilised for this project.

After filtering out all the inconsistencies, redundant data and performing normalization we have 
created the following tables:-

______________________________________________
	Table Name		|		Column Names
____________________|_________________________
-	PEOPLE			|-	PersonId (Primary Key)
					|-	Name
					|-	BirthYear
					|-	DeathYear
____________________|_________________________
-	MOVIE_TV		|-	TitleId (Primary Key)
					|-	Name
					|-	TitleType
					|-	StartYear
					|-	EndYear
					|-	TotalRunTime
					|-	AvgRating
					|-	NoOfVotes
____________________|_________________________
-	GENRES			|-	GenreId (Primary Key)
					|-	Name
____________________|_________________________
-	HAVE_GENRES		|-	GenreId
					|-	TitleId	
____________________|_________________________
-	ACTED_IN		|-	PersonId
					|-	TitleId	 
____________________|_________________________
-	DIRECTED		|-	PersonId
					|-	TitleId	
____________________|_________________________
-	PRODUCED		|-	PersonId
					|-	TitleId	
____________________|_________________________
-	WRITTEN			|-	PersonId
					|-	TitleId	
____________________|_________________________

A sample subset of these tables is provided in the "sample subset" folder along with this submission. (These files are in .xml format)

Considerations:-
The title.basics.tsv.gz file is used for data insertion into the MOVIE_TV table and the title.rating.tsv.gz file is used for updating the table.
We are filtering the data by not considering those records that do not have any value for the genre and runtime in minutes columns in the title.basics.tsv.gz file.
We are considering the title types movie and tvMovie as Movie and tvMiniSeries, tvSeries, tvSpecial as Tv-Show.
After this is done, we are using the title.rating.tsv.gz file to update the AvgRating and NoOfVotes in our MOVIE_TV table by mapping those values using the
tcont(after converting this alphanumeric value into an integer) from the file to the TitleId present in our table.
We further clean the data by eliminating those records that have AvgRating and NoOfVotes as 0.
For populating the PEOPLE table we use name.basics.tsv.gz. From this file, the records that do not have values for birthYear, primary profession and known for titles
are eliminated and the remaining data is used for insertion.
The ACTED_IN table contains the PersonId of the actor/actress and the TitleId of the movie/tv show in which they have contributed.
Similarly, DIRECTED, PRODUCED and WRITTEN tables contain this information of the individuals with the respective profession.
For these tables, we used the file title.principals.tsv.gz.
All the tables are created while keeping track of the values in the PersonId and TitleId in the PEOPLE and the MOVIE_TV table respectively.
GENRES table contains only the unique genre names from the title.basics.tsv.gz file and HAVE_GENRES contains the TitleId and their respective GenreId.
