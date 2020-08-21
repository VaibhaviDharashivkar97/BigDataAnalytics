#Group 2
#Members :-
#Udit Wasan
#Varsha Venkatachalam
#Vaibhavi Dharashivkar

#Installing the required packages.
install.packages("RMySQL")
install.packages("stats")
install.packages("dplyr")

#Importing all the packages previously installed.
library("RMySQL")
library("stats")
library("dplyr")

#Establish a connection and access MySQL.
my.db.connect <- dbConnect(RMySQL::MySQL(), username = "root", password = "", host = "localhost", dbname = "imdb")

#Display the names of the tables in the respective database.
dbListTables(my.db.connect)

#Executing the given queries and storing the results in the respective data frame.
people.df <- dbGetQuery(my.db.connect, "SELECT * FROM people")
movie.tv.df <- dbGetQuery(my.db.connect, "SELECT * FROM movie_tv")
genres.df <- dbGetQuery(my.db.connect, "SELECT * FROM genres")

#Display the first six entries in each data frame.
head(people.df)
head(movie.tv.df)
head(genres.df)

#Storing the last 2 columns of movie.tv.df into a new data structure.
new.data = select(movie.tv.df,c(7,8))

#Determines the type of the data structure used.
typeof(new.data)

#Calculating mean and standard deviation using new.data.
mean.val <- apply(new.data,2,mean)
sd.val <- apply(new.data,2,sd)

#Scaling new.data using mean and sd values and storing the result in a new variable.
updated.data <- scale(new.data,mean.val,sd.val)

#Performing k means clustering using kmeans() and displaying the properties/components.
cluster.data <- kmeans(updated.data,5)
cluster.data

#Display cluster plot.
plot(NoOfVotes ~ AvgRatings ,new.data, col = cluster.data$cluster )

#Display properties/components of all 1 to 10 clusters.
for(i in 1:10){
  cluster.data <- kmeans(updated.data,i)
  print(cluster.data)
}

#Elbow method implementation to determine optimal number of clusters.
tw.vals <- c()
num.range <- 1:10
for(i in num.range){
  tw.vals[i] <- kmeans(updated.data,i)$tot.withinss
}
plot(num.range,tw.vals, type = "b",main = "Elbow Method Graph", xlab = "Number of clusters" , ylab = "Within Cluster sum of squares")

#Terminating the connection.
dbDisconnect(my.db.connect)