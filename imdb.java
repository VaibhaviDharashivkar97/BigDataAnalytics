import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class imdb {

	static Connection connect = null;
	static PreparedStatement queryStatement = null;
	static String url;
	static String username;
	static String password;
	static String fPath;
	static String fName;
	static int bSize = 1000;

	public static void insertWriInfo() {
		System.out.println("insertWriInfo started");
		PreparedStatement writer = null;
		Set<Integer> tIdSet = new HashSet<>();
		HashMap<Integer, Set<Integer>> writerHmap = new HashMap<>();
		int size = 0;
		fName = fPath + "\\title.principals.tsv.gz";
		try {
			InputStream iStream = new GZIPInputStream(new FileInputStream(fName));
			Scanner scan = new Scanner(iStream, "UTF-8");
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect.setAutoCommit(false);
			scan.nextLine();
			ResultSet result = connect.prepareStatement("SELECT TitleId from Movie_Tv").executeQuery();
			while(result.next()) {
				tIdSet.add(result.getInt("TitleId"));
			}
			writer = connect.prepareStatement("INSERT INTO WRITTEN(PersonId,TitleId)"
					+ "SELECT PEOPLE.PersonId, MOVIE_TV.TitleId FROM PEOPLE,MOVIE_TV "
					+ "WHERE PEOPLE.PersonId = ? AND MOVIE_TV.TitleId = ?");
			while (scan.hasNextLine()) {
				String information[] = scan.nextLine().split("\t");
				int titleId = Integer.parseInt(information[0].substring(2));
				if (tIdSet.contains(titleId)) {
					int personId = Integer.parseInt(information[2].substring(2));
					String profession = information[3];
					if (profession.equals("writer")) {
						if (!writerHmap.containsKey(titleId)) {
							writerHmap.put(titleId, new HashSet<>());
						}
						if (!writerHmap.get(titleId).contains(personId)) {
							writerHmap.get(titleId).add(personId);
							writer.setInt(2, titleId);
							writer.setInt(1, personId);
							writer.addBatch();
						}

					}
					size++;
					if (size % bSize == 0) {
						writer.executeBatch();
						connect.commit();
						writer.clearBatch();
					}
				}
			}
			writer.executeBatch();
			connect.commit();
			scan.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (writer != null) {
					writer.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("insertWriInfo ended");
	}

	public static void insertActInfo() {
		System.out.println("insertActInfo started");
		PreparedStatement actor = null;
		Set<Integer> tIdSet = new HashSet<>();
		HashMap<Integer, Set<Integer>> actorHmap = new HashMap<>();
		int size = 0;
		fName = fPath + "\\title.principals.tsv.gz";
		try {
			InputStream iStream = new GZIPInputStream(new FileInputStream(fName));
			Scanner scan = new Scanner(iStream, "UTF-8");
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect.setAutoCommit(false);
			scan.nextLine();
			ResultSet result = connect.prepareStatement("SELECT TitleId from Movie_Tv").executeQuery();
			while(result.next()) {
				tIdSet.add(result.getInt("TitleId"));
			}
			actor = connect.prepareStatement("INSERT INTO ACTED_IN(PersonId,TitleId)"
					+ "SELECT PEOPLE.PersonId, MOVIE_TV.TitleId FROM PEOPLE,MOVIE_TV "
					+ "WHERE PEOPLE.PersonId = ? AND MOVIE_TV.TitleId = ?");
			while (scan.hasNextLine()) {
				String information[] = scan.nextLine().split("\t");
				int titleId = Integer.parseInt(information[0].substring(2));
				if (tIdSet.contains(titleId)) {
					int personId = Integer.parseInt(information[2].substring(2));
					String profession = information[3];
					if (profession.equals("self") || profession.equals("actor") || profession.equals("actress")) {
						if (!actorHmap.containsKey(titleId)) {
							actorHmap.put(titleId, new HashSet<>());
						}
						if (!actorHmap.get(titleId).contains(personId)) {
							actorHmap.get(titleId).add(personId);
							actor.setInt(2, titleId);
							actor.setInt(1, personId);
							actor.addBatch();
						}
					}
					size++;
					if (size % bSize == 0) {
						actor.executeBatch();
						connect.commit();
						actor.clearBatch();
					}
				}
			}

			actor.executeBatch();
			connect.commit();

			scan.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (actor != null) {
					actor.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("insertActInfo ended");
	}

	public static void insertProdInfo() {
		System.out.println("insertProdInfo started");
		PreparedStatement producer = null;
		Set<Integer> tIdSet = new HashSet<>();
		HashMap<Integer, Set<Integer>> producerHmap = new HashMap<>();
		int size = 0;
		fName = fPath + "\\title.principals.tsv.gz";
		try {
			InputStream iStream = new GZIPInputStream(new FileInputStream(fName));
			Scanner scan = new Scanner(iStream, "UTF-8");
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect.setAutoCommit(false);
			scan.nextLine();
			ResultSet result = connect.prepareStatement("SELECT TitleId from Movie_Tv").executeQuery();
			while(result.next()) {
				tIdSet.add(result.getInt("TitleId"));
			}
			producer = connect.prepareStatement("INSERT INTO PRODUCED(PersonId,TitleId)"
					+ "SELECT PEOPLE.PersonId, MOVIE_TV.TitleId FROM PEOPLE,MOVIE_TV "
					+ "WHERE PEOPLE.PersonId = ? AND MOVIE_TV.TitleId = ?");
			while (scan.hasNextLine()) {
				String information[] = scan.nextLine().split("\t");
				int titleId = Integer.parseInt(information[0].substring(2));
				if (tIdSet.contains(titleId)) {
					int personId = Integer.parseInt(information[2].substring(2));
					String profession = information[3];
					if (profession.equals("producer")) {
						if (!producerHmap.containsKey(titleId)) {
							producerHmap.put(titleId, new HashSet<>());
						}
						if (!producerHmap.get(titleId).contains(personId)) {
							producerHmap.get(titleId).add(personId);
							producer.setInt(2, titleId);
							producer.setInt(1, personId);
							producer.addBatch();
						}

					}
					size++;
					if (size % bSize == 0) {
						producer.executeBatch();
						connect.commit();
						producer.clearBatch();
					}
				}
			}

			producer.executeBatch();

			connect.commit();

			scan.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (producer != null) {
					producer.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("insertProdInfo ended");
	}

	public static void insertDirInfo() {
		System.out.println("insertDirInfo started");
		PreparedStatement director = null;
		Set<Integer> tIdSet = new HashSet<>();
		HashMap<Integer, Set<Integer>> directorHmap = new HashMap<>();
		int size = 0;
		fName = fPath + "\\title.principals.tsv.gz";
		try {
			InputStream iStream = new GZIPInputStream(new FileInputStream(fName));
			Scanner scan = new Scanner(iStream, "UTF-8");
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect.setAutoCommit(false);
			scan.nextLine();
			ResultSet result = connect.prepareStatement("SELECT TitleId from Movie_Tv").executeQuery();
			while(result.next()) {
				tIdSet.add(result.getInt("TitleId"));
			}
			director = connect.prepareStatement("INSERT INTO DIRECTED(PersonId,TitleId)"
					+ "SELECT PEOPLE.PersonId, MOVIE_TV.TitleId FROM PEOPLE,MOVIE_TV "
					+ "WHERE PEOPLE.PersonId = ? AND MOVIE_TV.TitleId = ?");

			while (scan.hasNextLine()) {
				String information[] = scan.nextLine().split("\t");
				int titleId = Integer.parseInt(information[0].substring(2));
				if (tIdSet.contains(titleId)) {
					int personId = Integer.parseInt(information[2].substring(2));
					String profession = information[3];
					if (profession.equals("director")) {
						if (!directorHmap.containsKey(titleId)) {
							directorHmap.put(titleId, new HashSet<>());
						}
						if (!directorHmap.get(titleId).contains(personId)) {
							directorHmap.get(titleId).add(personId);
							director.setInt(2, titleId);
							director.setInt(1, personId);
							director.addBatch();
						}

					}
					size++;
					if (size % bSize == 0) {
						director.executeBatch();

						connect.commit();
						director.clearBatch();

					}
				}
			}
			director.executeBatch();
			connect.commit();

			scan.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (director != null) {
					director.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("insertDirInfo ended");
	}

	// Function to create indexes
	public static void createInd() {

	}

	// Function to execute 2 complex queries
	public static void complexQE() {

	}

	// Function to insert data into the HAVE_GENRES Table.
	public static void insertHaveG() {
		System.out.println("insertHaveG started");
		int size = 0;
		Set<Integer> tIdSet = new HashSet<>();
		HashMap<String, Integer> gMap = new HashMap<>();
		fName = fPath + "\\title.basics.tsv.gz";
		try {
			InputStream iStream = new GZIPInputStream(new FileInputStream(fName));
			Scanner scan = new Scanner(iStream, "UTF-8");
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect.setAutoCommit(false);
			scan.nextLine();
			ResultSet result = connect.prepareStatement("SELECT TitleId FROM movie_tv").executeQuery();
			while(result.next()) {
				tIdSet.add(result.getInt("TitleId"));
			}
			result = connect.prepareStatement("SELECT * FROM genres").executeQuery();
			while(result.next()) {
				gMap.put(result.getString("Name"),result.getInt("GenreId") );
			}
			queryStatement = connect.prepareStatement("INSERT INTO HAVE_GENRES(GenreId,TitleId)"
					+ "SELECT GENRES.GenreId,MOVIE_TV.TitleId  FROM GENRES,MOVIE_TV "
					+ "WHERE GENRES.GenreId = ? AND MOVIE_TV.TitleId = ?");
			while (scan.hasNextLine()) {
				String information[] = scan.nextLine().split("\t");
				if (tIdSet.contains(Integer.parseInt(information[0].substring(2)))) {
					int ttlId = Integer.parseInt(information[0].substring(2));
					if (information[8].equals("\\N")) {
						queryStatement.setInt(1, 0);
						queryStatement.setInt(2, ttlId);
						queryStatement.addBatch();
						size++;
						if (size % bSize == 0) {
							queryStatement.executeBatch();
							connect.commit();
							queryStatement.clearBatch();
						}
						continue;
					}
					String gNames[] = information[8].split(",");
					for (int index = 0; index < gNames.length; index++) {
						queryStatement.setInt(1, gMap.get(gNames[index]));
						queryStatement.setInt(2, ttlId);
						queryStatement.addBatch();
						size++;
						if (size % bSize == 0) {
							queryStatement.executeBatch();
							connect.commit();
							queryStatement.clearBatch();
						}
					}
				}
			}

			queryStatement.executeBatch();
			connect.commit();
			scan.close();

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (queryStatement != null) {
					queryStatement.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("insertHaveG ended");
	}

	// Function to insert data into the GENRES Table.
	public static void insertGenre() {
		System.out.println("insertGenre started");
		int gId = 1;
		HashMap<String, Integer> gMap = new HashMap<>();
		fName = fPath + "\\title.basics.tsv.gz";
		try {
			InputStream iStream = new GZIPInputStream(new FileInputStream(fName));
			Scanner scan = new Scanner(iStream, "UTF-8");
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect.setAutoCommit(false);
			scan.nextLine();
			queryStatement = connect.prepareStatement("INSERT INTO GENRES(GenreId,Name) VALUES(?,?)");
			while (scan.hasNextLine()) {
				String information[] = scan.nextLine().split("\t");
				if (information[8].equals("\\N")) {
					continue;
				}
				String gNames[] = information[8].split(",");
				for (int index = 0; index < gNames.length; index++) {
					String genreN = gNames[index];
					if (!gMap.containsKey(genreN)) {
						gMap.put(genreN, gId);
						queryStatement.setString(2, genreN);
						queryStatement.setInt(1, gId);
						queryStatement.addBatch();
						gId++;
					}
				}

			}
			queryStatement.executeBatch();
			connect.commit();
			scan.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (queryStatement != null) {
					queryStatement.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("insertGenre ended");
	}

	// Function to insert data into the DIRECTED, WRITTEN, PRODUCED and ACTED_IN
	// TABLES.
	public static void insertRelData() {
		System.out.println("insertRelData started");
		Set<Integer> tIdSet = new HashSet<>();
		PreparedStatement director = null;
		HashMap<Integer, Set<Integer>> directorHmap = new HashMap<>();
		PreparedStatement writer = null;
		HashMap<Integer, Set<Integer>> writerHmap = new HashMap<>();
		PreparedStatement producer = null;
		HashMap<Integer, Set<Integer>> producerHmap = new HashMap<>();
		PreparedStatement actor = null;
		HashMap<Integer, Set<Integer>> actorHmap = new HashMap<>();
		int size = 0;
		fName = fPath + "\\title.principals.tsv.gz";
		try {
			
			InputStream iStream = new GZIPInputStream(new FileInputStream(fName));
			Scanner scan = new Scanner(iStream, "UTF-8");
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect.setAutoCommit(false);
			scan.nextLine();
			ResultSet result = connect.prepareStatement("SELECT TitleId from Movie_Tv").executeQuery();
			while(result.next()) {
				tIdSet.add(result.getInt("TitleId"));
			}
			director = connect.prepareStatement("INSERT INTO DIRECTED(PersonId,TitleId)"
					+ "SELECT PEOPLE.PersonId, MOVIE_TV.TitleId FROM PEOPLE,MOVIE_TV "
					+ "WHERE PEOPLE.PersonId = ? AND MOVIE_TV.TitleId = ?");
			writer = connect.prepareStatement("INSERT INTO WRITTEN(PersonId,TitleId)"
					+ "SELECT PEOPLE.PersonId, MOVIE_TV.TitleId FROM PEOPLE,MOVIE_TV "
					+ "WHERE PEOPLE.PersonId = ? AND MOVIE_TV.TitleId = ?");
			producer = connect.prepareStatement("INSERT INTO PRODUCED(PersonId,TitleId)"
					+ "SELECT PEOPLE.PersonId, MOVIE_TV.TitleId FROM PEOPLE,MOVIE_TV "
					+ "WHERE PEOPLE.PersonId = ? AND MOVIE_TV.TitleId = ?");
			actor = connect.prepareStatement("INSERT INTO ACTED_IN(PersonId,TitleId)"
					+ "SELECT PEOPLE.PersonId, MOVIE_TV.TitleId FROM PEOPLE,MOVIE_TV "
					+ "WHERE PEOPLE.PersonId = ? AND MOVIE_TV.TitleId = ?");
			while (scan.hasNextLine()) {
				String information[] = scan.nextLine().split("\t");
				int titleId = Integer.parseInt(information[0].substring(2));
				if (tIdSet.contains(titleId)) {
					int personId = Integer.parseInt(information[2].substring(2));
					String profession = information[3];
					if (profession.equals("director")) {
						if (!directorHmap.containsKey(titleId)) {
							directorHmap.put(titleId, new HashSet<>());
						}
						if (!directorHmap.get(titleId).contains(personId)) {
							directorHmap.get(titleId).add(personId);
							director.setInt(2, titleId);
							director.setInt(1, personId);
							director.addBatch();
						}

					} else if (profession.equals("writer")) {
						if (!writerHmap.containsKey(titleId)) {
							writerHmap.put(titleId, new HashSet<>());
						}
						if (!writerHmap.get(titleId).contains(personId)) {
							writerHmap.get(titleId).add(personId);
							writer.setInt(2, titleId);
							writer.setInt(1, personId);
							writer.addBatch();
						}

					} else if (profession.equals("producer")) {
						if (!producerHmap.containsKey(titleId)) {
							producerHmap.put(titleId, new HashSet<>());
						}
						if (!producerHmap.get(titleId).contains(personId)) {
							producerHmap.get(titleId).add(personId);
							producer.setInt(2, titleId);
							producer.setInt(1, personId);
							producer.addBatch();
						}

					} else if (profession.equals("self") || profession.equals("actor")
							|| profession.equals("actress")) {
						if (!actorHmap.containsKey(titleId)) {
							actorHmap.put(titleId, new HashSet<>());
						}
						if (!actorHmap.get(titleId).contains(personId)) {
							actorHmap.get(titleId).add(personId);
							actor.setInt(2, titleId);
							actor.setInt(1, personId);
							actor.addBatch();
						}
					}
					size++;
					if (size % bSize == 0) {
						director.executeBatch();
						writer.executeBatch();
						producer.executeBatch();
						actor.executeBatch();
						connect.commit();
						director.clearBatch();
						writer.clearBatch();
						producer.clearBatch();
						actor.clearBatch();
					}
				}
			}
			director.executeBatch();
			writer.executeBatch();
			producer.executeBatch();
			actor.executeBatch();
			connect.commit();

			scan.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (director != null) {
					director.close();
				}
				if (writer != null) {
					writer.close();
				}
				if (producer != null) {
					producer.close();
				}
				if (actor != null) {
					actor.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("insertRelData ended");
	}

	// Function to insert data into the MOVIE_TV Table.
	public static void insertMovieAndTv() {
		System.out.println("insertMovieAndTv started");
		Set<Integer> tIdSet = new HashSet<>();
		int size = 0;
		int size1 = 0;
		String fName1 = fPath + "\\title.ratings.tsv.gz";
		fName = fPath + "\\title.basics.tsv.gz";
		try {
			InputStream iStream = new GZIPInputStream(new FileInputStream(fName));
			Scanner scan = new Scanner(iStream, "UTF-8");
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect.setAutoCommit(false);
			scan.nextLine();
			queryStatement = connect.prepareStatement(
					"INSERT INTO MOVIE_TV(TitleId,Name,TitleType,StartYear,EndYear,TotalRunTime,AvgRatings,NoOfVotes) VALUES(?,?,?,?,?,?,?,?)");
			while (scan.hasNextLine()) {
				String information[] = scan.nextLine().split("\t");
				int tId = Integer.parseInt(information[0].substring(2));
				int sYear;
				if (information[5].equals("\\N")) {
					sYear = 0;
				} else {
					sYear = Integer.parseInt(information[5]);
				}
				int eYear;
				if (information[6].equals("\\N")) {
					eYear = 0;
				} else {
					eYear = Integer.parseInt(information[6]);
				}
				int ttRun;
				if (information[7].equals("\\N")) {
					ttRun = 0;
				} else {
					ttRun = Integer.parseInt(information[7]);
				}
				queryStatement.setInt(1, tId);
				queryStatement.setInt(4, sYear);
				queryStatement.setInt(5, eYear);
				queryStatement.setInt(6, ttRun);
				queryStatement.setDouble(7, 0);
				queryStatement.setInt(8, 0);
				String tName = information[2];
				queryStatement.setString(2, tName);
				if (information[1].equals("short") || information[1].equals("movie")
						|| information[1].equals("tvMovie")) {
					String tType = "Movie";
					queryStatement.setString(3, tType);
				} else if (information[1].equals("tvEpisode") || information[1].equals("tvMiniSeries")
						|| information[1].equals("tvSeries") || information[1].equals("tvSpecial")
						|| information[1].equals("tvShort")) {
					String tType = "Tv-Show";
					queryStatement.setString(3, tType);
				} else {
					continue;
				}
				queryStatement.addBatch();
				size++;
				if (size % bSize == 0) {
					queryStatement.executeBatch();
					connect.commit();
					queryStatement.clearBatch();
				}
			}
			queryStatement.executeBatch();
			connect.commit();
			scan.close();
			InputStream iStream1 = new GZIPInputStream(new FileInputStream(fName1));
			Scanner scan1 = new Scanner(iStream1, "UTF-8");
			scan1.nextLine();
			ResultSet result = connect.prepareStatement("SELECT TitleId from Movie_Tv").executeQuery();
			while(result.next()) {
				tIdSet.add(result.getInt("TitleId"));
			}
			queryStatement = connect
					.prepareStatement("UPDATE MOVIE_TV SET AvgRatings = ?, NoOfVotes = ? WHERE TitleId = ?");
			while (scan1.hasNextLine()) {
				String information1[] = scan1.nextLine().split("\t");
				double avgR = Double.parseDouble(information1[1]);
				int noOfVote = Integer.parseInt(information1[2]);
				int tId = Integer.parseInt(information1[0].substring(2));
				if (tIdSet.contains(tId)) {
					queryStatement.setDouble(1, avgR);
					queryStatement.setInt(2, noOfVote);
					queryStatement.setInt(3, tId);
				} else {
					continue;
				}
				queryStatement.addBatch();
				size1++;
				if (size1 % bSize == 0) {
					queryStatement.executeBatch();
					connect.commit();
					queryStatement.clearBatch();
				}
			}
			queryStatement.executeBatch();
			connect.commit();
			scan1.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (queryStatement != null) {
					queryStatement.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("insertMovieAndTv ended");
	}

	// Function to insert data into the PEOPLE Table.
	public static void insertPeople() {
		System.out.println("insertPeople started");
		int size = 0;
		fName = fPath + "\\name.basics.tsv.gz";
		try {
			InputStream iStream = new GZIPInputStream(new FileInputStream(fName));
			Scanner scan = new Scanner(iStream, "UTF-8");
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			connect.setAutoCommit(false);
			scan.nextLine();
			queryStatement = connect
					.prepareStatement("INSERT INTO PEOPLE(PersonId,Name,BirthYear,DeathYear) VALUES(?,?,?,?)");
			while (scan.hasNextLine()) {
				String information[] = scan.nextLine().split("\t");
				int bYear;
				int dYear;
				if (information[2].equals("\\N")) {
					bYear = 0;
				} else {
					bYear = Integer.parseInt(information[2]);
				}
				if (information[3].equals("\\N")) {
					dYear = 0;
				} else {
					dYear = Integer.parseInt(information[3]);
				}
				queryStatement.setInt(3, bYear);
				queryStatement.setInt(4, dYear);
				int prsnId = Integer.parseInt(information[0].substring(2));
				queryStatement.setInt(1, prsnId);
				String pName = information[1];
				queryStatement.setString(2, pName);
				queryStatement.addBatch();
				size++;
				if (size % bSize == 0) {
					queryStatement.executeBatch();
					connect.commit();
					queryStatement.clearBatch();
				}
			}
			queryStatement.executeBatch();
			connect.commit();
			scan.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (queryStatement != null) {
					queryStatement.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("insertPeople ended");
	}

	// Function to create all the required tables.
	public static void createTable() {
		System.out.println("createTable started");
		try {
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			queryStatement = connect
					.prepareStatement("CREATE TABLE IF NOT EXISTS PEOPLE(\r\n" + "PersonId INT PRIMARY KEY,\r\n"
							+ "Name VARCHAR(120),\r\n" + "BirthYear INT,\r\n" + "DeathYear INT\r\n" + ");");
			queryStatement.executeUpdate();
			queryStatement = connect.prepareStatement("CREATE TABLE IF NOT EXISTS MOVIE_TV(\r\n"
					+ "TitleId INT PRIMARY KEY,\r\n" + "Name VARCHAR(420),\r\n" + "TitleType VARCHAR(30),\r\n"
					+ "StartYear INT,\r\n" + "EndYear INT,\r\n" + "TotalRunTime INT,\r\n" + "AvgRatings FLOAT,\r\n"
					+ "NoOfVotes INT\r\n" + ");");
			queryStatement.executeUpdate();
			queryStatement = connect.prepareStatement("CREATE TABLE IF NOT EXISTS GENRES(\r\n"
					+ "GenreId INT PRIMARY KEY,\r\n" + "Name VARCHAR(30)\r\n" + ");");
			queryStatement.executeUpdate();
			queryStatement = connect.prepareStatement("CREATE TABLE IF NOT EXISTS ACTED_IN(\r\n" + "PersonId INT,\r\n"
					+ "TitleId INT,\r\n" + "PRIMARY KEY(PersonId,TitleId),\r\n"
					+ "FOREIGN KEY(PersonId) REFERENCES PEOPLE(PersonId),\r\n"
					+ "FOREIGN KEY(TitleId) REFERENCES MOVIE_TV(TitleId)\r\n" + ");");
			queryStatement.executeUpdate();
			queryStatement = connect.prepareStatement("CREATE TABLE IF NOT EXISTS DIRECTED(\r\n" + "PersonId INT,\r\n"
					+ "TitleId INT,\r\n" + "PRIMARY KEY(PersonId,TitleId),\r\n"
					+ "FOREIGN KEY(PersonId) REFERENCES PEOPLE(PersonId),\r\n"
					+ "FOREIGN KEY(TitleId) REFERENCES MOVIE_TV(TitleId)\r\n" + ");");
			queryStatement.executeUpdate();
			queryStatement = connect.prepareStatement("CREATE TABLE IF NOT EXISTS HAVE_GENRES(\r\n" + "GenreId INT,\r\n"
					+ "TitleId INT,\r\n" + "PRIMARY KEY(GenreId,TitleId),\r\n"
					+ "FOREIGN KEY(GenreId) REFERENCES GENRES(GenreId),\r\n"
					+ "FOREIGN KEY(TitleId) REFERENCES MOVIE_TV(TitleId)\r\n" + ");");
			queryStatement.executeUpdate();
			queryStatement = connect.prepareStatement("CREATE TABLE IF NOT EXISTS PRODUCED(\r\n" + "PersonId INT,\r\n"
					+ "TitleId INT,\r\n" + "PRIMARY KEY(PersonId,TitleId),\r\n"
					+ "FOREIGN KEY(PersonId) REFERENCES PEOPLE(PersonId),\r\n"
					+ "FOREIGN KEY(TitleId) REFERENCES MOVIE_TV(TitleId)\r\n" + ");");
			queryStatement.executeUpdate();
			queryStatement = connect.prepareStatement("CREATE TABLE IF NOT EXISTS WRITTEN(\r\n" + "PersonId INT,\r\n"
					+ "TitleId INT,\r\n" + "PRIMARY KEY(PersonId,TitleId),\r\n"
					+ "FOREIGN KEY(PersonId) REFERENCES PEOPLE(PersonId),\r\n"
					+ "FOREIGN KEY(TitleId) REFERENCES MOVIE_TV(TitleId)\r\n" + ");");
			queryStatement.executeUpdate();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (queryStatement != null) {
					queryStatement.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("createTable ended");
	}

	// Function to establish the connection and create the database.
	public static void connectNcreateDb() {
		System.out.println("connectNcreateDb started");
		try {
			connect = DriverManager.getConnection(url, username, password);
			Class.forName("com.mysql.cj.jdbc.Driver");
			queryStatement = connect.prepareStatement("CREATE DATABASE IMDB");
			queryStatement.executeUpdate();
			connect.close();
			String databaseName = "IMDB";
			url = url + databaseName;
			connect = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (queryStatement != null) {
					queryStatement.close();
				}
				if (connect != null) {
					connect.close();
				}
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
		System.out.println("connectNcreateDb ended");
	}

	public static void main(String[] args) {
		url = "jdbc:mysql://localhost:3306/IMDB";
		username = "root";
		password = "";
		fPath = "";
		connectNcreateDb();
		createTable();
		insertPeople();
		insertMovieAndTv();
//		insertRelData();
		insertActInfo();
		insertDirInfo();
		insertProdInfo();
		insertWriInfo();
		insertGenre();
		insertHaveG();
	}

}
