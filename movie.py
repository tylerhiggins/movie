#!/anaconda3/bin/python3

import csv
import sys
import pymysql
from ast import literal_eval
import warnings



def createschema(usern, password):
	""" This function opens a connection to the
	database server on the local machine and attempts to connect
	with the specified username and password provided in the arguments
	then creates the schema, it returns a tuple of the connection
	and the cursor to be stored for further use by other functions. """
	display = 0
	try:
		connection = pymysql.connect(host='localhost', port=3306, user=usern, passwd=password)
	except pymysql.err.OperationalError:
		print("Connection error... Invalid username or password.")
		exit()
	cursor = connection.cursor()
	# Drop (if exists) then create a new movies schema
	with warnings.catch_warnings():
		warnings.simplefilter('ignore')
		cursor.execute('DROP DATABASE IF EXISTS movies')
	cursor.execute('CREATE DATABASE movies')
	connection.commit()
	# create the movie table
	cursor.execute('CREATE TABLE movies.movie ('
		'budget INT, homepage VARCHAR(150), movie_id INT NOT NULL, original_language CHAR(2), '
		'original_title VARCHAR(200), overview VARCHAR(1000), popularity FLOAT, release_date DATE, ' 
		'revenue BIGINT, runtime SMALLINT(3), status VARCHAR(50), tagline VARCHAR(300), title VARCHAR(200), ' 
		'vote_avg FLOAT, vote_count INT, PRIMARY KEY(movie_id))')
	connection.commit()
	
	# create the genre table
	cursor.execute('CREATE TABLE movies.genres ( genre_id SMALLINT(2) NOT NULL, genre_name VARCHAR(50), ' 
		'PRIMARY KEY(genre_id))')
	connection.commit()
	# connect genres with movies
	cursor.execute('CREATE TABLE movies.movies_genres ('
		'movie_id INT NOT NULL, genre_id SMALLINT(2) NOT NULL, genre_name VARCHAR(50),'
		'PRIMARY KEY(movie_id, genre_id), FOREIGN KEY(movie_id) REFERENCES movies.movie(movie_id), '
		'FOREIGN KEY(genre_id) REFERENCES movies.genres(genre_id))')
	connection.commit()
	
	# Keywords table
	cursor.execute('CREATE TABLE movies.keywords ( kw_id INT NOT NULL, kw_name VARCHAR(30), '
		'PRIMARY KEY(kw_id))')
	connection.commit()
	
	# keywords/movies
	cursor.execute('CREATE TABLE movies.movies_keywords ( kw_id INT NOT NULL, '
		'movie_id INT NOT NULL, kw_name VARCHAR(30), PRIMARY KEY(kw_id, movie_id), '
		'FOREIGN KEY(kw_id) REFERENCES movies.keywords(kw_id), FOREIGN KEY(movie_id) REFERENCES '
		'movies.movie(movie_id))')
	connection.commit()

	# create production companites
	cursor.execute('CREATE TABLE movies.prod_company ( prodc_id INT NOT NULL, prodc_name VARCHAR(50), '
		'PRIMARY KEY(prodc_id))')
	connection.commit()

	# prod companies/movies
	cursor.execute('CREATE TABLE movies.prodc_movies ( prodc_id INT NOT NULL, '
		'movie_id INT NOT NULL, prodc_name VARCHAR(50), PRIMARY KEY(prodc_id, movie_id), '
		'FOREIGN KEY(prodc_id) REFERENCES movies.prod_company(prodc_id), '
		'FOREIGN KEY(movie_id) REFERENCES movies.movie(movie_id))')
	connection.commit()
	#production countries
	cursor.execute('CREATE TABLE movies.prod_countries ( iso_3166_1 CHAR(2) NOT NULL, '
		'count_name VARCHAR(50), PRIMARY KEY(iso_3166_1))')
	connection.commit()
	#prodcountries/movies
	cursor.execute('CREATE TABLE movies.prodcount_movies ( iso_3166_1 CHAR(2) NOT NULL, '
		'movie_id INT NOT NULL, count_name VARCHAR(50), PRIMARY KEY(iso_3166_1, movie_id), '
		'FOREIGN KEY(iso_3166_1) REFERENCES movies.prod_countries(iso_3166_1), '
		'FOREIGN KEY(movie_id) REFERENCES movies.movie(movie_id))')
	connection.commit()
	# spoken languages
	cursor.execute('CREATE TABLE movies.spoken_lang ( iso_639_1 CHAR(2) NOT NULL, '
		'lang_name VARCHAR(25), PRIMARY KEY(iso_639_1))')
	connection.commit()
	# spoken/movies
	cursor.execute('CREATE TABLE movies.spoken_movies ( iso_639_1 CHAR(2) NOT NULL, '
		'movie_id INT NOT NULL, lang_name VARCHAR(25), PRIMARY KEY(iso_639_1, movie_id), '
		'FOREIGN KEY(iso_639_1) REFERENCES movies.spoken_lang(iso_639_1), '
		'FOREIGN KEY(movie_id) REFERENCES movies.movie(movie_id))')
	connection.commit()

	return connection, cursor

def fill_genre(conn, cur, genresraw, m_id):
	""" This function creates the genres and the movie_genres table.
	The arguments are the connection to the database, cursor, the raw data pulled
	from the csv file for genre, and the movie_id"""
	# Statement to use to insert into the genres table

	ginsert = "INSERT IGNORE INTO movies.genres(genre_id, genre_name) VALUES (%s, %s)"
	# Statement to use to insert into the movies_genres table
	mginsert = "INSERT INTO movies.movies_genres (genre_id, movie_id) VALUES (%s, %s)"
	# convert from a string
	genrelist = literal_eval(genresraw)
	# loop through each genre
	for genre in genrelist:					
		# insert the id and the name into the genres table.
		val = [genre["id"], genre["name"]]
		with warnings.catch_warnings():
			warnings.simplefilter('ignore')
			cur.execute(ginsert, val)
		conn.commit()
		# then insert into the movies_genres table.
		val = [genre["id"], m_id]
		cur.execute(mginsert, val)
		conn.commit()

def fill_keywords(conn, cur, kwraw, m_id):
	""" This function inserts items into the keywords and the movies_keywords table.
	The arguments for this function are the connection and cursor of the database, the
	raw keyword data from the csv file, and the movie_id"""
	# Statement to use to insert into the keywords table
	kwinsert = "INSERT IGNORE INTO movies.keywords(kw_id, kw_name) VALUES (%s, %s)"
	# Statement to use to insert into the movies_keywords table
	mkwinsert = "INSERT INTO movies.movies_keywords (kw_id, movie_id) VALUES (%s, %s)"
	# convert from a string
	kwlist = literal_eval(kwraw)
	# loop through each keyword
	for keyword in kwlist:			
		# insert the id and the name into the keywords table.
		val = [keyword["id"], keyword["name"]]
		with warnings.catch_warnings():
			warnings.simplefilter('ignore')
			cur.execute(kwinsert, val)		
		conn.commit()
		# then insert into the movies_keywords table.
		val = [keyword["id"], m_id]
		cur.execute(mkwinsert, val)
		conn.commit()

def fill_prodc(conn, cur, pcraw, m_id):
	""" This function inserts items into the prod_company and the prodc_movies table.
	Arguments are connection and cursor from the database, the production company's raw
	data from the csv file, and the movie_id"""
	# Statement to use to insert into the prod_company table
	pcinsert = "INSERT IGNORE INTO movies.prod_company(prodc_id, prodc_name) VALUES (%s, %s)"
	# Statement to use to insert into the prodc_movies table
	mpcinsert = "INSERT INTO movies.prodc_movies (prodc_id, movie_id) VALUES (%s, %s)"
	# convert from a string
	pclist = literal_eval(pcraw)

	# loop through each production company
	for prodc in pclist:
		# insert the id and the name into the prod_company table.
		val = [prodc["id"], prodc["name"]]
		with warnings.catch_warnings():
			warnings.simplefilter('ignore')
			cur.execute(pcinsert, val)		
		conn.commit()
		# then insert into the prodc_movies table.
		val = [prodc["id"], m_id]
		cur.execute(mpcinsert, val)
		conn.commit()

def fill_prodcount(conn, cur, pcraw, m_id):
	""" This function inserts items into the prod_countries and the prodcount_movies table.
	Arguments for this function include the connection and cursor from the database, the production
	country raw data from the csv file, and the movie_id"""
	# Statement to use to insert into the prod_countries table
	pcinsert = "INSERT IGNORE INTO movies.prod_countries(iso_3166_1, count_name) VALUES (%s, %s)"
	# Statement to use to insert into the prodcount_movies table
	mpcinsert = "INSERT INTO movies.prodcount_movies (iso_3166_1, movie_id) VALUES (%s, %s)"
	# convert from a string
	pclist = literal_eval(pcraw)
	# loop through each production country
	for prodc in pclist:			
		# insert the iso_3166_1 and the name into the prod_country table.
		val = [prodc["iso_3166_1"], prodc["name"]]
		with warnings.catch_warnings():
			warnings.simplefilter('ignore')
			cur.execute(pcinsert, val)		
		conn.commit()
		# then insert into the prodcount_movies table.
		val = [prodc["iso_3166_1"], m_id]
		cur.execute(mpcinsert, val)
		conn.commit()

def fill_lang(conn, cur, langraw, m_id):
	""" This function inserts items into the spoken_languages and the spoken_movies table.
	Arguments are the connection and cursor from the database, the raw spoken language data
	from the csv file, and the movie_id"""
	# Statement to use to insert into the spoken_languages table
	langinsert = "INSERT IGNORE INTO movies.spoken_lang(iso_639_1, lang_name) VALUES (%s, %s)"
	# Statement to use to insert into the spoken_movies table
	mlanginsert = "INSERT INTO movies.spoken_movies(iso_639_1, movie_id) VALUES (%s, %s)"
	# convert from a string
	langlist = literal_eval(langraw)
	# loop through each language
	for language in langlist:			
		# insert the iso_639_1 and the name into the spoken_languages table.
		val = [language["iso_639_1"], language["name"]]
		with warnings.catch_warnings():
			warnings.simplefilter('ignore')
			cur.execute(langinsert, val)		
		conn.commit()
		# then insert into the spoken_movies table.
		val = [language["iso_639_1"], m_id]
		cur.execute(mlanginsert, val)
		conn.commit()

def filltable(conn, cur, dbug):
	""" This function takes the connection and cursor to the database as arguments
	then takes the tmdb_5000_movies.csv file and fills the tables with the appropriate data 
	filltable does not return anything """

	movie = []
	# statement to insert into the movie table/
	exstmt = ("INSERT INTO movies.movie (budget, homepage, movie_id, original_language, " 
	"original_title, overview, popularity, release_date, revenue, runtime, status, " 
	"tagline, title, vote_avg, vote_count) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, " 
	"%s, %s, %s, %s, %s, %s)")
	# opens the csv file in read mode
	file = open('tmdb_5000_movies.csv', 'r')
	# traverse through the file grabbing every movie
	reader = csv.reader(file)
	for row in reader:
		movie.append(row)
	file.close()
	# traverse through the movies (0 is skipped since they are the names of the columns)
	for i in range(1, len(movie)):
		# If theres nothing in the release date or runtime is empty.
		if movie[i][13] == "":
			movie[i][13] = 0
		if movie[i][11] == "":
			movie[i][11] = '2015-03-01'
		# insert the values into the movie table.
		values = [movie[i][0], movie[i][2], movie[i][3], movie[i][5], movie[i][6], movie[i][7], 
			movie[i][8], movie[i][11], movie[i][12], movie[i][13], movie[i][15], movie[i][16], 
			movie[i][17], movie[i][18], movie[i][19]]
		if dbug:
			print(i, " insert statment ", exstmt, values)
		cur.execute(exstmt, values)
		conn.commit()

		if dbug:
			print("going to create_genre")
		# create the genres and movies_genres tables
		fill_genre(conn, cur, movie[i][1], movie[i][3])
		fill_keywords(conn, cur, movie[i][4], movie[i][3])
		fill_prodc(conn, cur, movie[i][9], movie[i][3])
		fill_prodcount(conn, cur, movie[i][10], movie[i][3])
		fill_lang(conn, cur, movie[i][14], movie[i][3])
		if dbug:
			print('inserted row ', i)
	# dump all of the movie entries on success
	if dbug:
		print('Finished movie table')
		cur.execute("SELECT * FROM movies.movie")
		tuples = cur.fetchall()
		conn.commit()
		for t in tuples:
			print(t)

def avgbudget(conn, cur):
	""" This function takes arguments the connection
	to the database and the cursor and fetches the
	average budget of all movies.  Returns the average budget"""
	cur.execute("SELECT AVG(budget) FROM movies.movie")
	avg = cur.fetchall()
	conn.commit()
	return float(avg[0][0])

def produced(conn, cur):
	""" This function takes arguments the connection to the database
	and the cursor, returning the tuple with the movie name and production
	companies that were produced in the US"""
	cur.execute("SELECT DISTINCT title, prodc_name FROM movies.movie, movies.prod_company "
		"NATURAL JOIN movies.prodcount_movies NATURAL JOIN movies.prodc_movies "
		"WHERE movie.movie_id = prodcount_movies.movie_id AND prodcount_movies.iso_3166_1 = 'US' "
		"AND prodc_movies.movie_id = movie.movie_id AND prod_company.prodc_id = prodc_movies.prodc_id")
	result = cur.fetchall()
	return result

def revenue(conn, cur):
	""" This function takes arguments the connection to the database and the
	cursor, returning a tuple of the movie titles and revenues of the top
	5 movies with the most revenue"""
	cur.execute("SELECT title, revenue FROM movies.movie ORDER BY revenue DESC LIMIT 5")
	result = cur.fetchall()
	return result

def genres(conn, cur):
	cur.execute("SELECT * FROM (SELECT movie.title FROM movies.movie INNER JOIN "
		"movies.movies_genres ON movies_genres.movie_id = movie.movie_id, movies.genres "
		"WHERE movies_genres.genre_id = genres.genre_id AND genres.genre_name IN ('Science Fiction', "
		"'Mystery') GROUP BY movie.title HAVING COUNT(movie.title) > 1) AS A")
	result = cur.fetchall()
	return result

def popular(conn, cur):
	"""This function determines the 5 most popular movies."""
	cur.execute("SELECT title, popularity FROM movies.movie ORDER BY popularity DESC")
	result = cur.fetchall()
	return result

# main function
if __name__ == '__main__':
	conn = pymysql.connect(host='localhost', port=3306, user=sys.argv[1], passwd=sys.argv[2])
	cur = conn.cursor()
	query = 0
	if len(sys.argv) > 3:
		query = int(sys.argv[3])
	# used for debugging statments
	debug = False
	# used to manipulate the database
	conn = 0
	cur = 0
	argc = len(sys.argv)
	# if '-d' (debugging mode)
	if (argc == 4 and sys.argv[3] == '-d') or (argc == 5 and sys.argv[4] == '-d'):
		print('Debugging mode.')
		debug = True
		argc -= 1
	# deleting schema option
	elif (argc == 4 and sys.argv[3] == '-delete') or (argc == 5 and sys.argv[4] == '-delete'):
		print('Deleting schema')
		try:
			conn = pymysql.connect(host='localhost', port=3306, user=sys.argv[1], passwd=sys.argv[2])
		except pymysql.err.OperationalError:
			print("Connection error... invalid username or password.")
			exit()
		cur = conn.cursor()
		cur.execute('DROP DATABASE IF EXISTS movies')
		conn.commit()
		cur.close()
		conn.close()
		exit()
	
	if debug:
		print('argc = ', argc)
	query = 0
	# too few arguments error
	if argc < 3:
		print(sys.argv[0], ': No username or password specified, Exiting...')
		exit()
	# too many arguments error
	if argc > 4:
		print(sys.argv[0], ': Too many arguments, Exiting...')
		exit()
	# retrieve the username and password
	user = sys.argv[1]
	password = sys.argv[2]
	if debug:
		print('user = ', user, ' password = ', password)
	#if theres a 4th argument grab the query number.
	if argc == 4:
		query = int(sys.argv[3])
	# connect and create the schema
	sqlControls = createschema(user, password)
	conn = sqlControls[0]
	cur = sqlControls[1]
	# call filltable
	sys.stdout.write("Inserting into schema...")
	sys.stdout.flush()
	filltable(conn, cur, debug)
	sys.stdout.write("done!\n")
	sys.stdout.flush()

	# Average budget
	if query == 1:
		avgbud = avgbudget(conn,cur)
		print("Average budget = ${:,.4f}".format(avgbud))
	# Movies that were produced in the US and all thier production
	# companies
	if query == 2:
		usprod = produced(conn, cur)
		print("{:100s}|\t{:30s}".format("title", "prodc_name"))
		print("-"*150)
		for movie in usprod:
			print("{:100s}|\t{:30s}".format(movie[0], movie[1]))
	# Movies with the most revenue
	elif query == 3:
		topfive = revenue(conn, cur)
		print("{:14s}|\trevenue".format("title"))
		print("-"*50)
		for movie in topfive:
			print("{:14s}|\t${:,d}".format(movie[0], movie[1]))
	#Scifi/Mystery
	elif query == 4:
		genrel = genres(conn,cur)
		print("{:50s}|\tgenre".format("title"))
		print("-"*80)
		for movie in genrel:
			print("{:50s}".format(movie[0]))
		
	# Most popular movies
	elif query == 5:
		topfive = popular(conn, cur)
		print("{:100s}|\tpopularity".format("title"))
		print("-"*150)
		for movie in topfive:
			print("{:100s}|\t{}".format(movie[0], movie[1]))
	else:
		print("running queries\n")
		# Average Budget	
		avgbud = avgbudget(conn,cur)
		print("Average budget = {:,.4f}\n".format(avgbud))
		# Produced in US
		print("Movies produced in the US")
		usprod = produced(conn, cur)
		print("{:100s}|\t{:30s}".format("title", "prodc_name"))
		print("-"*150)
		for movie in usprod:
			print("{:100s}|\t{:30s}".format(movie[0], movie[1]))
		# Top Five Revenues
		topfive = revenue(conn, cur)
		title = "title"
		print("\nTop 5 revenues")
		print("{:14s}|\trevenue".format("title"))
		print("-"*50)
		for movie in topfive:
			print("{:14s}|\t${:,d}".format(movie[0], movie[1]))
		# Scifi/Mystery
		print("\nMovies and their associated genres that contain the genres "
			"'Science Fiction' and 'Mystery'")
		genrel = genres(conn,cur)
		print("{:50s}|\tgenre".format("title"))
		print("-"*80)
		for movie in genrel:
			print("{:50s}".format(movie[0]))
		#Popularity
		print("\nMovies in order of popularity.")
		topfive = popular(conn, cur)
		print("{:100s}|\tpopularity".format("title"))
		print("-"*150)
		for movie in topfive:
			print("{:100s}|\t{}".format(movie[0], movie[1]))



	# close the connection when finished
	cur.close()
	conn.close()

