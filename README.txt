To run this program

python3 movie.py username password <query#/debug/delete> <debug/delete> 

'-d' is debug mode
'-delete' drops the database and quits
will display an message and exit if there's an invalid username/password.

You cannot do '-d' and '-delete' at the same time, either has to be one or the other

The program will error and exit on too many or too few arguments

When the program runs it will display "inserting into schema..."
When it is finished, it will output "done!" Followed by all of the queries (see below)

If the movies schema isn't already created it will take approximately 
1min and 7.9sec to create.

If the user enters a query number then the following applies:
1: Average budget
2: Movies produced in US
3: Top 5 movies with most revenue
4: Movies with Sci-Fi and Mystery
5: More popular than average
