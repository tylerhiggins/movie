import pymysql


conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='password', db='assignment3')

cur = conn.cursor()

cur.execute("SELECT * FROM assignment3.Apply")
tuples = cur.fetchall()

for t in tuples:
	print(t)



conn.close() 
cur.close()
