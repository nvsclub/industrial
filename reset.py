import psycopg2


def SqlLog():

    log = open("login.txt", 'r')
    info = log.read().splitlines()
    try:
        myconn=psycopg2.connect(host=info[0], user=info[1], password=info[2], dbname=info[3])
        myconn.autocommit = True
    except:
        print ("Error Connecting to SQL SERVER")
        return -1

    try:
        cur = myconn.cursor()
    except:
        print("ERROR CONNECTING TO SQL DATABASE")
        return -1

    log.close()
    return myconn

def SqlQuery(conn, cmd):

    cur = conn.cursor()
    try:
        cur.execute(cmd)
        result = cur.fetchall()
        return result
    except psycopg2.Error:
        return -1

def SqlQueryVarOne(conn, cmd, vars):

	cur = conn.cursor()
	try:
		cur.execute(cmd, vars)
		result = cur.fetchone()
		return result
	except psycopg2.Error:
		return -1

def SqlCount(conn,cmd):
    cur = conn.cursor()
    try:
        cur.execute(cmd)
        result = cur.rowcount
        return result
    except psycopg2.Error:
        return -1

def SqlCreate(conn, cmd):
    cur = conn.cursor()
    try:
        result = cur.execute(cmd)
        return result
    except psycopg2.Error:
        return -1

def SqlCreateVar(conn, cmd, vars):
    cur = conn.cursor()
    try:
        result = cur.execute(cmd,vars)
        return result
    except:
        return -1

def SqlClose(myconn):
    myconn.close()


conn = SqlLog()

i = "SET search_path TO ii"
res = SqlQuery(conn,i)

for z in range(7):
  i = "UPDATE orders SET done = %s WHERE id = %s"
  var = (False, z)
  res = SqlCreateVar(conn,i,var)

SqlClose(conn)