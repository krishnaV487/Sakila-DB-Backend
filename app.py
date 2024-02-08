from flask import Flask, jsonify, request
from flask_mysqldb import MySQL
import json


app = Flask(__name__)

app.config['MYSQL_HOST'] = 'localhost'
app.config['MYSQL_USER'] = 'root'
app.config['MYSQL_PASSWORD'] = '12abAB@#'
app.config['MYSQL_DB'] = 'sakila'

mysql = MySQL(app)


@app.route("/homepage", methods = ['GET'])
def hometop5():
    cursor = mysql.connection.cursor()
    cursor.execute(
        """SELECT f.*, COUNT(*) AS rental_count
FROM film AS f
JOIN film_category AS fc ON f.film_id = fc.film_id
JOIN category AS c ON fc.category_id = c.category_id
JOIN inventory AS inv ON f.film_id = inv.film_id
JOIN rental AS r ON r.inventory_id = inv.inventory_id
GROUP BY f.film_id, f.title, c.name
ORDER BY rental_count DESC
LIMIT 5;"""
    )
    row_headers=[x[0] for x in cursor.description]
    toret = cursor.fetchall()
    jsontrt = []
    for i in toret:
        jsontrt.append(dict(zip(row_headers,i)))
    cursor.close()
    jsondict = {"data" : jsontrt}
    return jsonify(jsondict)

@app.route("/homepage2", methods = ['GET'])
def hometop5_2():
    cursor = mysql.connection.cursor()
    cursor.execute(
        """SELECT a.*, COUNT(*) 
FROM actor AS a 
JOIN film_actor AS fa ON a.actor_id = fa.actor_id 
INNER JOIN film AS f ON f.film_id = fa.film_id
GROUP BY a.actor_id 
ORDER BY COUNT(*) DESC
LIMIT 5;"""
    )
    row_headers=[x[0] for x in cursor.description]
    toret = cursor.fetchall()
    jsontrt = []
    for i in toret:
        jsontrt.append(dict(zip(row_headers,i)))
    cursor.close()
    jsondict = {"data" : jsontrt}
    return jsonify(jsondict)


@app.route("/films", methods = ['GET'])
def films():
    cursor = mysql.connection.cursor()
    cursor.execute("""select * from film""")
    row_headers=[x[0] for x in cursor.description]
    toret = cursor.fetchall()
    jsontrt = []
    cursor.close()
    for i in toret:
        jsontrt.append(dict(zip(row_headers,i)))
    cursor.close()
    jsondict = {"data" : jsontrt}
    return jsonify(jsondict)


@app.route("/actor5")
def actor():
    cursor = mysql.connection.cursor()
    actorId = request.args.get('actorId')
    searchterm = """SELECT f.film_id, f.title, c.name AS category, COUNT(*) AS rental_count
FROM film AS f
JOIN film_category AS fc ON f.film_id = fc.film_id
JOIN category AS c ON fc.category_id = c.category_id
JOIN inventory AS inv ON f.film_id = inv.film_id
JOIN rental AS r ON r.inventory_id = inv.inventory_id
JOIN film_actor AS fa ON f.film_id = fa.film_id
where fa.actor_id = """ + actorId + """
GROUP BY f.film_id, f.title, c.name
ORDER BY rental_count DESC
LIMIT 5;"""
    cursor.execute(searchterm)
    row_headers=[x[0] for x in cursor.description]
    toret = cursor.fetchall()
    jsontrt = []
    cursor.close()
    for i in toret:
        jsontrt.append(dict(zip(row_headers,i)))
    cursor.close()
    jsondict = {"data" : jsontrt}
    return jsonify(jsondict)

#search FILMS
@app.route("/sf")
def searchfilm():
    cursor = mysql.connection.cursor()
    film_id = request.args.get('filmname')
    actor_id = request.args.get('actorname')
    category = request.args.get('category')
    if film_id == None:
        film_id = ""
    if actor_id == None:
        actor_id = ""
    if category == None:
        category = ""
    searchterm = """SELECT DISTINCT f.*
FROM film AS f
inner JOIN film_category AS fc ON f.film_id = fc.film_id
inner JOIN category AS c ON c.category_id = fc.category_id
inner JOIN film_actor AS fa ON f.film_id = fa.film_id
inner JOIN actor AS a ON a.actor_id = fa.actor_id
WHERE (a.first_name LIKE '%"""+actor_id+"""%' 
OR a.last_name LIKE '%"""+actor_id+"""%') 
AND f.title LIKE '%"""+film_id+"""%' 
AND c.name LIKE '%"""+category+"""%';"""
    cursor.execute(searchterm)
    row_headers=[x[0] for x in cursor.description]
    toret = cursor.fetchall()
    jsontrt = []
    cursor.close()
    for i in toret:
        jsontrt.append(dict(zip(row_headers,i)))
    cursor.close()
    jsondict = {"data" : jsontrt}
    return jsonify(jsondict)


if __name__  == "__main__":
    app.run(debug=True)