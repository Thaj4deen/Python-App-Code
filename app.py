from flask import Flask, request, jsonify
import psycopg2

app = Flask(__name__)

# Database connection parameters
DB_NAME = "postgres"
DB_USER = "postgres"
DB_PASSWORD = "admin1234"
DB_HOST = "172.17.0.2"
DB_PORT = "5432"

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )

        # Create 'users' table if not exists
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100) NOT NULL,
                age INTEGER
            )
        """)
        conn.commit()
        cur.close()

        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    return conn

@app.route('/create', methods=['POST'])
def create_user():
    data = request.get_json()
    name = data.get('name')
    email = data.get('email')
    age = data.get('age')

    sql = """INSERT INTO users (name, email, age)
             VALUES(%s, %s, %s) RETURNING id;"""
    conn = None
    user_id = None
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql, (name, email, age))
        user_id = cur.fetchone()[0]
        conn.commit()
        cur.close()

        return jsonify({'user_id': user_id}), 201
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return jsonify({'error': str(error)}), 500
    finally:
        if conn is not None:
            conn.close()

@app.route('/read', methods=['GET'])
def read_users():
    conn = None
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute("SELECT id, name, email, age FROM users ORDER BY id")
        rows = cur.fetchall()
        cur.close()
        users = [{'id': row[0], 'name': row[1], 'email': row[2], 'age': row[3]} for row in rows]
        return jsonify(users), 200
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return jsonify({'error': str(error)}), 500
    finally:
        if conn is not None:
            conn.close()

@app.route('/update/<int:user_id>', methods=['PUT'])
def update_user(user_id):
    data = request.get_json()
    name = data.get('name')
    email = data.get('email')
    age = data.get('age')

    sql = """ UPDATE users
              SET name = %s,
                  email = %s,
                  age = %s
              WHERE id = %s"""
    conn = None
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql, (name, email, age, user_id))
        conn.commit()
        cur.close()

        return jsonify({'message': 'User updated successfully'}), 200
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return jsonify({'error': str(error)}), 500
    finally:
        if conn is not None:
            conn.close()

@app.route('/delete/<int:user_id>', methods=['DELETE'])
def delete_user(user_id):
    sql = "DELETE FROM users WHERE id = %s"
    conn = None
    try:
        conn = connect()
        cur = conn.cursor()
        cur.execute(sql, (user_id,))
        conn.commit()
        cur.close()

        return jsonify({'message': 'User deleted successfully'}), 200
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return jsonify({'error': str(error)}), 500
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    app.run(debug=True)
