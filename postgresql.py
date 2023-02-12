import psycopg2

# Connect to a PostgreSQL database
conn = psycopg2.connect(
    host="host_name",
    database="database_name",
    user="user_name",
    password="password"
)

# Create a cursor
cur = conn.cursor()

# Execute a SELECT statement
cur.execute("SELECT * FROM table_name")

# Fetch all the results
results = cur.fetchall()

# Loop through the results and print them
for row in results:
    print(row)

# Close the cursor and the connection
cur.close()
conn.close()
