from flask import Flask, render_template, request
import mysql.connector

app = Flask(__name__)


connection = mysql.connector.connect(
    host="qtc353.encs.concordia.ca",
    user="qtc353_1",
    password="OP7383SP",
    database="qtc353_1"
)
cursor = connection.cursor(dictionary=True)

def get_all_tables():
    cursor.execute("SHOW TABLES")
    return [row[f'Tables_in_{connection.database}'] for row in cursor.fetchall()]

@app.route('/')
def home():
    return render_template('home.html', tables=get_all_tables())

@app.route('/table/<table_name>')
def show_table(table_name):
    if table_name not in get_all_tables():
        return f"Table {table_name} not found.", 404
    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        if rows:
            columns = rows[0].keys()
        else:
            cursor.execute(f"SHOW COLUMNS FROM {table_name}")
            columns = [col['Field'] for col in cursor.fetchall()]
    except Exception as e:
        return f"Error accessing table data: {e}", 500

    cursor.execute("""
        SELECT COLUMN_NAME, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s
    """, (table_name,))
    nullable_info = cursor.fetchall()
    nullables = {col["COLUMN_NAME"]: col["IS_NULLABLE"] == "YES" for col in nullable_info}

    cursor.execute("""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE TABLE_SCHEMA = DATABASE()
          AND TABLE_NAME = %s
          AND CONSTRAINT_NAME = 'PRIMARY'
    """, (table_name,))
    pk_cols_info = cursor.fetchall()
    primary_keys = [row['COLUMN_NAME'] for row in pk_cols_info]

    return render_template('table_view.html', table=table_name, columns=columns, rows=rows, nullables=nullables,
                           primary_keys=primary_keys)

# Add create_table and delete_table routes
@app.route('/create_table', methods=['POST'])
def create_table():
    table_name = request.form['table_name']
    column_definitions = request.form['columns']  # e.g., "id INT PRIMARY KEY, name VARCHAR(100)"
    try:
        cursor.execute(f"CREATE TABLE {table_name} ({column_definitions})")
        connection.commit()
        get_all_tables().append(table_name)
        return f"Table {table_name} created successfully! <a href='/'>Return</a>"
    except Exception as e:
        return f"Error creating table: {e} <a href='/'>Return</a>"


@app.route('/delete_table', methods=['POST'])
def delete_table():
    table_name = request.form['delete_table_name']
    if table_name not in get_all_tables():
        return f"Table {table_name} not found. <a href='/'>Return</a>"
    try:
        cursor.execute(f"DROP TABLE {table_name}")
        connection.commit()
        return f"Table {table_name} deleted successfully! <a href='/'>Return</a>"
    except Exception as e:
        return f"Error deleting table: {e} <a href='/'>Return</a>"

@app.route('/insert_row/<table_name>', methods=['POST'])
def insert_row(table_name):
    try:
        columns = request.form.to_dict()
        keys = ', '.join(columns.keys())
        values = list(columns.values())
        placeholders = ', '.join(['%s'] * len(values))

        cursor.execute(f"INSERT INTO {table_name} ({keys}) VALUES ({placeholders})", values)
        connection.commit()
        return f"Row inserted into {table_name}. <a href='/table/{table_name}'>Return</a>"
    except Exception as e:
        return f"Error inserting row: {e} <a href='/table/{table_name}'>Return</a>"


@app.route('/edit_row/<table_name>', methods=['POST'])
def edit_row(table_name):
    try:
        # Collect composite key components
        pk_columns = []
        pk_values = []
        for key in request.form:
            if key.startswith("pk_column_"):
                suffix = key.split("_")[-1]
                pk_col = request.form[key]
                pk_val = request.form.get(f"pk_value_{suffix}")
                if pk_col and pk_val:
                    pk_columns.append(pk_col)
                    pk_values.append(pk_val)

        # Collect updated fields
        updates = []
        values = []
        for key, value in request.form.items():
            if not key.startswith("pk_") and value.strip() != "":
                updates.append(f"{key} = %s")
                values.append(value.strip())

        if not updates:
            return f"No fields to update. <a href='/table/{table_name}'>Return</a>"

        # Combine update + where clause
        where_clause = " AND ".join(f"{col} = %s" for col in pk_columns)
        values += pk_values

        update_query = f"UPDATE {table_name} SET {', '.join(updates)} WHERE {where_clause}"
        cursor.execute(update_query, values)
        connection.commit()
        return f"Row updated in {table_name}. <a href='/table/{table_name}'>Return</a>"

    except Exception as e:
        return f"Error updating row: {e} <a href='/table/{table_name}'>Return</a>"

@app.route('/delete_row/<table_name>', methods=['POST'])
def delete_row(table_name):
    try:
        pk_columns = []
        pk_values = []
        for key in request.form:
            if key.startswith("pk_column_"):
                suffix = key.split("_")[-1]
                pk_col = request.form[key]
                pk_val = request.form.get(f"pk_value_{suffix}")
                if pk_col and pk_val:
                    pk_columns.append(pk_col)
                    pk_values.append(pk_val)

        where_clause = " AND ".join(f"{col} = %s" for col in pk_columns)

        delete_query = f"DELETE FROM {table_name} WHERE {where_clause}"
        cursor.execute(delete_query, pk_values)
        connection.commit()
        return f"Row deleted from {table_name}. <a href='/table/{table_name}'>Return</a>"

    except Exception as e:
        return f"Error deleting row: {e} <a href='/table/{table_name}'>Return</a>"


@app.route('/query/<int:num>', methods=["GET"])
def run_query(num):
    queries = {
        8: """
            SELECT 
        l.address, l.city, l.province, l.postalCode, l.phoneNumber, l.webAddress, 
        l.type, l.maxCapacity,
        (
            SELECT CONCAT(p.firstName, ' ', p.lastName)
            FROM Personnel p
            JOIN PersonnelHistory ph ON ph.personnelID = p.personnelID
            WHERE ph.locID = l.locID AND p.role = 'General Manager'
            ORDER BY ph.startDate DESC
            LIMIT 1
        ) AS generalManagerName,   -- This comma was missing
        (
            SELECT COUNT(*) FROM ClubMember cm
            JOIN Minor m ON m.memberID = cm.memberID
            WHERE cm.locID = l.locID
        ) AS numMinors,
        (
            SELECT COUNT(*) FROM ClubMember cm
            WHERE cm.locID = l.locID AND cm.memberID NOT IN (SELECT memberID FROM Minor)
        ) AS numMajors,
        (
            SELECT COUNT(*) FROM Team t WHERE t.locID = l.locID
        ) AS numTeams
        FROM Location l
        ORDER BY l.province, l.city;
    """,
        9: """
                SELECT 
          sfm.firstName AS secondary_first_name,
          sfm.lastName AS secondary_last_name,
          sfm.phoneNumber AS secondary_phone,
          cm.memberID,
          cm.firstName AS member_first_name,
          cm.lastName AS member_last_name,
          cm.dob,
          cm.SSN,
          cm.medicareNumber,
          cm.phoneNumber AS member_phone,
          cm.address,
          cm.city,
          cm.province,
          cm.postalCode,
          m.relationship  
        FROM SecondaryFamilyMember sfm
        JOIN FamilyMember fm
          ON fm.familyMemberID = sfm.primaryFamilyMemberID
        JOIN Minor m
          ON m.familyMemberID = fm.familyMemberID
        JOIN ClubMember cm
          ON cm.memberID = m.memberID
        WHERE sfm.primaryFamilyMemberID = 100;
    """,
        10: """
        SELECT
  coach.firstName AS headCoachFirstName,
  coach.lastName AS headCoachLastName,
  CONCAT(s.date, ' ', s.time) AS sessionStartTime,
  l.address AS sessionAddress,
  s.type AS sessionType,
  t.teamName,
  CASE
    WHEN CONCAT(s.date, ' ', s.time) <= NOW() THEN s.team_1Score
    ELSE NULL
  END AS team1Score,
  CASE
    WHEN CONCAT(s.date, ' ', s.time) <= NOW() THEN s.team_2Score
    ELSE NULL
  END AS team2Score,
  player.firstName AS playerFirstName,
  player.lastName AS playerLastName,
  tf.position AS playerRole
FROM Sessions s
JOIN Location l ON s.locID = l.locID
JOIN Team t ON t.teamID = s.team_1ID
JOIN Personnel coach ON coach.personnelID = t.headCoachID
JOIN TeamFormation tf ON tf.sessionID = s.sessionID AND tf.teamID = t.teamID
JOIN ClubMember player ON player.memberID = tf.memberID
WHERE s.locID = 1
  AND s.date BETWEEN '2025-01-01' AND '2025-05-31'
ORDER BY sessionStartTime ASC;
""",
        11: """
         SELECT 
  cm.memberID, 
  cm.firstName, 
  cm.lastName
FROM ClubMember cm
JOIN FamilyHistory fh ON cm.memberID = fh.memberID
WHERE cm.status = 'Inactive'
GROUP BY cm.memberID, cm.firstName, cm.lastName
HAVING COUNT(DISTINCT fh.locID) >= 2
   AND MIN(fh.startDate) <= DATE_SUB(CURDATE(), INTERVAL 2 YEAR)
ORDER BY cm.memberID ASC;
""",
        12: """
        SELECT
  l.name AS locationName,
 COUNT(DISTINCT CASE WHEN s.type = 'Training' THEN s.sessionID END) AS totalTrainingSessions,
  COUNT(CASE WHEN s.type = 'Training' THEN tf.memberID END) AS totalTrainingPlayers,
  COUNT(DISTINCT CASE WHEN s.type = 'Game' THEN s.sessionID END) AS totalGameSessions,
  COUNT(CASE WHEN s.type = 'Game' THEN tf.memberID END) AS totalGamePlayers
FROM Location l
JOIN Sessions s ON s.locID = l.locID
LEFT JOIN TeamFormation tf ON tf.sessionID = s.sessionID
WHERE s.date BETWEEN '2025-01-01' AND '2025-05-31'
GROUP BY l.locID, l.name
HAVING COUNT(DISTINCT CASE WHEN s.type = 'Game' THEN s.sessionID END) >= 4
ORDER BY totalGameSessions DESC;
""",
        13: """
        SELECT
  cm.memberID,
  cm.firstName,
  cm.lastName,
  TIMESTAMPDIFF(YEAR, cm.dob, CURDATE()) AS age,
  cm.phoneNumber,
  cm.email,
  l.name AS locationName
FROM ClubMember cm
LEFT JOIN TeamFormation tf ON cm.memberID = tf.memberID
JOIN Location l ON cm.locID = l.locID
WHERE cm.status = 'Active'
  AND tf.memberID IS NULL
ORDER BY l.name ASC, age ASC
LIMIT 5;
""",
        14: """
        SELECT
  cm.memberID,
  cm.firstName,
  cm.lastName,
  maj.dateJoined,
  TIMESTAMPDIFF(YEAR, cm.dob, CURDATE()) AS age,
  cm.phoneNumber,
  cm.email,
  l.name AS locationName
FROM ClubMember cm
JOIN Major maj ON cm.memberID = maj.memberID
JOIN Minor min ON cm.memberID = min.memberID
JOIN Location l ON cm.locID = l.locID
WHERE cm.status = 'Active'
ORDER BY locationName ASC, age ASC;
""",
        15: """
        SELECT
  cm.memberID,
  cm.firstName,
  cm.lastName,
  TIMESTAMPDIFF(YEAR, cm.dob, CURDATE()) AS age,
  cm.phoneNumber,
  cm.email,
  l.name AS locationName
FROM ClubMember cm
JOIN TeamFormation tf ON cm.memberID = tf.memberID
JOIN Location l ON cm.locID = l.locID
WHERE cm.status = 'Active'
AND cm.memberID IN (
  SELECT memberID
  FROM TeamFormation
  GROUP BY memberID
  HAVING 
    COUNT(*) >= 1
    AND COUNT(DISTINCT position) = 1
    AND MAX(position) = 'setter'
)
GROUP BY cm.memberID, cm.firstName, cm.lastName, cm.dob, cm.phoneNumber, cm.email, l.name
ORDER BY locationName ASC, cm.memberID ASC;
""",
        16: """
        SELECT 
  cm.memberID,
  cm.firstName,
  cm.lastName,
  FLOOR(DATEDIFF(CURDATE(), cm.dob) / 365.25) AS age,
  cm.phoneNumber,
  cm.email,
  l.name AS locationName
FROM ClubMember cm
JOIN Location l ON cm.locID = l.locID
WHERE cm.status = 'Active'
  AND cm.memberID IN (
    SELECT tf.memberID
    FROM TeamFormation tf
    JOIN Sessions s ON tf.sessionID = s.sessionID
    WHERE s.type = 'Game'
    GROUP BY tf.memberID
    HAVING 
      SUM(position = 'setter') > 0 AND
      SUM(position = 'libero') > 0 AND
      SUM(position = 'outside hitter') > 0 AND
      SUM(position = 'opposite hitter') > 0
  )
ORDER BY locationName ASC, cm.memberID ASC;
""",
        17: """
        SELECT DISTINCT
fm.firstName,
fm.lastName,
fm.phoneNumber
FROM FamilyMember fm
JOIN Minor m ON m.familyMemberID = fm.familyMemberID
JOIN ClubMember cm ON cm.memberID = m.memberID
JOIN Team t ON t.headCoachID = fm.familyMemberID
JOIN TeamFormation tf ON tf.teamID = t.teamID
JOIN Sessions s ON s.sessionID = tf.sessionID
WHERE cm.status = 'Active'
AND cm.locID = s.locID
AND s.locID = 1;
""",
        18: """
        SELECT
  cm.memberID,
  cm.firstName,
  cm.lastName,
  TIMESTAMPDIFF(YEAR, cm.dob, CURDATE()) AS age,
  cm.phoneNumber,
  cm.email,
  l.name AS locationName
FROM ClubMember cm
JOIN Location l ON cm.locID = l.locID
WHERE cm.status = 'Active'
  AND cm.memberID IN (
    SELECT tf.memberID
    FROM TeamFormation tf
    JOIN Sessions s ON tf.sessionID = s.sessionID
    WHERE s.type = 'game'
    GROUP BY tf.memberID
    HAVING SUM(
      CASE
        WHEN tf.teamID = s.team_1ID AND s.team_1Score > s.team_2Score THEN 0
        WHEN tf.teamID = s.team_2ID AND s.team_2Score > s.team_1Score THEN 0
        ELSE 1
      END
    ) = 0
  )
ORDER BY l.province, l.city, cm.memberID;
""",
        19: """
        SELECT 
    p.firstName AS 'First Name',
    p.lastName AS 'Last Name',
    COUNT(m.memberID) AS 'Number of Associated Minors',
    p.phoneNumber AS 'Phone Number',
    p.email AS 'Email',
    l.name AS 'Current Location',
    p.role AS 'Role'
FROM 
    Personnel p
JOIN 
    FamilyMember fm ON p.personnelID = fm.familyMemberId
JOIN 
    Minor m ON fm.familyMemberId = m.familyMemberID
JOIN 
    PersonnelHistory ph ON p.personnelID = ph.personnelID AND ph.endDate IS NULL
JOIN 
    Location l ON ph.locID = l.locID
WHERE 
    p.mandate = 'Volunteer'
GROUP BY 
    p.personnelID, p.firstName, p.lastName, p.phoneNumber, p.email, l.name, p.role
ORDER BY 
    l.name ASC, 
    p.role ASC, 
    p.firstName ASC, 
    p.lastName ASC;
"""
    }

    query = queries.get(num)
    if not query:
        return f"Query {num} not found. <a href='/'>Return</a>"

    try:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = rows[0].keys() if rows else []
        return render_template("query_result.html", query_numbers=num, columns=columns, rows=rows)
    except Exception as e:
        return f"Error executing query: {e} <a href='/'>Return</a>"


# Route to handle user-submitted custom SQL queries from a form
@app.route('/run_custom_query', methods=['POST'])
def run_custom_query():
    custom_query = request.form.get('custom_sql', '')
    if not custom_query.strip():
        return "No query provided. <a href='/'>Return</a>"

    try:
        cursor.execute(custom_query)
        if cursor.with_rows:
            rows = cursor.fetchall()
            columns = rows[0].keys() if rows else []
            return render_template("query_result.html", query_numbers="Custom", columns=columns, rows=rows)
        else:
            connection.commit()
            return f"Query executed successfully. <a href='/'>Return</a>"
    except Exception as e:
        return f"Error in query: {e} <a href='/'>Return</a>"


if __name__ == '__main__':
    app.run(debug=True)