import psycopg2

connection = psycopg2.connect(user="cs143",password="cs143",
    host="localhost", port="5432", database="cs143")
connection.autocommit = True
with connection.cursor() as cur:
    cur.execute("""
    SELECT
        sf_trip_start.id AS trip_id,
        user_type,
        CASE user_type
            WHEN 'Subscriber' THEN COALESCE(0.2*(EXTRACT(EPOCH FROM sf_trip_end.time - sf_trip_start.time)/60), 1000)::numeric(7,2)
            ELSE COALESCE(3.49+0.3*(EXTRACT(EPOCH FROM sf_trip_end.time - sf_trip_start.time)/60), 1000)::numeric(7,2)
        END AS trip_cost
    FROM sf_trip_start
    LEFT JOIN sf_trip_end
    ON sf_trip_start.id=sf_trip_end.id
    JOIN sf_trip_user
    ON sf_trip_start.id=sf_trip_user.trip_id
    ORDER BY sf_trip_user.trip_id;
    """)
    rows = cur.fetchall()
    print("SAN FRANCISCO BIKE SHARE")
    print("Roster of Charges\n")
    print("Trip ID        Charge")
    print("-----------    -----------")
    for result in rows:
        trip_id, user_type, trip_cost = result
        print("{}            $ {}".format(trip_id, trip_cost))
connection.close()
