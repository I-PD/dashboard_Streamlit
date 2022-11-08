import psycopg2


class min_temp:
    def __init__(self, channel, mins, date, time):
        self.channel = channel
        self.mins = mins
        self.date = date
        self.time = time


def database_temps():
    list_channel = []
    list_mins = []
    list_date = []
    list_time = []
    try:
        query = 'select channel, mins, date_min, time_min from dashboard_temperature order by date_min'
        conn = psycopg2.connect(
            host="digitalhub-1.c7gscdaeyvjj.eu-west-2.rds.amazonaws.com",
            port="5432",
            database="temps DS100+",
            user="corkgres",
            password="Corksupply2022db")

        cur = conn.cursor()

        cur.execute(query)

        data = cur.fetchall()

        cur.close()

        for row in data:
            min_temp(row[0], row[1], row[2], row[3])

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

        return min_temp
