import datetime as dt
import psycopg2 as pg


def openCSV(data):
    # Abrir ficheiros
    data_file = open(data, 'r').readlines()
    # Auxiliares
    row = []
    maqs = ''
    data = []
    db_list = []
    for i in range(len(data_file)):
        row = data_file[i].split(';')
        dates = dt.datetime.strptime(row[0], '%d/%m/%Y').date()
        maqs = row[1]
        for j in range(2, len(row)):
            if row[j] == '':
                continue
            db_list.append([maqs, dates, float(row[j].replace(',', '.'))])

    print(db_list)
    import_to_db(db_list)

    return db_list


def import_to_db(list):
    try:
        query = 'insert into humidity(machine,scan_date, value) values(%s,%s,%s)'
        conn = pg.connect(
            host="digitalhub-1.c7gscdaeyvjj.eu-west-2.rds.amazonaws.com",
            port="5432",
            database="Humidity DS100+",
            user="corkgres",
            password="Corksupply2022db")

        cur = conn.cursor()

        cur.executemany(query, list)

        conn.commit()
        cur.close()
    except (Exception, pg.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

        return


def main():
    openCSV('humidade_CSV_Func.csv')

