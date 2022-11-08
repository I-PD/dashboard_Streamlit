import psycopg2


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def find_min_index(lst):
    min_value = min(lst)
    min_index = lst.index(min_value)
    return min_index, min_value


def find_max_index(lst):
    max_value = max(lst)
    max_index = lst.index(max_value)
    return max_index, max_value


def openCSV(data):
    # Abrir ficheiros
    data_file = open(data, 'r').readlines()
    # Auxiliares
    date_rows = []
    time_rows = []
    channel_1 = []
    channel_2 = []
    channel_3 = []
    channel_4 = []
    for i in range(len(data_file)):
        aux_list = data_file[i].replace('\n', '').split(';')
        date = aux_list[0]
        time_value = aux_list[1]
        ch1_unmodded = aux_list[2]
        ch2_unmodded = aux_list[3]
        ch3_unmodded = aux_list[4]
        ch4_unmodded = aux_list[5]

        if ch1_unmodded == 'OL' or float(ch1_unmodded.replace(',', '.')) < -10:
            continue
        if ch2_unmodded == 'OL' or float(ch2_unmodded.replace(',', '.')) < -10:
            continue
        if ch3_unmodded == 'OL' or float(ch3_unmodded.replace(',', '.')) < -10:
            continue
        if ch4_unmodded == 'OL' or float(ch4_unmodded.replace(',', '.')) < -10:
            continue

        ch1 = float(ch1_unmodded.replace(',', '.'))
        ch2 = float(ch2_unmodded.replace(',', '.'))
        ch3 = float(ch3_unmodded.replace(',', '.'))
        ch4 = float(ch4_unmodded.replace(',', '.'))

        channel_1.append(ch1)
        channel_2.append(ch2)
        channel_3.append(ch3)
        channel_4.append(ch4)
        date_rows.append(date)
        time_rows.append(time_value)

    return channel_1, channel_2, channel_3, channel_4, date_rows, time_rows


def get_graphs(channel_list, channel, time_list):
    chunks_list = list(chunks(channel_list, 164))
    times_list = list(chunks(time_list, 164))
    legend = []
    min_list = []
    min_times_list = []
    max_list = []

    for i in range(len(chunks_list) - 1):
        # find max and min
        min_index, min_value = find_min_index(chunks_list[i])
        max_index, max_value = find_max_index(chunks_list[i])
        min_list.append(min_value)
        max_list.append(max_value)
        min_times_list.append(times_list[i][min_index])

    return max_list, min_list, min_times_list


#
# def statistics(max_list, min_list, channel):
#     # MAX
#     max_in_max_list = max(max_list)
#     min_in_max_list = min(max_list)
#     average_max = sum(max_list) / len(max_list)
#
#     # MIN
#     max_in_min_list = max(min_list)
#     min_in_min_list = min(min_list)
#     average_min = sum(min_list) / len(min_list)
#
#     print('CHANNEL {}:'.format(channel))
#     print('max: {}'.format(max_in_max_list))
#     print('average max: {}'.format(average_max))
#     print('min: {}'.format(min_in_min_list))
#     print('average min: {}'.format(average_min))


# def plot_mins(min_list, times_list, channel, date):
#     plt.figure()
#     plt.plot(times_list, min_list, marker='.', markersize=10)
#     plt.title('Min Values on Channel {} on {}'.format(channel, date))
#     plt.xticks(rotation=45, fontsize=8)
#     plt.ylim([-7, 25])
#     plt.show()


def database_temps(list):
    try:
        query = 'insert into dashboard_temperature(channel, mins, date_min, time_min) VALUES(%s,%s,%s,%s)'
        conn = psycopg2.connect(
            host="digitalhub-1.c7gscdaeyvjj.eu-west-2.rds.amazonaws.com",
            port="5432",
            database="temps DS100+",
            user="corkgres",
            password="Corksupply2022db")

        cur = conn.cursor()

        cur.executemany(query, list)

        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

        return


def main(file):
    data1, data2, data3, data4, data_date, data_time = openCSV(file)

    print(data_date[0])
    # channels = input('channels: ex 1_2_3_4: ')
    # channels_parsed = channels.split('_')
    channels_parsed = [5, 6, 7, 8]
    # Graphs to see the sync
    max_1, min_1, times1 = get_graphs(data1, data_time)
    max_2, min_2, times2 = get_graphs(data2, data_time)
    max_3, min_3, times3 = get_graphs(data3, data_time)
    max_4, min_4, times4 = get_graphs(data4, data_time)

    db_channel_1 = []
    db_channel_2 = []
    db_channel_3 = []
    db_channel_4 = []
    import datetime
    d = datetime.datetime.strptime(data_date[0], "%d/%m/%Y")
    s = d.strftime('%Y-%m-%d')

    for i in range(len(min_1)):
        db_channel_1.append([channels_parsed[0], min_1[i], s, times1[i]])

    for i in range(len(min_2)):
        db_channel_2.append([channels_parsed[1], min_2[i], s, times2[i]])

    for i in range(len(min_3)):
        db_channel_3.append([channels_parsed[2], min_3[i], s, times3[i]])

    for i in range(len(min_4)):
        db_channel_4.append([channels_parsed[3], min_4[i], s, times4[i]])

    database_temps(db_channel_1)
    database_temps(db_channel_2)
    database_temps(db_channel_3)
    database_temps(db_channel_4)
