import telegram
import matplotlib.pyplot as plt
import io
import pandahouse as ph
import sys
from datetime import date, datetime, timedelta
from airflow.decorators import dag, task

default_args = {
    'owner': 'e.popova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 7, 11),
}

schedule_interval = '0 11 * * *'

connection = {
    'host': 'host',
    'password': '12345',
    'user': 'user',
    'database': 'simulator_20220620'
}

q_1 = '''SELECT count(DISTINCT user_id) as dau, countIf(action='view') as views, 
    countIf(action='like') as likes, round(countIf(action='like')/countIf(action='view'),2) as ctr
    FROM simulator_20220620.feed_actions
    WHERE toDate(time) = yesterday()'''

q_2 = '''SELECT toDate(time) as date, count(DISTINCT user_id) as dau, countIf(action='view') as views, 
    countIf(action='like') as likes, round(countIf(action='like')/countIf(action='view'),2) as ctr
    FROM simulator_20220620.feed_actions
    WHERE date >= yesterday() - 6 and date < today()
    GROUP BY date'''

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_bot_epopova():

    @task
    def my_report():
        chat_id = 123456789
        my_token = 'token'
        bot = telegram.Bot(token=my_token)

        df = ph.read_clickhouse(q_1, connection=connection)
        dau,likes,views,ctr = df.loc[0,'dau'],df.loc[0,'likes'],df.loc[0,'views'],df.loc[0,'ctr'] 
        yesterday = date.today() - timedelta(days=1)
        msg = '''Значения ключевых метрик на {yesterday}:\nDAU = {dau}\nLikes = {likes}\nViews = {views}\nCTR = {ctr}'''\
        .format(yesterday=yesterday,dau=dau,likes=likes,views=views,ctr=ctr)
        bot.sendMessage(text = msg,chat_id=chat_id)

        df_2 = ph.read_clickhouse(q_2, connection=connection)
        plt.rcParams["figure.figsize"] = (8,6)
        fig, ax = plt.subplots(4,1, sharex=True)
        ax[0].plot(df_2['date'], df_2['dau'])
        ax[0].set_ylabel('DAU')      
        ax[0].set_title('Значения метрик за последние 7 дней')
        ax[1].plot(df_2['date'], df_2['likes'])
        ax[1].set_ylabel('Likes')
        ax[2].plot(df_2['date'], df_2['views'])
        ax[2].set_ylabel('Views')
        ax[3].plot(df_2['date'], df_2['ctr'])
        ax[3].set_ylabel('CTR')
        plt.xticks(rotation=20)

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id,photo=plot_object) 
     
    my_report()
    
dag_bot_epopova = dag_bot_epopova()