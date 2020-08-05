from kiteconnect import KiteConnect
from kiteconnect import KiteTicker
import pandas as pd
import datetime
import pdb
from pandas.io.json import json_normalize
import telegram
import mysql.connector
import time
import math
import pandas as pd
from sqlalchemy import create_engine
import pymysql
pymysql.install_as_MySQLdb()

kws = ""
kite = ""
login_flag = 0
entry_status = 0
exit_value = 0
capital = 5000
max_loss = capital*0.4/100

api_k = "Enter your api key"  # id
api_s = "Enter your secret key"  # pass

blacklist = ['ADANIPOWER', 'MRF']
traded_list = []
traded_once_today = []
reverse_traded_once_today = []
missed_trade_reversal = []
traded_stocks_dict = {}
ohlc_dict = {}
total_pnl = 0
today_pnl = 0

login_flag = 0
database_flag = 0

print('getting bot')
telegram_token = '1137420204:AAF1XgCsUKELTeM00U-Uo_zRLrCzGMr3Svo'  # big ohlc
bot = telegram.Bot(token=telegram_token)


def fetch_access_token():
    print("fetching access token value")
    mycursor.execute("select value from token_val")
    myresult = mycursor.fetchone()
    for row in myresult:
        a = row
    return a


def get_login(api_k, api_s):  # log in to zerodha API panel
    try:
        print("trying to login...")
        global kite, login_flag, kws
        kite = KiteConnect(api_key=api_k)
        access_token = fetch_access_token()
        kite.set_access_token(access_token)
        kws = KiteTicker(api_k, access_token)
        login_flag = 1
        bot.sendMessage(chat_id=984101934,
                        text="you are now logged in for today")
        print("you are now logged in for today")
    except Exception as e:
        bot.sendMessage(chat_id=984101934,
                        text="update api key, not able to login")
        print(e)


def get_nifty_direction():  # one for postive
    data = kite.quote(256265)
    change = data['256265']['ohlc']['open']-data['256265']['ohlc']['close']
    if change > 0:
        return 1
    else:
        return 0


def gap_valid_stock_list(df):
    global ohlc_dict
    eligible_list = []
    stock_direction_dict = {}

    eligible_df = df.loc[df['change'] < 0]
    eligible_list = list(eligible_df.iloc[:, 1])
    traded_stocks_dic = kite.quote(eligible_list)
    for stock in list(traded_stocks_dic.keys()):
        open_price = traded_stocks_dic[stock]['ohlc']['open']
        close = eligible_df.loc[eligible_df['index'] == stock].iloc[0, 5]
        if open_price > close:
            stock_direction_dict[stock] = 1  # for going up
            high = traded_stocks_dic[stock]['ohlc']['high']
            low = traded_stocks_dic[stock]['ohlc']['low']
            ohlc_dict[stock] = {"today_high": high, "today_low": low}

    eligible_df = df.loc[df['change'] > 0]
    eligible_list = list(eligible_df.iloc[:, 1])
    traded_stocks_dic = kite.quote(eligible_list)
    for stock in list(traded_stocks_dic.keys()):
        open_price = traded_stocks_dic[stock]['ohlc']['open']
        close = eligible_df.loc[eligible_df['index'] == stock].iloc[0, 5]
        if open_price < close:
            stock_direction_dict[stock] = 0  # for going down
            high = traded_stocks_dic[stock]['ohlc']['high']
            low = traded_stocks_dic[stock]['ohlc']['low']
            ohlc_dict[stock] = {"today_high": high, "today_low": low}

    return stock_direction_dict


def check_trade_status(dictt, stock, df, nifty, direction):
    global total_pnl, today_pnl, traded_stocks_dict, reverse_traded_once_today, missed_trade_reversal, traded_list

    if stock in missed_trade_reversal:
        reversal_trade(dictt, stock, df, nifty, direction)

    else:
        ltp = dictt[stock]['last_price']
        initial_value = traded_stocks_dict[stock]['initial_value']
        target = traded_stocks_dict[stock]['target']
        sl = traded_stocks_dict[stock]['sl']
        quantity = traded_stocks_dict[stock]['quantity']

        if traded_stocks_dict[stock]['direction'] == "buy":
            if ltp >= target:
                profit = quantity*abs(ltp-initial_value)
                total_pnl += profit
                today_pnl += profit
                message = f'{stock}\nbook profit\n{profit}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_list.remove(stock)
                print("profit")
            elif ltp <= sl:
                traded_list.remove(stock)
                # if stock not in reverse_traded_once_today:
                reversal_trade(dictt, stock, df, nifty, direction)
                loss = -quantity*abs(initial_value-ltp)
                total_pnl += loss
                today_pnl += loss
                message = f'{stock}\nbook loss\n{loss}'
                bot.sendMessage(chat_id=984101934, text=message)
                print("loss")

        else:
            if ltp <= target:
                profit = quantity*abs(ltp-initial_value)
                total_pnl += profit
                today_pnl += profit
                message = f'{stock}\nbook profit\n{profit}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_list.remove(stock)
                print('profit')
            elif ltp >= sl:
                traded_list.remove(stock)
                # if stock not in reverse_traded_once_today:
                reversal_trade(dictt, stock, df, nifty, direction)
                loss = -quantity*abs(initial_value-ltp)
                total_pnl += loss
                today_pnl += loss
                message = f'{stock}\nbook loss\n{loss}'
                bot.sendMessage(chat_id=984101934, text=message)
                print("loss")


def live_track_opportunity(dictt, stock, df, nifty, direction):
    global traded_stocks_dict, traded_list, traded_once_today, ohlc_dict, reverse_traded_once_today, missed_trade_reversal, capital, max_loss
    ltp = dictt[stock]['last_price']
    today_low = ohlc_dict[stock]["today_low"]
    today_high = ohlc_dict[stock]["today_high"]
    prev_low = df.loc[df['index'] == stock].iloc[0, 4]
    prev_high = df.loc[df['index'] == stock].iloc[0, 3]
    # print(stock, ltp)
    if direction and ltp >= today_high*1.00001 and stock not in traded_list and stock not in traded_once_today:  # stocks from low to high
        error = abs((ltp-today_high)/today_high)*100
        if error < 0.15:
            sl = max(today_low, prev_low)
            target = (prev_high+prev_low)/2
            risk_reward = abs((target-today_high)/(today_high-sl))
            if target <= ltp:
                # bot.sendMessage(chat_id=984101934,
                #                 text=f'{stock}\ncant trade, target already met')
                # missed_trade_reversal.append(stock)
                # traded_once_today.append(stock)
                previous_target = target
                sl = today_high*0.996
                target = today_high*1.01
                message = f'{stock}:\nprevious target was:\n{previous_target}\ntarget:\n{target}\nexpected_entry:\n{today_high}\nenter at price:\n{ltp}\nsl:\n{sl}\nerror\n{error}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_stocks_dict[stock] = {
                    "direction": "buy", "sl": sl, "target": target, "initial_value": ltp, 'quantity': math.floor(capital/ltp)}
                # print(traded_stocks_dict)
                traded_once_today.append(stock)
                traded_list.append(stock)
            elif risk_reward > 0.85:
                loss = abs(ltp-sl)
                quantity = math.floor(max_loss/loss)
                if quantity > 0:
                    message = f'{stock}:\ntarget:\n{target}\nexpected_entry:\n{today_high}\nenter at price:\n{ltp}\nsl:\n{sl}\nrr\n{risk_reward}\nerror\n{error}'
                    bot.sendMessage(chat_id=984101934, text=message)
                    traded_stocks_dict[stock] = {
                        "direction": "buy", "sl": sl, "target": target, "initial_value": ltp, 'quantity': quantity}
                    # print(traded_stocks_dict)
                    traded_once_today.append(stock)
                    traded_list.append(stock)
                else:
                    missed_trade_reversal.append(stock)
                    traded_once_today.append(stock)
                    message = f'{stock}:\nno trade stop loss too big, quantity 0'
                    bot.sendMessage(chat_id=984101934, text=message)
            else:
                missed_trade_reversal.append(stock)
                traded_once_today.append(stock)
                bot.sendMessage(chat_id=984101934,
                                text=f'{stock}\ncant trade,risk reward less\nrr\n{risk_reward}\nerror\n{error}')
        else:
            missed_trade_reversal.append(stock)
            traded_once_today.append(stock)
            message = f'{stock}:\nbig initial error, not safe to enter\n{error}'
            bot.sendMessage(chat_id=984101934, text=message)

    elif not direction and ltp <= today_low*0.99999 and stock not in traded_list and stock not in traded_once_today:  # stocks from high to low
        error = abs((ltp-today_low)/today_low)*100
        if error < 0.15:
            sl = min(today_high, prev_high)
            target = (prev_high+prev_low)/2
            risk_reward = abs((target-today_low)/(today_low-sl))
            if target >= ltp:
                # bot.sendMessage(chat_id=984101934,
                #                 text=f'{stock}\ncant trade, target already met')
                # missed_trade_reversal.append(stock)
                # traded_once_today.append(stock)
                previous_target = target
                sl = today_low*1.004
                target = today_low*0.99
                message = f'{stock}:\nprevious target was:\n{previous_target}\nsl:\n{sl}\nexpected_entry :\n{today_low}\nenter at price:\n{ltp}\ntarget:\n{target}\nerror\n{error}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_stocks_dict[stock] = {
                    "direction": "short", "sl": sl, "target": target, "initial_value": ltp, 'quantity': math.floor(capital/ltp)}
                # print(traded_stocks_dict)
                traded_once_today.append(stock)
                traded_list.append(stock)
            elif risk_reward > 0.85:
                loss = abs(ltp-sl)
                quantity = math.floor(max_loss/loss)
                if quantity > 0:
                    message = f'{stock}:\nsl:\n{sl}\nexpected_entry:\n{today_low}\nenter at price:\n{ltp}\ntarget:\n{target}\nrr\n{risk_reward}\nerror\n{error}'
                    bot.sendMessage(chat_id=984101934, text=message)
                    traded_stocks_dict[stock] = {
                        "direction": "short", "sl": sl, "target": target, "initial_value": ltp, 'quantity': quantity}
                    # print(traded_stocks_dict)
                    traded_once_today.append(stock)
                    traded_list.append(stock)
                else:
                    missed_trade_reversal.append(stock)
                    traded_once_today.append(stock)
                    message = f'{stock}:\nno trade stop loss too big, quantity 0'
                    bot.sendMessage(chat_id=984101934, text=message)
            else:
                missed_trade_reversal.append(stock)
                traded_once_today.append(stock)
                bot.sendMessage(chat_id=984101934,
                                text=f'{stock}\ncant trade,risk reward less\nrr\n{risk_reward}\nerror\n{error}')
        else:
            missed_trade_reversal.append(stock)
            traded_once_today.append(stock)
            message = f'{stock}:\nbig initial error, not safe to enter\n{error}'
            bot.sendMessage(chat_id=984101934, text=message)


def reversal_trade(dictt, stock, df, nifty, direction):
    global traded_stocks_dict, traded_list, traded_once_today, ohlc_dict, reverse_traded_once_today, missed_trade_reversal
    ltp = dictt[stock]['last_price']
    today_low = ohlc_dict[stock]["today_low"]
    today_high = ohlc_dict[stock]["today_high"]
    prev_low = df.loc[df['index'] == stock].iloc[0, 4]
    prev_high = df.loc[df['index'] == stock].iloc[0, 3]

    if direction and ltp <= today_low*0.99999 and stock not in reverse_traded_once_today:
        error = abs((ltp-today_low)/today_low)*100
        if error < 0.1:
            sl = today_low*1.004
            target = today_low*0.99
            message = f'reversal_trade\n{stock}:\nsl:\n{sl}\nexpected_entry :\n{today_low}\nenter at price:\n{ltp}\ntarget:\n{target}\nerror\n{error}'
            bot.sendMessage(chat_id=984101934, text=message)
            traded_stocks_dict[stock] = {
                "direction": "short", "sl": sl, "target": target, "initial_value": ltp, 'quantity': math.floor(5000/ltp)}
            # print(traded_stocks_dict)
            if stock in missed_trade_reversal:
                missed_trade_reversal.remove(stock)
            reverse_traded_once_today.append(stock)
            traded_list.append(stock)
        else:
            if stock in missed_trade_reversal:
                missed_trade_reversal.remove(stock)
            reverse_traded_once_today.append(stock)
            message = f'reversal_trade\n{stock}:\nbig error\n{error}'
            bot.sendMessage(chat_id=984101934, text=message)

    elif not direction and ltp >= today_high*1.00001 and stock not in reverse_traded_once_today:
        error = abs((ltp-today_high)/today_high)*100
        if error < 0.1:
            sl = today_high*0.996
            target = today_high*1.01
            message = f'reversal_trade\n{stock}:\ntarget:\n{target}\nexpected_entry:\n{today_high}\nenter at price:\n{ltp}\nsl:\n{sl}\nerror\n{error}'
            bot.sendMessage(chat_id=984101934, text=message)
            traded_stocks_dict[stock] = {
                "direction": "buy", "sl": sl, "target": target, "initial_value": ltp, 'quantity': math.floor(5000/ltp)}
            # print(traded_stocks_dict)
            if stock in missed_trade_reversal:
                missed_trade_reversal.remove(stock)
            reverse_traded_once_today.append(stock)
            traded_list.append(stock)
        else:
            if stock in missed_trade_reversal:
                missed_trade_reversal.remove(stock)
            reverse_traded_once_today.append(stock)
            message = f'reversal_trade\n{stock}:\nbig error\n{error}'
            bot.sendMessage(chat_id=984101934, text=message)


def the_main(df, nifty, tracking_stocks, stock_direction_dict):
    global entry_status, traded_list, missed_trade_reversal
    if len(tracking_stocks) == 0:
        print("no_data for today")
        entry_status = 1
    else:
        dictt = kite.quote(tracking_stocks)
        for stock in list(dictt.keys()):
            if stock in traded_list or stock in missed_trade_reversal:
                check_trade_status(dictt, stock, df, nifty,
                                   stock_direction_dict[stock])
            else:
                live_track_opportunity(
                    dictt, stock, df, nifty, stock_direction_dict[stock])


def fetch_initial_data():
    global today_pnl, traded_list, traded_stocks_dict, traded_once_today, entry_status, ohlc_dict, reverse_traded_once_today, missed_trade_reversal
    missed_trade_reversal = []
    reverse_traded_once_today = []
    traded_list = []
    traded_once_today = []
    ohlc_dict = {}
    traded_stocks_dict = {}
    today_pnl = 0
    df = pd.read_sql('SELECT * FROM top_changers_data', con=connection)
    if df.empty:
        entry_status = 1
        bot.sendMessage(chat_id=984101934,
                        text="your database is apprently empty")
        raise Exception("your database is apprently empty")
    df = df = df.loc[(df["change"] >= 3) | (df["change"] <= -3)]
    nifty = get_nifty_direction()
    print("nyfty is", nifty)
    stock_direction_dict = gap_valid_stock_list(df)
    tracking_stocks = list(stock_direction_dict.keys())
    bot.sendMessage(chat_id=984101934,
                    text=f'stocks for today\ntotal stocks{len(tracking_stocks)}'+str(tracking_stocks))
    return df, nifty, tracking_stocks, stock_direction_dict


while True:
    now = datetime.datetime.now()
    # utc  #start when 1st 5 min candle has been formed
    starttime = now.replace(hour=3, minute=49, second=51, microsecond=0)
    endtime = now.replace(hour=9, minute=0, second=0, microsecond=0)
    # starttime = now.replace(hour=0, minute=0, second=57, microsecond=0)  # ist
    # endtime = now.replace(hour=23, minute=12, second=0, microsecond=0)
    if starttime <= now < endtime and datetime.datetime.today().weekday() not in [5, 6]:
        if not entry_status:
            try:
                if not database_flag:
                    try:
                        print("getting engine")
                        engine = create_engine(
                            'mysql://admin:password@database-restored1.cninlcvpipmk.us-east-1.rds.amazonaws.com/stocks', echo=False)
                        connection = engine.raw_connection()

                        mydb = mysql.connector.connect(
                            host="database-restored1.cninlcvpipmk.us-east-1.rds.amazonaws.com", user="admin", passwd="password", database="stocks")
                        time.sleep(2)
                        mycursor = mydb.cursor()
                        database_flag = 1
                    except Exception as e:
                        bot.sendMessage(chat_id=984101934,
                                        text="database instance error")
                        print(e)
                if not login_flag:
                    print('b')
                    try:
                        get_login(api_k, api_s)
                        df, nifty, tracking_stocks, stock_direction_dict = fetch_initial_data()
                        login_flag = 1
                    except Exception as e:
                        print(e)
                if login_flag and now >= now.replace(hour=3, minute=58, second=51, microsecond=0):
                    the_main(df, nifty, tracking_stocks, stock_direction_dict)
                    exit_value = 1
                    # pdb.set_trace()
                    # time.sleep(0.01)
                    # print("sleeping for 5 secs..")
            except Exception as e:
                bot.sendMessage(chat_id=984101934,
                                text="error occured in the while loop")
                print(e)
    else:
        if exit_value:
            print("exit from the main loop")
            message = f'open positions\n{traded_list}'
            bot.sendMessage(chat_id=984101934, text=message)
            message = f'total\n{total_pnl}\ntodays\n{today_pnl}'
            bot.sendMessage(chat_id=984101934, text=message)
        login_flag = 0
        database_flag = 0
        entry_status = 0
        exit_value = 0
