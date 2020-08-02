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

api_k = "nq31y6ndsyyqwdbc"  # id
api_s = "5qmzojs1srptl5bhrgz1k13kyjip9tfu"  # pass

traded_list = []
traded_once_today = []
traded_stocks_dict = {}
total_pnl = 0
today_pnl = 0

login_flag = 0
database_flag = 0

print('getting bot')
# telegram_token = '1191509580:AAEOGMA0bfLcEQ86XBXKufBdg9HKbCJtoLM' #calendar spread
telegram_token = '1261533555:AAEiTh6CDxSMI6MrLShXqhMHM1fziny_Fkg'  # 180 degree
bot = telegram.Bot(token=telegram_token)


def buy_order_market(name, quantity):
    print("placing order with name quantity sl price", name, quantity)
    kite.place_order(tradingsymbol=name, variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                     transaction_type=kite.TRANSACTION_TYPE_BUY, quantity=quantity, order_type=kite.ORDER_TYPE_MARKET, product=kite.PRODUCT_MIS)
    # trd_portfolio[inst_of_single_company]["order_ids"] = order_id


def sell_order_market(name, quantity):
    print("placing order with name quantity sl price", name, quantity)
    kite.place_order(tradingsymbol=name, variety=kite.VARIETY_REGULAR, exchange=kite.EXCHANGE_NSE,
                     transaction_type=kite.TRANSACTION_TYPE_SELL, quantity=quantity, order_type=kite.ORDER_TYPE_MARKET, product=kite.PRODUCT_MIS)
    # trd_portfolio[inst_of_single_company]["order_ids"] = order_id


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


def liquidity_check(stock):
    data = kite.quote(stock)
    sell_price = data[stock]['depth']['buy'][0]['price']
    buy_price = data[stock]['depth']['sell'][0]['price']
    if sell_price != 0 and buy_price != 0:
        difference = sell_price-buy_price
        percentage = abs((difference)/buy_price*100)
        if percentage > 0.4:
            bot.sendMessage(chat_id=984101934,
                            text=f'{stock}\nnot liquid\n{percentage}')
            return False
        else:
            bot.sendMessage(chat_id=984101934,
                            text=f'{stock}\nliquid\n{percentage}')
            return True
    else:
        bot.sendMessage(chat_id=984101934, text=f'{stock}\ncircuit')
        return True


def gap_valid_stock_list(df, nifty):
    global bot
    print("checking gap validation")
    eligible_list = []
    final_list = []
    if nifty:
        eligible_df = df.loc[df['change'] < 0]
        eligible_list = list(eligible_df.iloc[:, 1])
        traded_stocks_dict = kite.quote(eligible_list)
        for stock in list(traded_stocks_dict.keys()):
            open_price = traded_stocks_dict[stock]['ohlc']['open']
            low = eligible_df.loc[eligible_df['index'] == stock].iloc[0, 4]
            gap = abs((low-open_price)/low*100)
            if open_price < low:
                if gap <= 3:
                    if liquidity_check(stock):
                        final_list.append(stock)
                else:
                    # f'{stock}\nopen price\n{open_price}\nprevious_low{low}'
                    bot.sendMessage(
                        chat_id=984101934, text=f'{stock}\ngap to big to trade\n{gap}')
    else:
        eligible_df = df.loc[df['change'] > 0]
        eligible_list = list(eligible_df.iloc[:, 1])
        traded_stocks_dict = kite.quote(eligible_list)
        for stock in list(traded_stocks_dict.keys()):
            open_price = traded_stocks_dict[stock]['ohlc']['open']
            high = eligible_df.loc[eligible_df['index'] == stock].iloc[0, 3]
            gap = abs((high-open_price)/high*100)
            if open_price > high:
                if gap <= 3:
                    if liquidity_check(stock):
                        final_list.append(stock)
                else:
                    bot.sendMessage(
                        chat_id=984101934, text=f'{stock}\ngap to big to trade\n{gap}')
    return final_list


def check_trade_status(stock, dictt):
    global total_pnl, today_pnl, traded_stocks_dict
    ltp = dictt[stock]['last_price']
    initial_value = traded_stocks_dict[stock]['initial_value']
    target = traded_stocks_dict[stock]['target']
    sl = traded_stocks_dict[stock]['sl']
    name = stock.split(':')[1]
    quantity = traded_stocks_dict[stock]['quantity']
    if traded_stocks_dict[stock]['direction'] == "buy":
        if ltp >= target:
            try:
                profit = quantity*abs(ltp-initial_value)
                total_pnl += profit
                today_pnl += profit
                sell_order_market(name, quantity)
                message = f'{stock}\nbook profit\n{profit}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_list.remove(stock)
                print("profit")
            except:
                bot.sendMessage(chat_id=984101934,
                                text='error while exiting buy postion')
        elif ltp <= sl:
            try:
                loss = -quantity*abs(initial_value-ltp)
                total_pnl += loss
                today_pnl += loss
                sell_order_market(name, quantity)
                message = f'{stock}\nbook loss\n{loss}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_list.remove(stock)
                print("loss")
            except:
                bot.sendMessage(chat_id=984101934,
                                text='error while exiting buy postion')
    else:
        # print("sold")
        if ltp <= target:
            try:
                profit = quantity*abs(ltp-initial_value)
                total_pnl += profit
                today_pnl += profit
                buy_order_market(name, quantity)
                message = f'{stock}\nbook profit\n{profit}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_list.remove(stock)
                print('profit')
            except:
                bot.sendMessage(chat_id=984101934,
                                text='error while exiting short postion')
        elif ltp >= sl:
            try:
                loss = -quantity*abs(initial_value-ltp)
                total_pnl += loss
                today_pnl += loss
                buy_order_market(name, quantity)
                message = f'{stock}\nbook loss\n{loss}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_list.remove(stock)
                print("loss")
            except:
                bot.sendMessage(chat_id=984101934,
                                text='error while exiting short postion')


def live_track_opportunity(dictt, stock, df, nifty):
    global traded_list, traded_stocks_dict, traded_once_today, capital, max_loss
    ltp = dictt[stock]['last_price']
    today_low = dictt[stock]['ohlc']['low']
    today_high = dictt[stock]['ohlc']['high']
    prev_low = df.loc[df['index'] == stock].iloc[0, 4]
    prev_high = df.loc[df['index'] == stock].iloc[0, 3]
    name = stock.split(':')[1]

    if nifty and ltp >= prev_low*1.00001 and stock not in traded_list and stock not in traded_once_today:  # stocks from low to high
        sl = today_low
        target = (prev_high+prev_low)/2
        risk_reward = abs((target-ltp)/(ltp-sl))
        error = abs((ltp-prev_low)/prev_low)*100

        if error <= 0.3 and risk_reward > 1 and risk_reward <= 3:
            loss = abs(ltp-sl)
            quantity = math.floor(max_loss/loss)
            if quantity > 0:
                if target <= ltp:
                    bot.sendMessage(chat_id=984101934,
                                    text=f'{stock}\ncant trade, target already met\n{target}\nexpected_entry:\n{prev_low}\nenter at price:\n{ltp}\nsl:\n{sl}')
                    traded_once_today.append(stock)
                else:
                    try:
                        message = f'{stock}:\ntarget:\n{target}\nexpected_entry:\n{prev_low}\nenter at price:\n{ltp}\nsl:\n{sl}\nrr\n{risk_reward}\n{error}'
                        bot.sendMessage(chat_id=984101934, text=message)
                        buy_order_market(name, quantity)
                        traded_stocks_dict[stock] = {
                            "direction": "buy", "sl": sl, "target": target, "initial_value": ltp, 'quantity': quantity}
                        print(traded_stocks_dict)
                        traded_once_today.append(stock)
                        traded_list.append(stock)
                    except Exception as e:
                        traded_once_today.append(stock)
                        message = f'stock:{stock}\nerror occed while placing buy order'
                        bot.sendMessage(chat_id=984101934, text=message)
                        print(message)
            else:
                message = f'stock:{stock}\nout of budget,quantity 0'
                bot.sendMessage(chat_id=984101934, text=message)
                print(message)
        else:
            traded_once_today.append(stock)
            message = f'{stock}:\nbig error or\n{error}\nrr\n{risk_reward}'
            bot.sendMessage(chat_id=984101934, text=message)

    elif not nifty and ltp <= prev_high*0.99999 and stock not in traded_list and stock not in traded_once_today:  # stocks from high to low
        sl = today_high
        target = (prev_high+prev_low)/2
        risk_reward = abs((target-ltp)/(ltp-sl))
        error = abs((ltp-prev_high)/prev_high)*100

        if error <= 0.3 and risk_reward > 1 and risk_reward <= 3:
            loss = abs(ltp-sl)
            quantity = math.floor(max_loss/loss)
            if quantity > 0:
                if target >= ltp:
                    bot.sendMessage(chat_id=984101934,
                                    text=f'{stock}\ncant trade, target already met\n{target}\nexpected_entry:\n{prev_high}\nenter at price:\n{ltp}\ntarget:\n{target}')
                    traded_once_today.append(stock)
                else:
                    try:
                        message = f'{stock}:\nsl:\n{sl}\nexpected_entry:\n{prev_high}\nenter at price:\n{ltp}\ntarget:\n{target}\nrr\n{risk_reward}\n{error}'
                        bot.sendMessage(chat_id=984101934, text=message)
                        sell_order_market(name, quantity)
                        traded_stocks_dict[stock] = {
                            "direction": "short", "sl": sl, "target": target, "initial_value": ltp, 'quantity': quantity}
                        print(traded_stocks_dict)
                        traded_once_today.append(stock)
                        traded_list.append(stock)
                    except Exception as e:
                        traded_once_today.append(stock)
                        message = f'stock:{stock}\neurrror occed while placing sell order'
                        bot.sendMessage(chat_id=984101934, text=message)
                        print(message)
            else:
                message = f'stock:{stock}\nout of budget,quantity 0'
                bot.sendMessage(chat_id=984101934, text=message)
                print(message)
        else:
            traded_once_today.append(stock)
            message = f'{stock}:\nbig error or\n{error}\nrr\n{risk_reward}'
            bot.sendMessage(chat_id=984101934, text=message)


def the_main(df, nifty, tracking_stocks):
    global entry_status, traded_list
    if len(tracking_stocks) == 0:
        print("no_data for today")
        entry_status = 1
    else:
        dictt = kite.quote(tracking_stocks)
        for stock in list(dictt.keys()):
            if stock in traded_list:
                check_trade_status(stock, dictt)
            else:
                live_track_opportunity(dictt, stock, df, nifty)


def fetch_initial_data():
    global today_pnl, traded_list, traded_stocks_dict, traded_once_today, entry_status
    traded_once_today = []
    traded_stocks_dict = {}
    traded_list = []
    today_pnl = 0
    df = pd.read_sql('SELECT * FROM top_changers_500_data', con=connection)
    print(df.shape)
    if df.empty:
        entry_status = 1
        bot.sendMessage(chat_id=984101934,
                        text="your database is apprently empty")
        raise Exception("your database is apprently empty")
    df = df = df.loc[(df["change"] >= 4) | (df["change"] <= -4)]
    nifty = get_nifty_direction()
    print("nyfty is", nifty)
    tracking_stocks = gap_valid_stock_list(df, nifty)
    no_of_stocks = len(tracking_stocks)
    if no_of_stocks < 2:
        entry_status = 1
        bot.sendMessage(chat_id=984101934,
                        text=f'no trading, less no. of stocks {no_of_stocks}')
        raise Exception(f'no trading, less no. of stocks {no_of_stocks}')
    bot.sendMessage(chat_id=984101934,
                    text=f'stocks for today\ntotal stocks {no_of_stocks}'+str(tracking_stocks))
    return df, nifty, tracking_stocks


def calc_open_profit(open_position_list):
    global total_pnl, today_pnl, traded_stocks_dict, traded_list
    dictt = kite.quote(open_position_list)
    for stock in list(dictt.keys()):
        ltp = dictt[stock]['last_price']
        initial_value = traded_stocks_dict[stock]['initial_value']
        quantity = traded_stocks_dict[stock]['quantity']
        if traded_stocks_dict[stock]['direction'] == "buy":
            if ltp > initial_value:
                profit = quantity*abs(ltp-initial_value)
                total_pnl += profit
                today_pnl += profit
                message = f'{stock}\nbook profit\n{profit}\nltp\n{ltp}\nexit_value\n{initial_value}\nquantity\n{quantity}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_list.remove(stock)
                print("profit")
            if ltp <= initial_value:
                loss = -quantity*abs(initial_value-ltp)
                total_pnl += loss
                today_pnl += loss
                message = f'{stock}\nbook loss\n{loss}\nltp\n{ltp}\nexit_value\n{initial_value}\nquantity\n{quantity}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_list.remove(stock)
                print("loss")
        else:
            if ltp < initial_value:
                profit = quantity*abs(ltp-initial_value)
                total_pnl += profit
                today_pnl += profit
                message = f'{stock}\nbook profit\n{profit}\nltp\n{ltp}\nexit_value\n{initial_value}\nquantity\n{quantity}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_list.remove(stock)
                print('profit')
            if ltp >= initial_value:
                loss = -quantity*abs(initial_value-ltp)
                total_pnl += loss
                today_pnl += loss
                message = f'{stock}\nbook loss\n{loss}\nltp\n{ltp}\nexit_value\n{initial_value}\nquantity\n{quantity}'
                bot.sendMessage(chat_id=984101934, text=message)
                traded_list.remove(stock)
                print("loss")


while True:
    now = datetime.datetime.now()
    starttime = now.replace(hour=3, minute=45, second=30, microsecond=0)  # utc
    endtime = now.replace(hour=9, minute=0, second=0, microsecond=0)
    # starttime = now.replace(hour=0, minute=0, second=57, microsecond=0)  # ist
    # endtime = now.replace(hour=23, minute=50, second=0, microsecond=0)
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
                        df, nifty, tracking_stocks = fetch_initial_data()
                        login_flag = 1
                        exit_value = 1
                    except Exception as e:
                        print(e)
                if login_flag:
                    the_main(df, nifty, tracking_stocks)
                    # pdb.set_trace()
                    # time.sleep(0.1)
                    # print("sleeping for 5 secs..")
            except Exception as e:
                bot.sendMessage(chat_id=984101934,
                                text='error occured in the while loop')
                print(e)
    else:
        if exit_value:
            print("exit from the main loop")
            message = f'open positions\n{traded_list}'
            bot.sendMessage(chat_id=984101934, text=message)
            if traded_list:
                calc_open_profit(traded_list)
            message = f'total\n{total_pnl}\ntodays\n{today_pnl}'
            bot.sendMessage(chat_id=984101934, text=message)
        login_flag = 0
        database_flag = 0
        entry_status = 0
        exit_value = 0
