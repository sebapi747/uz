import requests, re
from bs4 import BeautifulSoup
from lxml import html
import datetime as dt
import os, os.path, glob, time
import numpy as np
import pandas as pd
import sys
import sqlite3
import matplotlib.pyplot as plt
import config
dirname = os.path.dirname(sys.argv[0])
if dirname != '':
    os.chdir(dirname)
remotedir = config.remotedir
DATABASE= "quotes.db"

def get_metadata():
    return {'Creator':os.uname()[1] +":"+__file__+":"+str(dt.datetime.utcnow())}
    
def sendTelegram(text):
    prefix = os.uname()[1] + __file__ + ":"
    params = {'chat_id': config.telegramchatid, 'text': prefix+text, 'parse_mode': 'HTML'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
''' ---------------------------------------------------------------------------------------------
 SQLite utils
'''
class DBObj(object):
    def __init__(self):
        self.db = sqlite3.connect(DATABASE)

g = DBObj()

def cursor_col_names(cursor):
    return [description[0] for description in cursor.description]

def insert_df_to_table(df, tablename, cols):
    df[cols].to_sql(name=tablename+'_tmp', con=g.db, if_exists='replace',index=False)
    sql = 'insert or replace into '+tablename+' ('+','.join(cols)+') select '+','.join(cols)+' from '+tablename+'_tmp'
    g.db.execute(sql)
    g.db.execute('drop table '+tablename+'_tmp')
    g.db.commit()

def init_sql_schema():
    print("init sql tables")
    f = open("sql/schema.sql", mode='r') 
    g.db.cursor().executescript(f.read())
    f.close()
    g.db.commit()

# get isins for summary pages
def getAllIsins():
    detailslink = []
    for i in range(1,4):
        url = 'https://uzse.uz/isu_infos?locale=en&page=%d' % i
        x = requests.get(url, verify=False)
        if x.status_code!=200:
            msg = "ERROR:"+url
            sendTelegram(msg)
            raise Exception(msg)
        print("page %d %d" % (i,x.status_code))
        soup = BeautifulSoup(x.text, 'html.parser')
        for tag in set(soup.find_all('a')):
            urllink = tag.get('href')
            if re.match(r"/isu_infos/.*/detail", urllink):
                detailslink.append(urllink)
    detailslink = list(set(detailslink))
    print(len(detailslink))
    return detailslink

# get details info for given link
def getDetailsInfo(d,datestring=None):
    isin = d[11:].replace(r"/detail","")
    filename = "html/"+datestring+"/"+isin+".html"
    if os.path.exists(filename):
        file = open(filename, "r")
        text = file.read()
        file.close()
        return text, isin
    else:
        url = 'https://uzse.uz' + d + '?locale=en'
        x = requests.get(url, verify=False)
        time.sleep(1)
        if x.status_code!=200:
            msg = "ERROR: url=%s code=%d"%(url,x.status_code)
            sendTelegram(msg)
            raise Exception(msg)
        if datestring is not None:
            file = open(filename, "w")
            file.write(x.text)
            file.close()
        return x.text, isin

def to_scalar(x):
    return float(x.replace(",","").replace(" ","").replace("\n","").replace("\t",""))
def getAllData():
    ymdstr = dt.datetime.strftime(dt.datetime.utcnow(),'%Y%m%d')
    #ymdstr = "latest"
    dirname = os.path.dirname(sys.argv[0])
    if dirname != '':
        os.chdir(dirname)
    os.system('mkdir -p csv')
    os.system('mkdir -p html/'+ymdstr)
    isins = []
    prices = []
    dates = []
    marketcaps = []
    names = []
    for d in getAllIsins():
        try:
            htmltext, isin = getDetailsInfo(d, ymdstr)
            parsed_body=html.fromstring(htmltext)
            td = parsed_body.xpath('//table[1]/thead/tr[3]/td/text()')
            price = to_scalar(td[10])
            date = dt.date(*map(int, td[11].split('-')))
            marketcap = to_scalar(td[13])
            th = parsed_body.xpath('//table[1]/thead/tr/th[2]/text()')
            name = th[0].replace("\n","").replace("  ","",-1).replace("<","(").replace(">",")")
            # append
            prices.append(price)
            dates.append(date) 
            marketcaps.append(marketcap) 
            isins.append(isin)
            names.append(name)
        except Exception as e:
            print("ERROR: %s failed for %s %s" % (str(e),isin,str(td)))
    os.system("rm -rf html/"+ymdstr)
    df = pd.DataFrame({'isin':isins, 'name':names, 'price':prices, 'date':dates, 'marketcap':marketcaps})
    print("INFO:found %d isin" % len(df))
    df.sort_values(by="marketcap", ascending=False).to_csv("csv/mktcap-%s.csv" % "latest", index=False) # ymdstr
    #insert_df_to_table(df, 'quotes', list(df.columns))

def insertfx():
    try:
        print("INFO: inserting USDUZS fx")
        headers = {'accept':'*/*', 'user-agent': 'Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Raspbian Chromium/78.0.3904.108 Chrome/78.0.3904.108 Safari/537.36'}
        #x = requests.get('https://finance.yahoo.com/quote/UZS=X/', headers=headers)
        #print(x.status_code)
        #parsed_body=html.fromstring(x.text)
        #fx = float(parsed_body.xpath('//div/fin-streamer[@data-symbol="%s"]/text()' % "UZS=X")[0].replace(",",""))
        #fx = float(parsed_body.xpath("//fin-streamer[@data-symbol='%s' and @data-field='regularMarketPrice']/@data-value" % "UZS=X")[0].replace(",",""))
        #fx = float(parsed_body.xpath('//div/fin-streamer[@data-reactid=29]/text()')[0].replace(",",""))
        x = requests.get("https://www.google.com/search?q=1+usd+in+uzs", headers=headers)
        print(x.status_code)
        parsed_body=html.fromstring(x.text)
        fx = float(parsed_body.xpath("//span/@data-value")[0])
        ymdstr = dt.datetime.strftime(dt.datetime.utcnow(),'%Y-%m-%d')
        print("INFO: date=%s fx=%f" % (ymdstr, fx))
        g.db.execute('insert into fx (cob, fx) values (?, ?)', [ymdstr, fx])
        g.db.commit()
    except:
        sendTelegram("ERR: failed to get UZS fx")
        pass
        
def readcsv_to_db():
    filelist = glob.glob('csv/*.csv')
    for f in sorted(filelist):
        df = pd.read_csv(f)
        print("inserting %s - %d quotes" % (f, len(df)))
        insert_df_to_table(df, "quotes", list(df.columns))
    
def pltMostLiquid():
    print("produce html with most liquid stocks")
    with open('uzbek-mostliquid.html', 'w') as f:
        with open('uzbek-mostliquid-mktcap.html', 'w') as f2:
            with open('uzbek-mostliquid-usd.html', 'w') as f3:
                dates = pd.read_sql_query("select date from quotes order by date desc", g.db)
                dates = sorted(set(dates['date']))
                prevdate = dates[-20]
                print('<h1>Prices for most liquid uzbek stocks on %s</h1>' % dates[-1], file=f)
                print('<h1>Mkt cap for most liquid uzbek stocks on %s</h1>' % dates[-1], file=f2)
                dates[-1]
                mostliquid = pd.read_sql_query("select isin, name, sum(date) from quotes group by 1,2 order by 3 desc limit 14", g.db)
                if len(mostliquid)==0:
                    return
                m = mostliquid.iloc[0]
                for i in range(0,len(mostliquid)):
                    m = mostliquid.iloc[i]
                    df = pd.read_sql_query("select date, price, marketcap/(1.0*fx) as marketcap, price/(1.0*fx) as price_usd from quotes, fx where isin=? and date>? and date=cob", g.db, params=[m['isin'],"2020-01-01"])
                    name = re.sub('\).*', '',re.sub('.*\(', '', m['name']))
                    df['date'] = pd.to_datetime(df['date'])
                    fig = plt.figure(1)
                    #plt.gca().xaxis.set_major_locator(plt.MultipleLocator(10))
                    plt.gca().axes.set_ylim([0,np.max(df['price'])])
                    plt.xticks(rotation='vertical')
                    plt.plot(df['date'],df['price'])
                    plt.ylabel('price (UZS)')
                    plt.gcf().autofmt_xdate()
                    plt.title("%s\n%s" % (name, "last:"+str(np.max(df['date']))[:10]))
                    plt.savefig('svg/'+ m['isin']+'-price.svg',metadata=get_metadata())
                    #plt.show()
                    plt.close(fig)
                    fig = plt.figure(1)
                    #plt.gca().xaxis.set_major_locator(plt.MultipleLocator(10))
                    plt.gca().axes.set_ylim([0,np.max(df['marketcap'])])
                    plt.xticks(rotation='vertical')
                    plt.plot(df['date'],df['marketcap'])
                    plt.ylabel('market cap (USD)')
                    plt.gcf().autofmt_xdate()
                    plt.title("%s\n%s" % (name, "last:"+str(np.max(df['date']))[:10]))
                    plt.savefig('svg/'+ m['isin']+'-mktcap.svg',metadata=get_metadata())
                    #plt.show()
                    plt.close(fig)
                    fig = plt.figure(1)
                    #plt.gca().xaxis.set_major_locator(plt.MultipleLocator(10))
                    plt.gca().axes.set_ylim([0,np.max(df['price_usd'])])
                    plt.xticks(rotation='vertical')
                    plt.plot(df['date'],df['price_usd'])
                    plt.ylabel('price (USD)')
                    plt.gcf().autofmt_xdate()
                    plt.title("%s\n%s" % (name, "last:"+str(np.max(df['date']))[:10]))
                    plt.savefig('svg/'+ m['isin']+'-priceusd.svg',metadata=get_metadata())
                    #plt.show()
                    plt.close(fig)
                    print('<img src="svg/%s-price.svg" width="40%%" />' % m['isin'], file=f)
                    print('<img src="svg/%s-mktcap.svg" width="40%%" />' % m['isin'], file=f2)
                    print('<img src="svg/%s-priceusd.svg" width="40%%" />' % m['isin'], file=f)

def copy_to_web():
    print("copy to web")
    os.system('rsync -avzhe ssh svg %s' % remotedir)
    os.system('rsync -avzhe ssh uzbek*.html %s' % remotedir)

insertfx()
getAllData()
readcsv_to_db()
pltMostLiquid()
copy_to_web()
#init_sql_schema()
