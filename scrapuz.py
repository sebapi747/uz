import requests, re
from bs4 import BeautifulSoup
from lxml import html
import datetime as dt
import os, os.path, glob
import numpy as np
import pandas as pd
import sys
import sqlite3
import matplotlib.pyplot as plt


dirname = os.path.dirname(sys.argv[0])
if dirname != '':
    os.chdir(dirname)
DATABASE= "quotes.db"

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
        x = requests.get('https://uzse.uz/isu_infos?locale=en&page=%d' % i)
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
    x = requests.get('https://uzse.uz' + d + '?locale=en')
    print(x.status_code)
    if datestring is not None:
        file = open("html/"+datestring+"/"+isin+".html", "w")
        file.write(x.text)
        file.close()
    return x.text, isin

def getAllData():
    ymdstr = dt.datetime.strftime(dt.datetime.utcnow(),'%Y%m%d')
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
        htmltext, isin = getDetailsInfo(d)
        parsed_body=html.fromstring(htmltext)
        try:
            td = parsed_body.xpath('//table[1]/thead/tr[3]/td/text()')
            price = float(td[10].replace(",",""))
            date = dt.date(*map(int, td[11].split('-')))
            marketcap = float(td[13].replace(",",""))
            th = parsed_body.xpath('//table[1]/thead/tr/th[2]/text()')
            name = th[0].replace("\n","").replace("  ","",-1).replace("<","(").replace(">",")")
            # append
            prices.append(price)
            dates.append(date) 
            marketcaps.append(marketcap) 
            isins.append(isin)
            names.append(name)
        except:
            print("failed for %s" % isin)
    df = pd.DataFrame({'isin':isins, 'name':names, 'price':prices, 'date':dates, 'marketcap':marketcaps})
    print("found %d isin" % len(df))
    df.sort_values(by="marketcap", ascending=False).to_html("html/mktcap-%s.html" % ymdstr, index=False)
    df.sort_values(by="marketcap", ascending=False).to_csv("csv/mktcap-%s.csv" % ymdstr, index=False)
    #insert_df_to_table(df, 'quotes', list(df.columns))

def insertfx():
    print("inserting USDUZS fx")
    x = requests.get('https://finance.yahoo.com/quote/USDUZS=X/')
    print(x.status_code)
    fx = float(parsed_body.xpath('//div/span[@data-reactid=32]/text()')[0].replace(",",""))
    ymdstr = dt.datetime.strftime(dt.datetime.utcnow(),'%Y-%m-%d')
    g.db.execute('insert into fx (cob, fx) values (?, ?)', [ymdstr, fx])
    g.db.commit()
    
def readcsv_to_db():
    insertfx()
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
                mostliquid = pd.read_sql_query("select isin, count(*) as ct, name, avg(marketcap) as cap from quotes where date>? group by 1,3 having ct>18 order by 2 desc,4 desc", g.db, params=[prevdate])
                m = mostliquid.iloc[0]
                for i in range(0,len(mostliquid)):
                    m = mostliquid.iloc[i]
                    df = pd.read_sql_query("select date, price, marketcap/(1.0*fx) as marketcap, price/(1.0*fx) as price_usd from quotes, fx where isin=? and date>? and date=cob", g.db, params=[m['isin'],"2020-01-01"])
                    name = re.sub('\).*', '',re.sub('.*\(', '', m['name']))
                    fig = plt.figure(1)
                    plt.gca().xaxis.set_major_locator(plt.MultipleLocator(10))
                    plt.gca().axes.set_ylim([0,np.max(df['price'])])
                    plt.xticks(rotation='vertical')
                    plt.plot(df['date'],df['price'])
                    plt.ylabel('price (UZS)')
                    plt.title(name)
                    plt.savefig('svg/'+ m['isin']+'-price.svg')
                    #plt.show()
                    plt.close(fig)
                    fig = plt.figure(1)
                    plt.gca().xaxis.set_major_locator(plt.MultipleLocator(10))
                    plt.gca().axes.set_ylim([0,np.max(df['marketcap'])])
                    plt.xticks(rotation='vertical')
                    plt.plot(df['date'],df['marketcap'])
                    plt.ylabel('market cap (USD)')
                    plt.title(name)
                    plt.savefig('svg/'+ m['isin']+'-mktcap.svg')
                    #plt.show()
                    plt.close(fig)
                    fig = plt.figure(1)
                    plt.gca().xaxis.set_major_locator(plt.MultipleLocator(10))
                    plt.gca().axes.set_ylim([0,np.max(df['price_usd'])])
                    plt.xticks(rotation='vertical')
                    plt.plot(df['date'],df['price_usd'])
                    plt.ylabel('price (USD)')
                    plt.title(name)
                    plt.savefig('svg/'+ m['isin']+'-priceusd.svg')
                    #plt.show()
                    plt.close(fig)
                    print('<img src="svg/%s-price.svg" width="40%%" />' % m['isin'], file=f)
                    print('<img src="svg/%s-mktcap.svg" width="40%%" />' % m['isin'], file=f2)
                    print('<img src="svg/%s-priceusd.svg" width="40%%" />' % m['isin'], file=f)

def copy_to_web():
    print("copy to web")
    os.system('rsync -avzhe ssh svg sebapi747@ssh.pythonanywhere.com:/home/sebapi747/marko/static/pics')
    os.system('rsync -avzhe ssh uzbek*.html sebapi747@ssh.pythonanywhere.com:/home/sebapi747/marko/static/pics')

getAllData()
readcsv_to_db()
pltMostLiquid()
copy_to_web()
#init_sql_schema()
