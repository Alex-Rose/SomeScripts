from bs4 import BeautifulSoup
from urllib import quote_plus
import urllib2
import re
import MySQLdb
import unicodedata
import json
import argparse

parser = argparse.ArgumentParser(prog="Ski")
parser.add_argument('-v', dest='verbose', default=False, action='store_true', help='verbose')
parser.add_argument('-i', dest='interactive', default=False, action='store_true', help='interactive session for fixing DB')
args = parser.parse_args()

def output(msg):
    if args.verbose:
        print msg


def find_meteo(name):
    try:
        name = urllib2.quote(name.encode('utf-8'))
        url = "http://www.meteomedia.com/ca/search?q={!s}&lat=45.5067&lon=-73.5556".format(name)
        output(url)
        
        response = urllib2.urlopen(url)
        html = response.read()
        response.close()

        soup = BeautifulSoup(html)
        ski = soup.find_all("li", class_="ski")
        
        result = ski[0].find_next_sibling("li", class_="result")
       
        a = result.find_next("a")
        
        found = a.contents[0]
        
        lst = a['href'].split('/')
        id = lst[-1]
        # output(id)
        return (found, id)
    except:
        return (None, None)

response = urllib2.urlopen('http://www.maneige.com/fr/stations/conditions-de-neige.html?region=-1')
#response = urllib2.urlopen('http://www.polymtl.ca')
html = response.read()
response.close()

soup = BeautifulSoup(html)

#Open a database connection
db = MySQLdb.connect(host="localhost", user="alexroseroot", passwd="tydEbvuldInt", db="alexrose", charset="utf8") 

# you must create a Cursor object. It will let
#  you execute all the query you need
cur = db.cursor() 
    
#Loop for each station
for tag in soup.find_all("h3", class_="conditionsNomStation"):
    name = tag.contents[0].contents[0]
    output(name)
    
    update = tag.find_next("span").string
    
    # update = updateSpan[6:]
    # update = updateSpan[:7]
    #print update
    
    #meteo
    bloc = tag.find_next_sibling("div", class_="bloc-meteo")
    acc = bloc.find_next("div", class_="accumulations-neige clearfix")
    
    for col in acc.children:
        re.findall(r'\d+ cm', unicode(col))
        
    acc24 = re.findall(r'\d+ cm', unicode(acc.contents[1]))[0]  
    acc48 = re.findall(r'\d+ cm', unicode(acc.contents[3]))[0]  
    acc7 = re.findall(r'\d+ cm', unicode(acc.contents[5]))[0]   
    accS = re.findall(r'\d+ cm', unicode(acc.contents[7]))[0]
    
    #pistes
    pistes = tag.find_next_sibling("div", class_="les-pistes clearfix")
    
    liPistes = pistes.find_next("ul").find_next('li')
    ratioPistes = re.findall(r'\d+ / \d+', unicode(liPistes))[0]
    #print ratioPistes
    
    #condition neige
    blocCond = pistes.find_next("div", class_="colonne5 colonneE gauche")
    p = blocCond.find_all("p")
    
    neige = re.findall(r'<br/>[\w*\s\w*]*', unicode(p[0]), re.U)[0][5:]
    base = re.findall(r'<br/>[\w*\s\w*]*', unicode(p[1]), re.U)[0][5:]
    couverture = re.findall(r'<br/>[\w*\s\w*]*', unicode(p[2]), re.U)[0][5:]
    #print neige
    #print base
    #print couverture
    
    
    #first make sure everything is utf8
    
    name        = unicode(name         )#.encode('utf8', 'replace')
    update      = unicode(update       )#.encode('utf8', 'replace')
    acc24       = unicode(acc24        )#.encode('utf8', 'replace')
    acc48       = unicode(acc48        )#.encode('utf8', 'replace')
    acc7        = unicode(acc7         )#.encode('utf8', 'replace')
    accS        = unicode(accS         )#.encode('utf8', 'replace')
    ratioPistes = unicode(ratioPistes  )#.encode('utf8', 'replace')
    neige       = unicode(neige        )#.encode('utf8', 'replace')
    base        = unicode(base         )#.encode('utf8', 'replace')
    couverture  = unicode(couverture   )#.encode('utf8', 'replace')
    
    
    
    #secure for MySQL !!! Either escape OR user execute's attributes but NOT both
    # name      = MySQLdb.escape_string(name        ) 
    # update        = MySQLdb.escape_string(update      )
    # acc24         = MySQLdb.escape_string(acc24       )
    # acc48         = MySQLdb.escape_string(acc48       )
    # acc7      = MySQLdb.escape_string(acc7        ) 
    # accS      = MySQLdb.escape_string(accS        ) 
    # ratioPistes = MySQLdb.escape_string(ratioPistes )
    # neige         = MySQLdb.escape_string(neige       )
    # base      = MySQLdb.escape_string(base        ) 
    # couverture    = MySQLdb.escape_string(couverture  )   
    

    #name       = name         .decode('utf8', 'replace')
    query = "SELECT maneige.id, count(maneige.id), name, meteoID FROM maneige LEFT JOIN meteo_neige ON meteo_neige.maneigeID = maneige.id WHERE name = %s"
    output(query)
    output(name)
    cur.execute(query, [name])
    
    for row in cur.fetchall() :
        maneigeID = 0
        if row[1] == 0:
            id = "NULL"
            query = "INSERT INTO maneige VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NULL)";
            
            cur.execute(query, (id, name, acc24, acc48, acc7, accS, ratioPistes, neige, base, couverture, update))
            maneigeID = cur.lastrowid
            
        else:
            id = row[0]
            query = u"UPDATE maneige SET acc24 = %s, acc48 = %s, acc7 = %s, accSeason = %s, trails = %s, snow = %s, base = %s, cover = %s, modified = %s, updated = NULL WHERE id = %s ;"
            cur.execute(query, [acc24, acc48, acc7, accS, ratioPistes, neige, base, couverture, update, unicode(id)])
            maneigeID = row[0]

        if row[-1] == None or row[1] == 0:
            if args.interactive:
                print 'Attempting to find meteo for ' + name
                (result, uid) = find_meteo(name)
                if result == None:
                    print 'Could not find a results'
                else: 
                    print 'Found "' + result + '" with id: ' + uid
                    
                    action = raw_input('Accept this result? (Y/n) : ')
                    
                    if action.lower() == 'y':
                        query = "INSERT INTO meteo (meteomediaID) VALUES (%s)"
                        cur.execute(query, [uid])
                        meteoID = cur.lastrowid
                        output('New meteoID ' + str(meteoID))
                        
                        query = "INSERT INTO meteo_neige VALUES (NULL, %s, %s)"
                        cur.execute(query, [meteoID, maneigeID])
                        print 'Added to DB'
                
            else:
                print name+' don\'t have associated meteo. Need to run interactive mode'
        

    # cur.execute("SELECT * FROM maneige")
    # # print all the first cell of all the rows
    # for row in cur.fetchall() :
        # print row
    db.commit()


# Meteomedia

query = "SELECT id, meteomediaID FROM meteo"
cur.execute(query)
    
for row in cur.fetchall() : 
    try:
        headers = {'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36'}
        url = 'http://www.meteomedia.com/api/data/' + row[1]
        request = urllib2.Request(url, None, headers)
        response = urllib2.urlopen(request)
        data = response.read()
        response.close()

        jdata = json.loads(data)

        s1 = jdata['sevendays']['periods'][0]['s']
        s2 = jdata['sevendays']['periods'][1]['s']
        s3 = jdata['sevendays']['periods'][2]['s']
        s4 = jdata['sevendays']['periods'][3]['s']
        s5 = jdata['sevendays']['periods'][4]['s']
        s6 = jdata['sevendays']['periods'][5]['s']
        s7 = jdata['sevendays']['periods'][6]['s']
        temperature = jdata['obs']['t']
        weather = jdata['obs']['background']

        output("Meteomedia for " + row[1] +" : snow1="+s1)
        
        query = "UPDATE meteo SET snow1 = %s, snow2 = %s, snow3 = %s, snow4 = %s, snow5 = %s, snow6 = %s, snow7 = %s, temperature = %s, weather = %s, updated = NULL WHERE id = %s"
        cur.execute(query, (s1, s2, s3, s4, s5, s6, s7, temperature, weather, row[0]))

        db.commit()
    except:
        print 'Could not fetch data for ' + 'http://www.meteomedia.com/api/data/' + row[1]

    
    
#once this is done close connection     
db.close()  
