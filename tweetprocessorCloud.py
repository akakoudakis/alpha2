from twitter import *
import os
import pymongo
import couchdbkit
'substring in string'


'''Return a collection named pcollectionname from the pDBname mongoDB'''
def connectMongoDB(pDBname,pcollectionname):
    # Connection to Mongo DB
    try:
        conn=pymongo.MongoClient()
        '''print "Connected successfully!!!"'''
    except pymongo.errors.ConnectionFailure, e:
       print "Could not connect to MongoDB: %s" % e 
    
    db=conn[pDBname]
    collection=db[pcollectionname]
    return collection;



def connectCloudant(pDBname):
    server = couchdbkit.Server('https://akakoudakis:Heraklion1@akakoudakis.cloudant.com')
    db = server.get_or_create_db('mytweets')
    return db
    
    
def tweetscan(lookupstr):
    CONSUMER_KEY= 'tc1mdpFs6A7h7IBeV9zXeJaCl'
    CONSUMER_SECRET= '6ifiVsUpZhv5tWfGskF2bEzV8waFiC4fa4Z7rQDxyusPB2lmr8'
    
    MY_TWITTER_CREDS = os.path.expanduser('~/.my_app_credentials')
    
    if not os.path.exists(MY_TWITTER_CREDS):
        oauth_dance("AlphaT", CONSUMER_KEY, CONSUMER_SECRET,
                MY_TWITTER_CREDS)
    
    oauth_token, oauth_secret = read_token_file(MY_TWITTER_CREDS)

    twitter = Twitter(auth=OAuth(
    oauth_token, oauth_secret, CONSUMER_KEY, CONSUMER_SECRET))
    
    x=twitter.statuses.home_timeline(count=200)
    z=[]
    if lookupstr=='*':
        return x
    else:
        count =0
        for l in x:
            str= x[count]['text']
            if str.find('lookupstr', 0)>0:
                z.append(x[count])
        count=count+1







def tweetscanuser(lookupstr,puser):
    CONSUMER_KEY= 'tc1mdpFs6A7h7IBeV9zXeJaCl'
    CONSUMER_SECRET= '6ifiVsUpZhv5tWfGskF2bEzV8waFiC4fa4Z7rQDxyusPB2lmr8'
    
    MY_TWITTER_CREDS = os.path.expanduser('~/.my_app_credentials')
    
    if not os.path.exists(MY_TWITTER_CREDS):
        oauth_dance("AlphaT", CONSUMER_KEY, CONSUMER_SECRET,
                MY_TWITTER_CREDS)
    
    oauth_token, oauth_secret = read_token_file(MY_TWITTER_CREDS)

    twitter = Twitter(auth=OAuth(
    oauth_token, oauth_secret, CONSUMER_KEY, CONSUMER_SECRET))
    
    # Get a particular friend's timeline
    x=twitter.statuses.user_timeline(screen_name=puser, count=200)
    z=[]
    if lookupstr=='*':
        return x
    else:
        count =0
        for l in x:
            str= x[count]['text']
            if str.find('lookupstr', 0)>0:
                z.append(x[count])
        count=count+1




def tweetexist(pCollection,pTid):
    count=0
    for post in pCollection.find({"id":pTid}):
        count=count+1
    if count>0:
        return True
    else:
        return False    




def inserttweets(pcollection,psearch,puser):
    mytweetlist=tweetscanuser(psearch,puser)
    count=0
    for x in mytweetlist:
        document=mytweetlist[count]
        
        if not tweetexist(pcollection,document['id']):
            pcollection.insert(document)
            print document['text']        
        count=count+1
            


def inserttweetscloudant(pcollection,psearch,puser):
    mytweetlist=tweetscanuser(psearch,puser)
    count=0
    for x in mytweetlist:
        document=mytweetlist[count]
        pcollection.save_doc(document)
        print document['text']        
        count=count+1


'''Refreshes local DB for tweets generates by sources specified in plist'''
def refreshlocalDB(plist):
    cur=0
    for x in plist:
        mycollection= connectMongoDB('mydb2',x)
        print 'processing:   '+x
        inserttweets(mycollection,'*',x)
    cur=cur+1



'''Look for a term in the tweets in the psourceCollection and if that term is found then post into pdestinCollection'''
def lookandinsertterm(psourceCollection,pdestinCollection,pTerm):
    for post in psourceCollection.find():
        str=post['text']
        if str.find(pTerm, 0)>0:
            if not tweetexist(pdestinCollection,post['id']):
                pdestinCollection.insert(post)
    


'''refresh results tables'''
def refreshresults(psourceslist,ptermslist):
    for term in ptermslist:
        '''print'LOOKING INTO TERM:   '+ term'''
        vdestinCollection= connectMongoDB('mydb2',term)       
        for source in psourceslist:
            vsourceCollection= connectMongoDB('mydb2',source)
            '''print'Opening the colection named:   '+ source'''
            lookandinsertterm(vsourceCollection,vdestinCollection,term)




def showresults(ptermslist):
    for term in ptermslist:
        vsourceCollection= connectMongoDB('mydb2',term)
        for post in vsourceCollection.find():
            print term +'--->'+post['text']
    

def populatecloudant(plist):
    cur=0
    for x in plist:
        mycollection= connectCloudant(plist)
        print 'processing:   '+x
        inserttweetscloudant(mycollection,'*',x)
    cur=cur+1
    
    
'''MAIN Programme'''

'''Specify the from where tweets will be scanned'''
mytlist=['Quant_FORUM','traders_tweets','eFinancialNews','Finextra','FOWgroup','TradeTech','ISDA','ltabb','MarketAxess','TABBGroup','SellSideTech','Tradeweb']

'''Specify the terms that we need to focus at to filter the results'''
myterms=['Derivatives','OTC','Fintech','SEF','Regulation','ISDA','liquidity','Clearing','Settlements','Collateral','Mifid']


populatecloudant(mytlist)
'''refreshlocalDB(mytlist)'''
'''refreshresults(mytlist,myterms)'''
'''showresults(myterms)'''
