

import pprint,sys
import blaze
from odo import odo
import pandas as pd
import numpy as np
from BTrees.OOBTree import OOBTree
from tables import *


class MDUpdateAction(object):
    NEW = '0'
    CHANGE = '1'
    DELETE = '2'
    OVERLAY = '5'

class MDEntryType(object):
    BID = '0'
    OFFER = '1'
    TRADE = '2'


class Message(object):
    APPLVERID               = '1128'
    BODYLENGTH              = '9'
    LASTMSGSEQNUMPROCESSED  = '369'
    MDSECURITYTRADINGSTATUS = '1682'
    MSGSEQNUM               = '34'
    MSGTYPE                 = '35'
    POSSDUPFLAG             = '43'
    SENDERCOMPID            = '49'
    SENDINGTIME             = '52'
    AGGRESSORSIDE       = '5797'
    FIXINGBRACKET       = '5790'
    MATCHEVENTINDICATOR = '5799'
    MDENTRYPX           = '270'
    MDENTRYSIZE         = '271'
    MDENTRYTIME         = '273'
    MDENTRYTYPE         = '269'
    MDPRICELEVEL        = '1023'
    MDQUOTETYPE         = '1070'
    MDUPDATEACTION      = '279'
    NETCHGPREVDAY       = '451'
    NOMDENTRIES         = '268'
    NUMBEROFORDERS      = '346'
    OPENCLOSESETTLEFLAG = '286'
    QUOTECONDITION      = '276'
    RPTSEQ              = '83'
    SECURITYDESC        = '107'
    SECURITYID          = '48'
    SECURITYIDSOURCE    = '22'
    SETTLDATE           = '64'
    SYMBOL              = '55'
    TICKDIRECTION       = '274'
    TRADECONDITION      = '277'
    TRADEDATE           = '75'
    TRADEVOLUME         = '1020'
    TRADINGSESSIONID    = '336'
    DELIMITER           = '\x01'

    def __init__ (self,msg):
        self.pp = pprint.PrettyPrinter(depth=4)
        self.header = {}
        self.repeatinggroups = []
        self.message = msg


    def isIncremental(self):
        s = self.message.split(Message.DELIMITER)
        s.pop(-1)
        p = dict( z.split('=') for z in s )
        return p.get('35') == 'X'

    def parse(self):
        header,repeatgroups = self.message.split('268=')
        self.__parseHeader(header)
        self.__parseRepeatingGroups(repeatgroups)
        #print header

    def __parseHeader (self,header):
        w=header.split(Message.DELIMITER)
        w.pop(-1)
        for elem in w:
            k,v=  elem.split('=')
            self.header[k] = v
        #self.pp.pprint(self.header)

    def __parseRepeatingGroups (self,rg):
        groups = rg.split('279=')
        groups.pop(0)
        #print len(groups)
        for g in groups:
            g= '279=' + g
            w=g.split(Message.DELIMITER)
            w.pop(-1)
            mydict={}
            for elem in w:
                k,v=  elem.split('=')
                mydict[k] = v
            self.repeatinggroups.append(mydict)
        #self.pp.pprint(self.repeatinggroups)

    def getSENDINGTIME(self):
        return self.header[Message.SENDINGTIME]

    def getMDENTRIES (self):
        return self.repeatinggroups

    def getNOMDENTRIES (self):
        return len(self.repeatinggroups)

    def getSorted (self):
        return sorted(self.repeatinggroups,key=lambda x: x[Message.MDENTRYPX])

    def getNew (self):
        """
        return indexes of new orders
        """
        idx=[]
        for i,ha in enumerate(self.repeatinggroups):
            #print ha
            if ha[Message.MDUPDATEACTION] == MDUpdateAction.NEW:
                idx.append(i)
        return idx

    def getChange (self):
        """
        return indexes of new orders
        """
        idx=[]
        for i,ha in enumerate(self.repeatinggroups):
            #print ha
            if ha[Message.MDUPDATEACTION] == MDUpdateAction.CHANGE:
                idx.append(i)
        return idx

    def getDelete (self):
        """
        return indexes of deleted orders
        """
        idx=[]
        for i,ha in enumerate(self.repeatinggroups):
            #print ha
            if ha[Message.MDUPDATEACTION] == MDUpdateAction.DELETE:
                idx.append(i)
        return idx




class Level(object):
    def __init__(self,kwargs):
        self.level = int(kwargs[Message.MDPRICELEVEL])
        self.price = kwargs[Message.MDENTRYPX]
        self.size  = kwargs[Message.MDENTRYSIZE]
        self.side  = kwargs[Message.MDENTRYTYPE]

    def __cmp__(self, other):
        if other.__class__ is Level:
            if self.side == MDEntryType.BID:
                return cmp(self.level, other.level)
            else:
                return cmp(other.level,self.level)
        else:
            return cmp(self.level, other.level)




class HDFreader(object):
    def __init__ (self,filename, depth):
        self.filename = filename
        self.depth = depth
        self.h5file = openFile(self.filename)

    def reader(self):
        self.rbook = self.h5file.root.orderbook.book

    def shutdown (self):
        try:
            self.h5file.close()
        except:
            pass

    def __del__ (self):
        self.shutdown()



class FileUtils(object):
    def __init__ (self,filename, depth):
        self.filename = filename
        self.depth = depth
        self.chunklen = 2000
        self.df = self.createDataFrame()
        #write header
        self.df.to_csv(self.filename,index=False)
        self.rowcounter = 0

    def createDataFrame (self):
        self.cols =['ticker'
                ,'timestamp'
                ,'bid1'
                ,'bidsize1'
                ,'ask1'
                ,'asksize1'
                ,'bid2'
                ,'bidsize2'
                ,'ask2'
                ,'asksize2'
                ,'bid3'
                ,'bidsize3'
                ,'ask3'
                ,'asksize3' ]
        return blaze.DataFrame(columns=self.cols)

    def add (self,bidbook,askbook,ticker,timestamp):
        bids = np.zeros(self.depth)
        asks = np.zeros(self.depth)
        bidsize = np.zeros(self.depth)
        asksize = np.zeros(self.depth)

        buys  = [ [bidbook[b].price, bidbook[b].size] for b in sorted(bidbook.keys()) ]
        sells = [ [askbook[a].price, askbook[a].size] for a in sorted(askbook.keys()) ]
        buys  = np.array(buys)
        sells = np.array(sells)

        bids[0:len(buys)] = buys[:,0][:self.depth]
        asks[0:len(sells)] = sells[:,0][:self.depth]
        bidsize[0:len(buys)] = buys[:,1][:self.depth]
        asksize[0:len(sells)] = sells[:,1][:self.depth]

        data=[ticker
            ,timestamp
            ,bids[0]
            ,asks[0]
            ,bidsize[0]
            ,asksize[0]
            ,bids[1]
            ,asks[1]
            ,bidsize[1]
            ,asksize[1]
            ,bids[2]
            ,asks[2]
            ,bidsize[2]
            ,asksize[2] ]
        ds = pd.Series(data,index=self.cols)
        self.df = self.df.append(ds,ignore_index=True)
        #flush to csv every chunklen (ie 2000 records)
        if ++self.rowcounter%self.chunklen == 0:
            self.tocsv()
            self.df = self.createDataFrame()

    def tocsv(self):
        self.df.to_csv(self.filename,index=False,mode = 'a',header=False)

    def tosqlite (self):
        odo(self.df, 'sqlite:///cme.db::ticktable')



class HDFutils(object):
    def __init__ (self,filename, depth):
        self.filename = filename
        self.depth = depth

    def create (self):
        self.h5file = openFile(self.filename, mode = "w", title = "Test file")
        class OBook(IsDescription):
            ticker    = StringCol(16)                   # 16-character String
            timestamp = Int64Col()                      # Signed 64-bit integer
            bids      = FloatCol(shape=(self.depth,))   # float
            bidsize   = Int32Col(shape=(self.depth,))   # integer
            asks      = FloatCol(shape=(self.depth,))   # float
            asksize   = Int32Col(shape=(self.depth,))   # integer
            bid1      = FloatCol()   # float
            bidsize1  = Int32Col()   # integer
            ask1      = FloatCol()   # float
            asksize1  = Int32Col()   # integer
            bid2      = FloatCol()   # float
            bidsize2  = Int32Col()   # integer
            ask2      = FloatCol()   # float
            asksize2  = Int32Col()   # integer
            bid3      = FloatCol()   # float
            bidsize3  = Int32Col()   # integer
            ask3      = FloatCol()   # float
            asksize3  = Int32Col()   # integer
        group = self.h5file.createGroup("/", 'orderbook', 'OrderBookgroup')
        self.table = self.h5file.createTable(group, 'book', OBook, "OrderBook")
        self.book = self.table.row

    def add (self,bidbook,askbook,ticker,timestamp):
        bids = np.zeros(self.depth)
        asks = np.zeros(self.depth)
        bidsize = np.zeros(self.depth)
        asksize = np.zeros(self.depth)

        buys  = [ [bidbook[b].price, bidbook[b].size] for b in sorted(bidbook.keys()) ]
        sells = [ [askbook[a].price, askbook[a].size] for a in sorted(askbook.keys()) ]
        buys  = np.array(buys)
        sells = np.array(sells)

        bids[0:len(buys)] = buys[:,0][:self.depth]
        asks[0:len(sells)] = sells[:,0][:self.depth]
        bidsize[0:len(buys)] = buys[:,1][:self.depth]
        asksize[0:len(sells)] = sells[:,1][:self.depth]

        self.book['ticker'] = ticker
        self.book['timestamp'] = timestamp
        self.book['bids'] = bids #.tolist()
        self.book['asks'] = asks #.tolist()
        self.book['bidsize'] = bidsize #.tolist()
        self.book['asksize'] = asksize #.tolist()
        #
        self.book['bid1'] = bids[0]
        self.book['ask1'] = asks[0]
        self.book['bidsize1'] = bidsize[0]
        self.book['asksize1'] = asksize[0]
        self.book['bid2'] = bids[1]
        self.book['ask2'] = asks[1]
        self.book['bidsize2'] = bidsize[1]
        self.book['asksize2'] = asksize[1]
        self.book['bid3'] = bids[2]
        self.book['ask3'] = asks[2]
        self.book['bidsize3'] = bidsize[2]
        self.book['asksize3'] = asksize[2]

        self.book.append()

    def shutdown (self):
        try:
            self.table.flush()
        except:
            pass
        self.h5file.close()

    def __del__ (self):
        self.shutdown()





class Parser(object):
    def __init__ (self,file,**kwargs):
        self.file = file
        self.fh = open(self.file, "r")
        self.symbol = kwargs['symbol']
        self.depth = kwargs['depth']
        self.bidbook = OOBTree()
        self.askbook = OOBTree()
        self.pp = pprint.PrettyPrinter(depth=4)
        self.fileutils = FileUtils('c:/tmp/cme.csv',5)
        #self.hdfutils = HDFutils('c:/tmp/cme.h5',5)
        #self.hdfutils.create()

    def read(self):
        c=0
        for line in self.fh:
            msg = Message(line)
            if not msg.isIncremental(): continue
            msg.parse()
            mdentries= msg.getMDENTRIES()
            for i in msg.getChange():
                if mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.BID:
                    self.bidbook[ mdentries[i][Message.MDPRICELEVEL] ] = Level(mdentries[i])
                    #self.pp.pprint( mdentries[i] )
                elif mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.OFFER:
                    self.askbook[ mdentries[i][Message.MDPRICELEVEL] ]= Level(mdentries[i])
            for i in msg.getDelete():
                if mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.BID:
                    try:
                        del self.bidbook[ mdentries[i][Message.MDPRICELEVEL] ]
                    except:
                        pass
                elif mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.OFFER:
                    try:
                        del self.askbook[ mdentries[i][Message.MDPRICELEVEL] ]
                    except:
                        pass
            for i in msg.getNew():
                if mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.BID:
                    try:
                        self.bidbook[ mdentries[i][Message.MDPRICELEVEL] ] = Level(mdentries[i])
                    except:
                        print "Unexpected error:", sys.exc_info()[0]
                elif mdentries[i][Message.SECURITYDESC] == self.symbol and mdentries[i][Message.MDENTRYTYPE] == MDEntryType.OFFER:
                    try:
                        self.askbook[ mdentries[i][Message.MDPRICELEVEL] ]= Level(mdentries[i])
                    except:
                        print "Unexpected error:", sys.exc_info()[0]
            c+=1
            self.display()
            try:
                #self.hdfutils.add(self.bidbook,self.askbook,self.symbol,msg.getSENDINGTIME() )
                self.fileutils.add(self.bidbook,self.askbook,self.symbol,msg.getSENDINGTIME() )
            except:
                pass
            #if c>2000:
            #    self.fileutils.tocsv()
            #    self.fileutils.tosqlite()
            #    break

    def display(self):
        try:
            del self.askbook[ '10' ]
        except:
            pass
        try:
            del self.bidbook[ '10' ]
        except:
            pass
        print '================================================================\\'
        for a in sorted(self.askbook.keys(), reverse=True):
            print self.askbook[a].price , self.askbook[a].size
        print "@@"
        for b in sorted(self.bidbook.keys()):
            print self.bidbook[b].price , self.bidbook[b].size
        print '================================================================/'
        print



    def __del__ (self):
        self.fh.close()




if __name__ =='__main__':
    f='C:/tmp/XCBT_MD_ZB_20110725_20110729/XCBT_MD_ZB_20110725_20110729'
    sym='ZBU1'
    #f="C:/tmp/XCME_MD_ES_FUT_20160315/XCME_MD_ES_FUT_20160315"
    #sym='ESH6'
    p = Parser(f,symbol=sym,depth='10')
    p.read()
    p.display()
