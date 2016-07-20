#!/usr/bin/env python
# encoding: utf-8

import os, sys, math, requests, time
import socket, threading, json, Queue
from threading import Thread

# 1. input a extent like: tid, minX maxX, minY, maxY, zoom
# 2. cal total of mission, add thread to work list, and show proess

# globel vars
mutex = threading.Lock()        # thread lock !
socket.setdefaulttimeout(20)    # outtime set 20s

proxies = {
  #"http": "http://220.202.123.34:55336"
}


##########################################################################
class Worker(Thread):
    # thread pool, must python 2.7 up
    worker_count = 0
    def __init__(self, workQueue, resultQueue, timeout = 0, **kwds):
       Thread.__init__(self, **kwds)
       self.id = Worker.worker_count
       Worker.worker_count += 1
       self.setDaemon(True)
       self.workQueue = workQueue
       self.resultQueue = resultQueue
       self.timeout = timeout
       self.start()
     
    def run(self):
        ''' the get-some-work, do-some-work main loop of worker threads '''
        while True:
            try:
                callable, args, kwds = self.workQueue.get(timeout=self.timeout)
                res = callable(*args, **kwds)
                #print "worker[%2d]: %s" % (self.id, str(res))
                self.resultQueue.put(res)
            except Queue.Empty:
                break
            except :
                print 'worker[%2d]' % self.id, sys.exc_info()[:2]

class WorkerPool:
    # thread pool
    def __init__(self, num_of_workers=10, timeout = 1):
        self.workQueue = Queue.Queue()
        self.resultQueue = Queue.Queue()
        self.workers = []
        self.timeout = timeout
        self._recruitThreads(num_of_workers)
    def _recruitThreads(self, num_of_workers):
        for i in range(num_of_workers): 
            worker = Worker(self.workQueue, self.resultQueue, self.timeout)
            self.workers.append(worker)
    def wait_for_complete(self):
        # ...then, wait for each of them to terminate:
        while len(self.workers):
            worker = self.workers.pop()
            worker.join()
            if worker.isAlive() and not self.workQueue.empty():
                self.workers.append(worker)
        #print "All jobs are are completed."
    def add_job(self, callable, *args, **kwds):
        self.workQueue.put((callable, args, kwds))
    def get_result(self, *args, **kwds):
        return self.resultQueue.get(*args, **kwds)
    

##########################################################################

class Spider:
    # the spider
    def __init__(self, outpath):
        # Initialize
        #/kh/v=693&x=210758&y=112861&z=18&s=Galileo
        #self.TILES_URL = 'http://khm1.google.com/kh/v=692&hl=en&x={0}&y={1}&z={2}&s=Galile'        # URLS
        self.TILES_URL = 'http://mt3.google.cn/vt/lyrs=s&hl=en&x={0}&y={1}&z={2}'   #
        self.outpath = outpath
        self.num = 0

    def GetIMG(self, url, savefile):
        # download picture, in this fun you can dispose outtime and 404 error etc.
        # and you can set proxy and http head and encode etc.
        if (os.path.exists(savefile)): return True
        mutex.acquire()
        path, name = os.path.split(savefile)
        if (os.path.exists(path)==False): os.makedirs(path)
        mutex.release()
        global proxies

        try:
            response = requests.get(url, proxies=proxies, stream=True)
            data = response.raw.read()
            #if (len(data) < 2048): return False
            open(savefile, 'wb').write(data)
            return True
        except:
            try:
                response = requests.get(url, proxies=proxies, stream=True)
                data = response.raw.read()
                #if (len(data) < 2048): return False
                open(savefile, 'wb').write(data)
                return True
            except Exception, ex:
                print ex
                return False
        
    def DownloadTiles(self, x, y, zoom, total):
        # download tiles
        url = self.TILES_URL.format(x, y, zoom)
        try:
            # save file
            savefile = '%s/%d/R%08d/C%08d.JPG' % (self.outpath, zoom, y, x)
            success = self.GetIMG(url, savefile)
            
            if (success == False):
                # faile
                error = 'Get IMG {%s, %s, %s} error' % (x, y, zoom)
                ShowInfo(error, 'e', True)
            else:
                # success
                self.num += 1
                if (self.num % 10 == 0):
                    ShowInfo('Downloaded IMG: %s / %s' % (self.num, total))
                    
        except Exception, ex:
            ShowInfo('xxxxxx' + str(ex))


    # =============================================================
    def Work(self, maxThreads, tiles, zoom):
        # the thread work
        self.num = 0
        wp = WorkerPool(maxThreads)                             # num of thread
        total = len(tiles)
        for tile in tiles:
            x = tile[0]
            y = tile[1]
            wp.add_job(self.DownloadTiles, x, y, zoom, total)   # add work
        wp.wait_for_complete()                                  # wait for complete
        ShowInfo('Total tiles {0}.'.format(len(tiles)))


##########################################################################
LOG_FILE = './tiles.log'        # log file

def ShowInfo(text, level='i', save=False):
    # display info
    # text: infoation
    # level: type of infoation > info, warning, error 
    # save: save to file
    mutex.acquire()
    # print time
    if (level==None or len(level)==0): level='i'
    stime = time.strftime(r'%m/%d %H:%M:%S')
    print stime,
    # print info
    print '[{0}]:'.format(level[0]),
    print text
    # write to file
    if (save == True):
        open(LOG_FILE, 'a').write('{0} [{1}]: {2}\r\n'.format(stime, level[0], text))
    mutex.release()

def GetTask(fname):
    # get task from json file
    #tasks = {
    #    0: {'minx':0, 'maxx':0, 'miny':0, 'maxy':0},
    #    1: {'minx':0, 'maxx':0, 'miny':0, 'maxy':0}
    #        }
    text = open(fname, 'r').read().encode('utf8')
    decodejson = json.loads(text)
    tasks = {}
    for tile in decodejson['tiles']:
        zoom = int(tile['zoom'])
        minx = int(tile['minx'])
        maxx = int(tile['maxx'])
        miny = int(tile['miny'])
        maxy = int(tile['maxy'])
        tasks[zoom] = {'minx':minx, 'maxx':maxx, 'miny':miny, 'maxy':maxy}
    return tasks


if __name__ == '__main__':
    # the main fun
    print 'Tile Maker.'
    print 'Encode: %s' %  sys.getdefaultencoding()

    # init
    maxThreads = 16                         # the num of thread
    outpath = './out/'                      # output path
    jsonfile = 'task.json'                  # task json file
    
    # make output dir
    if (os.path.exists(outpath)==False):
        os.makedirs(outpath)

    # load task
    tasks = GetTask(jsonfile)

    # do work
    success = True
    try:
        for zoom in tasks:
            # each zoom
            minX = tasks[zoom]['minx']      # the left X index
            maxX = tasks[zoom]['maxx']      # the right X index
            minY = tasks[zoom]['miny']      # the buttom Y index
            maxY = tasks[zoom]['maxy']      # the top Y index
            
            # list of tile
            tiles = []
            for y in range(minY, maxY + 1):
                for x in range(minX, maxX + 1):
                    tiles.append([x, y])

            print '============================='
            print '{0} -> [{1}, {2}, {3}, {4}] / zoom: {5} ...'.format(time.strftime(r'%m/%d %H:%M:%S'), minX, maxX, minY, maxY, zoom)
            print '{0} -> total: {1} ...\n'.format(time.strftime(r'%m/%d %H:%M:%S'), (maxX - minX + 1) * (maxY - minY + 1))
            
            # one of zooms
            spider = Spider(outpath)
            spider.Work(maxThreads, tiles, zoom)
            
    except Exception, ex:
        print ex
        success = False
        
    print 'Finish', success
    

    
