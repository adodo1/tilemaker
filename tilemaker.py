#!/usr/bin/env python
# encoding: utf-8

import os, sys, math, requests, time
import socket, threading, json, Queue
from threading import Thread

# 1. 输入一个范围参数 tid, minX maxX, minY, maxY, zoom
# 2. 计算出总任务数 加入线程队列中 能够显示线程进度
# 3. 信息输出到数据库中, 失败信息也记录下来, 任务数不能超过10W
#    如果程序结束后没有添加任何记录, 该数据库不会被保存
# 4. 第一步获取 UID 列表


# 定义全局变量
mutex = threading.Lock()        # 线程锁
socket.setdefaulttimeout(20)    # 超时时间15秒

proxies = {
  #"http": "http://220.202.123.34:55336"
}


##########################################################################
class Worker(Thread):
    # 线程池工作线程 只支持 python 2.7 或以上版本
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
    # 线程池
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
    # 爬虫
    def __init__(self, outpath):
        # 初始化
        #/kh/v=693&x=210758&y=112861&z=18&s=Galileo
        #self.TILES_URL = 'http://khm1.google.com/kh/v=692&hl=en&x={0}&y={1}&z={2}&s=Galile'        # 获取POI列表
        self.TILES_URL = 'http://mt3.google.cn/vt/lyrs=s&hl=en&x={0}&y={1}&z={2}'   #
        self.outpath = outpath
        self.num = 0

    def GetIMG(self, url, savefile):
        # 下载图片 在这里可以处理链接超时 404 等错误
        # 同时这里可以设置代理或构造数据头 页面编码等
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
        
    def DownloadTiles(self, x, y, zoom):
        # 下载POI列表UID
        url = self.TILES_URL.format(x, y, zoom)
        try:
            # 读取数据
            savefile = '%s/%d/R%08d/C%08d.JPG' % (self.outpath, zoom, y, x)
            success = self.GetIMG(url, savefile)
            
            if (success == False):
                # 失败
                error = 'Get IMG {%s, %s, %s} error' % (x, y, zoom)
                ShowInfo(error, 'e', True)
            else:
                # 成功
                self.num += 1
                if (self.num % 10 == 0):
                    ShowInfo('Downloaded IMG: %s' % (self.num))
                    
        except Exception, ex:
            ShowInfo('xxxxxx' + str(ex))


    # 多线程控制 =============================================================
    def Work(self, maxThreads, tiles, zoom):
        # 下载瓦片
        self.num = 0
        wp = WorkerPool(maxThreads)                     # 线程数量
        for tile in tiles:
            x = tile[0]
            y = tile[1]
            wp.add_job(self.DownloadTiles, x, y, zoom)  # 添加工作
        wp.wait_for_complete()                          # 等待完成
        ShowInfo('Total tiles {0}.'.format(len(tiles)))


##########################################################################
LOG_FILE = './tiles.log'        # 线程日志

def ShowInfo(text, level='i', save=False):
    # 输出信息
    # text 信息内容
    # level 信息类别 info, warning, error 
    # save 是否保存到日志里
    mutex.acquire()
    # 打印时间
    if (level==None or len(level)==0): level='i'
    stime = time.strftime(r'%m/%d %H:%M:%S')
    print stime,
    # 输出信息
    print '[{0}]:'.format(level[0]),
    print text
    # 写入日志
    if (save == True):
        open(LOG_FILE, 'a').write('{0} [{1}]: {2}\r\n'.format(stime, level[0], text))
    mutex.release()



if __name__ == '__main__':
    # 入口函数
    minX = 210660               # 最左边X号索引 单位是百度的瓦片索引坐标
    maxX = 210905               # 最右边X号索引 单位是百度的瓦片索引坐标
    minY = 112707               # 最下面Y号索引 单位是百度的瓦片索引坐标
    maxY = 112879               # 最上面Y号索引 单位是百度的瓦片索引坐标
    zoom  = 18                  # 缩放级别

    minX = 210752
    maxX = 210783
    minY = 112848
    maxY = 112879
    zoom = 18
    '''
0.59716428348
0
0
-0.59716428348
12181004.82752570000
2785976.80693810000
    '''


    #tid = sys.argv[1]
    #minX = int(sys.argv[2])
    #maxX = int(sys.argv[3])
    #minY = int(sys.argv[4])
    #maxY = int(sys.argv[5])
    #zoom  = 19
    
    maxThreads = 16                         # 线程的数量
    outpath = 'e:/output/'                  # 输出路径
    
    print '{0} -> [{1}, {2}, {3}, {4}] / zoom: {5} ...'.format(time.strftime(r'%m/%d %H:%M:%S'), minX, maxX, minY, maxY, zoom)
    print '{0} -> total: {1} ...\n'.format(time.strftime(r'%m/%d %H:%M:%S'), (maxX - minX + 1) * (maxY - minY + 1))

    #url = 'http://khm1.google.com/kh/v=193&hl=en&x=6583&y=3525&z=13&s=Gal'
    #data = urllib.urlopen(url, proxies={'http':'http://220.202.123.42:55336'}).read()
    #data = requests.get(url, proxies=proxies, stream=True)
    #open('a.jpg', 'wb').write(data.raw.read())

    
    success = True
    # 初始化
    if (os.path.exists(outpath)==False):
        os.makedirs(outpath)

    try:
        tiles = []
        for y in range(minY, maxY + 1):
            for x in range(minX, maxX + 1):
                tiles.append([x, y])
                
        spider = Spider(outpath)
        spider.Work(maxThreads, tiles, zoom)
        
        #for tile in tiles:
        #    x = tile[0]
        #    y = tile[1]
        #    spider.DownloadTiles(x, y, zoom)
        #spider.GetIMG('http://khm1.google.com/kh/v=184&hl=en&x=6583&y=3525&z=13&s=Gal', './abc.jpg', 'http://220.202.123.42:55336')
        
    except Exception, ex:
        print ex
        success = False
        
    print 'Finish', success
    

    
