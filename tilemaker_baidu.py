#!/usr/bin/env python
# encoding: utf-8

import os, sys, math, requests, time
import socket, threading, json, Queue
from threading import Thread

# 1. input a extent like: tid, minX maxX, minY, maxY, zoom
# 2. cal total of mission, add thread to work list, and show proess
# 3. the sp use web mercator x: [ -20037508.3427892, 20037508.3427892 ]

# globel vars
mutex = threading.Lock()        # thread lock !
socket.setdefaulttimeout(20)    # outtime set 20s
PI = 3.14159265358979323846     # PI
DOMAIN_LEN = 20037508.3427892   # web mercator

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
        #http://t6.tianditu.cn/DataServer?T=vec_w&X=26345&Y=14098&L=15
        #self.TILES_URL = 'http://t6.tianditu.cn/DataServer?T=vec_w&X={0}&Y={1}&L={2}'              # tianditu
        #self.TILES_URL = 'http://khm1.google.com/kh/v=692&hl=en&x={0}&y={1}&z={2}&s=Galile'        # URLS
        #http://mt[0123].google.cn/vt/lyrs=h&hl=zh-CN&gl=cn&&x={x}&y={y}&z={z}
        #http://mt0.google.cn/vt/lyrs=h&hl=zh-CN&gl=cn&x=26345&y=1409&z=15                          # google labels
        #http://mt1.google.cn/vt/lyrs=t@130,r@367000000&hl=zh-cn&gl=cn&src=app&x=26302&y=14040&z=15&s=Gali  # google dem
        self.TILES_URL = 'http://online1.map.bdimg.com/tile/?qt=tile&x={0}&y={1}&z={2}&styles=pl'   #
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
            if (data[0:2]=='<!'): return False
            open(savefile, 'wb').write(data)
            return True
        except:
            try:
                response = requests.get(url, proxies=proxies, stream=True)
                data = response.raw.read()
                #if (len(data) < 2048): return False
                if (data[0:2]=='<!'): return False
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
            bdMercator = BaiduMercator()
            tileX, tileY = bdMercator.BaiduTileToStandard(x, y, zoom)       # baidu tile xy to standard tile xy
            
            savefile = '%s/L%02d/R%08x/C%08x.PNG' % (self.outpath, zoom, tileY, tileX)
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

class GMap:
    # GMap class
    def __init__(self):
        self.MinLatitude = -85.05112878     # min latitude
        self.MaxLatitude = 85.05112878      # max latitude
        self.MinLongitude = -180            # min longitude
        self.MaxLongitude = 180             # max longitude
        self.TileSizeWidth = 256            # tile width
        self.TileSizeHeight = 256           # tile height
        self.Dpi = 96.0                     # tile dpi

    def GetTileMatrixMinXY(self, zoom):
        # tile min xy
        return 0, 0

    def GetTileMatrixMaxXY(self, zoom):
        # tile max xy
        xy = (1 << zoom)
        return xy - 1, xy - 1

    def GetTileMatrixSizePixel(self, zoom):
        # tile full pixel size
        sMin = self.GetTileMatrixMinXY(zoom)
        sMax = self.GetTileMatrixMaxXY(zoom)
        width = (sMax[0] - sMin[0] + 1) * self.TileSizeWidth
        height = (sMax[1] - sMin[1] + 1) * self.TileSizeHeight
        return width, height

    def GetMAPScale(self, zoom, lat=0):
        # http://wenku.baidu.com/link?url=I-RdILcOskWLkqYvLetcFFr7JiURwY4WxfOlKEe8gwkJp_WS6O9H7KNOz0YTBu5Fo8Ff0WcurgeYVPvRY2c2k10805MV-Taj4JXRK4aVqje
        # http://www.360doc.com/content/15/0319/13/9009195_456410364.shtml
        # http://wenku.baidu.com/view/359c88d6b14e852458fb5754.html
        # http://www.cnblogs.com/beniao/archive/2010/04/18/1714544.html
        # http://gis.stackexchange.com/questions/7430/what-ratio-scales-do-google-maps-zoom-levels-correspond-to < useful
        #
        #level     dis       px    map_dis   dpi      scale      ground_resolution
        #level2    5000km    70    2.47cm    72dpi    2b : 1     71km    
        #level3    2000km    55    1.94cm    72dpi    1b : 1     36km    36363.63636363636
        #level4    2000km    115   4.06cm    72dpi    5kw : 1    17km    17391.30434782609
        #level5    1000km    115   4.06cm    72dpi    2.5kw : 1  9km     8695.652173913043
        #level6    500km     115   4.06cm    72dpi    1.2kw : 1  4km     4347.826086956522
        #level7    200km     91    3.21cm    72dpi    6hw : 1    2km     2197.802197802198
        #level8    100km     176   6.21cm    72dpi    160w : 1   568m    568.1818181818182
        #level9    50km      91    3.21cm    72dpi    155w : 1   549m    549.4505494505495
        #level10   20km      72    2.54cm    72dpi    80w : 1    278m    277.7777777777778
        #level11   10km      72    2.54cm    72dpi    40w : 1    139m    138.8888888888889
        #level12   5km       72    2.54cm    72dpi    20w : 1    69m     69.44444444444444
        #level13   2km       57    2.01cm    72dpi    10w : 1    35m     35.0877192982456
        #level14   2km       118   4.16cm    72dpi    5w : 1     17m     16.9491525423729
        #level15   1km       118   4.16cm    72dpi    2.5w : 1   8m      8.4745762711864
        #level16   500m      118   4.16cm    72dpi    1.2w : 1   4m      4.23728813559322
        #level17   200m      93    3.28cm    72dpi    2300 : 1   2.15m   2.150537634408602
        #level18   100m      93    3.28cm    72dpi    3000 : 1   1.07m   1.075268817204301
        #level19   50m       93    3.28cm    72dpi    1500 : 1   0.54m   0.5376344086021505
        #level20   20m       74    2.61cm    72dpi    800 : 1    0.27m   0.2702702702702703


        # baidu resolution
        # http://blog.csdn.net/dickwang1229/article/details/43796259
        # 
        #zoom    Tiles    Pixels    PixelSize(Deg)          PixelSize(Mtr)           PPI                  Scale
        #00
        #01      2        306       1.17743980920114E+00    131072.00000000000000    96.00000000000000    495390236.22047200000000
        #02      4        612       5.88719904600570E-01    65536.00000000000000     96.00000000000000    247695118.11023600000000
        #03      6        1223      2.94359952300285E-01    32768.00000000000000     96.00000000000000    123847559.05511800000000
        #04      10       2446      1.47179976150142E-01    16384.00000000000000     96.00000000000000    61923779.52755910000000
        #05      20       4892      7.35899880750712E-02    8192.00000000000000      96.00000000000000    30961889.76377950000000
        #06      40       9784      3.67949940375356E-02    4096.00000000000000      96.00000000000000    15480944.88188980000000
        #07      78       19568     1.83974970187678E-02    2048.00000000000000      96.00000000000000    7740472.44094488000000
        #08      154      39136     9.19874850938390E-03    1024.00000000000000      96.00000000000000    3870236.22047244000000
        #09      306      78272     4.59937425469195E-03    512.00000000000000       96.00000000000000    1935118.11023622000000
        #10      612      156544    2.29968712734597E-03    256.00000000000000       96.00000000000000    967559.05511811000000
        #11      1224     313087    1.14984356367299E-03    128.00000000000000       96.00000000000000    483779.52755905500000
        #12      2446     626173    5.74921781836494E-04    64.00000000000000        96.00000000000000    241889.76377952800000
        #13      4892     1252345   2.87460890918247E-04    32.00000000000000        96.00000000000000    120944.88188976400000
        #14      9784     2504689   1.43730445459123E-04    16.00000000000000        96.00000000000000    60472.44094488190000
        #15      19568    5009378   7.18652227295617E-05    8.00000000000000         96.00000000000000    30236.22047244090000
        #16      39136    10018755  3.59326113647809E-05    4.00000000000000         96.00000000000000    15118.11023622050000
        #17      78272    20037509  1.79663056823904E-05    2.00000000000000         96.00000000000000    7559.05511811024000
        #18      156544   40075017  8.98315284119521E-06    1.00000000000000         96.00000000000000    3779.52755905512000
        #19      313088   80150034  4.49157642059761E-06    0.50000000000000         96.00000000000000    1889.76377952756000


        # ground_resolution = (math.cos(lat * math.pi/180) * 2 * math.pi * 6378137) / (256 * 2^level)
        # map_scale = (math.cos(lat * math.pi/180) * 2 * math.pi * 6378137 * dpi) / (256 * 2^level * 0.0254)
        # ---------------------------------------------------
        # fun 1
        #tile_full_px = self.GetTileMatrixSizePixel(zoom)[0]
        #map_dis = tile_full_px * 0.0254 / self.Dpi          # the dis on map > 1 inc = 0.0254 m
        #ground_dis = DOMAIN_LEN * 2                         # the dis on ground
        #scale = ground_dis / map_dis
        # ---------------------------------------------------
        # fun 2
        #scale = (math.cos(lat * math.pi/180) * (DOMAIN_LEN * 2) * self.Dpi) / (256 * (2 ** zoom) * 0.0254)
        # ---------------------------------------------------
        # fun3
        #scale = 591657550.500000 / (2^(zoom-1))
        scale = 495390232.333727 / (2 ** (zoom - 1))    # use DOMAIN_LEN = 20037508.3427892
        return scale
    def GetGroundResolution(self, zoom, lat=0):
        # get resolution, how much m in one pix
        #ground_resolution = (math.cos(lat * math.pi/180) * 2 * math.pi * 6378137) / (256 * (2 ** zoom))
        ground_resolution = (math.cos(0 * math.pi/180) * 2 * math.pi * 6378137) / (80150034 / 2 ** (19 - zoom))
        return ground_resolution

    def FromCoordinateToPixel(self, lat, lng, zoom):
        # gps coordinate to pixel xy  [ gps > pixel xy ]
        # lat: latitude
        # lng: longitude
        # zoom: 0 ~ 19

        # core !!
        # x=(y + 180) / 360
        # y = 0.5 - log((1 + sin(x * 3.1415926 / 180)) / (1 - sin(x * 3.1415926 / 180))) / (4 * pi)
        # y = (1 - (log(tan(x * 3.1415926 / 180) + sec(x * 3.1415926 / 180)) / pi)) / 2
        lat = min(max(lat, self.MinLatitude), self.MaxLatitude)
        lng = min(max(lng, self.MinLongitude), self.MaxLongitude)

        x = (lng + 180) / 360
        y = 0.5 - math.log((1 + math.sin(lat * math.pi / 180)) / (1 - math.sin(lat * math.pi / 180))) / (4 * math.pi)

        mapSizeX, mapSizeY = self.GetTileMatrixSizePixel(zoom)
        pixelX = min(max(x * mapSizeX + 0.5, 0), mapSizeX - 1)
        pixelY = min(max(y * mapSizeY + 0.5, 0), mapSizeY - 1)
        
        return int(pixelX), int(pixelY)

    def FromCoordinateToTileXY(self, lat, lng, zoom):
        # gps coordinate to tile xy  [ gps > tile xy ]
        # lat: latitude
        # lng: longitude
        # zoom: 0 ~ 19
        pixelX, pixelY = self.FromCoordinateToPixel(lat, lng, zoom)
        tileX, tileY = self.FromPixelToTileXY(pixelX, pixelY)
        return tileX, tileY

    def FromPixelToTileXY(self, pixelX, pixelY):
        # full pixel xy to tile xy index
        tileX = int(pixelX / self.TileSizeWidth)
        tileY = int(pixelY / self.TileSizeHeight)
        return tileX, tileY

    def FromPixelToCoordinate(self, x, y, zoom):
        # from pixel xy in tile to gps lat lng
        tile_full_width, tile_full_height = self.GetTileMatrixMaxXY(zoom)
        mapsizex = (tile_full_width + 1) * self.TileSizeWidth
        mapsizey = (tile_full_height + 1) * self.TileSizeHeight

        xx = min(max(x, 0), mapsizex - 1) * 1.0 / mapsizex - 0.5
        yy = 0.5 - (min(max(y, 0), mapsizey - 1) * 1.0 / mapsizey)

        lat = 90 - 360.0 * math.atan(math.exp(-yy * 2 * math.pi)) / math.pi
        lng = 360 * xx
        return lat, lng

    def GetBDTiles(self, top_lat, left_lng, bottom_lat, right_lng, zoom, buff = 0):
        # 获取百度瓦片列表

        # 先计算瓦片范围
        bdMercator = BaiduMercator()
        bd_left, bd_top = bdMercator.LngLatToTile(left_lng, top_lat, zoom)
        bd_right, bd_bottom = bdMercator.LngLatToTile(right_lng, bottom_lat, zoom)

        # region -> tile extent
        #left, top = self.FromCoordinateToTileXY(top_lat, left_lng, zoom)            # tile
        #right, bottom = self.FromCoordinateToTileXY(bottom_lat, right_lng, zoom)    # tile
        tmin_x, tmin_y = self.GetTileMatrixMinXY(zoom)                              # tile matrix size min
        tmax_x, tmax_y = self.GetTileMatrixMaxXY(zoom)                              # tile matrix size max

        # 百度瓦片转标准瓦片
        tile_left_x, tile_top_y = bdMercator.BaiduTileToStandard(bd_left, bd_top, zoom)
        tile_right_x, tile_bottom_y = bdMercator.BaiduTileToStandard(bd_right, bd_bottom, zoom)

        # buffer
        tile_left_x = tile_left_x - buff
        tile_top_y = tile_top_y - buff
        tile_right_x = tile_right_x + buff
        tile_bottom_y = tile_bottom_y +buff
        
        tile_min_x = min(max(tile_left_x, tmin_x), tmax_x)
        tile_max_x = min(max(tile_right_x, tmin_x), tmax_x)
        tile_min_y = min(max(tile_top_y, tmin_y), tmax_y)
        tile_max_y = min(max(tile_bottom_y, tmin_y), tmax_y)

        # 标准瓦片反转百度瓦片
        bd_left, bd_top = bdMercator.StandardTileToBaidu(tile_min_x, tile_min_y, zoom)
        bd_right, bd_bottom = bdMercator.StandardTileToBaidu(tile_max_x, tile_max_y, zoom)
        
        # tile xy -> full pixel xy
        pixel_lt_x = tile_min_x * self.TileSizeWidth
        pixel_lt_y = tile_min_y * self.TileSizeHeight
        pixel_rb_x = (tile_max_x + 1) * self.TileSizeWidth
        pixel_rb_y = (tile_max_y + 1) * self.TileSizeHeight

        # full pixel xy -> new gps extent
        gps_lt_lat, gps_lt_lng = self.FromPixelToCoordinate(pixel_lt_x, pixel_lt_y, zoom)
        gps_rb_lat, gps_rb_lng = self.FromPixelToCoordinate(pixel_rb_x, pixel_rb_y, zoom)

        # full pixel xy -> mercator coordinate xy
        pixel_full_width = (tmax_x + 1) * self.TileSizeWidth
        pixel_full_height = (tmax_y + 1) * self.TileSizeHeight
        mc_lt_x = (pixel_lt_x * 1.0 / pixel_full_width) * DOMAIN_LEN * 2 - DOMAIN_LEN
        mc_lt_y = DOMAIN_LEN - (pixel_lt_y * 1.0 / pixel_full_height) * DOMAIN_LEN * 2
        mc_rb_x = (pixel_rb_x * 1.0 / pixel_full_width) * DOMAIN_LEN * 2 - DOMAIN_LEN
        mc_rb_y = DOMAIN_LEN - (pixel_rb_y * 1.0 / pixel_full_height) * DOMAIN_LEN * 2

        # make json result
        result = {
            # tile info
            'tile_minx':bd_left,
            'tile_maxx':bd_right,
            'tile_miny':bd_bottom,
            'tile_maxy':bd_top,
            # pixel info
            'pixel_width':(tile_max_x - tile_min_x + 1) * self.TileSizeWidth,
            'pixel_height':(tile_max_y - tile_min_y + 1) * self.TileSizeHeight,
            # gps info
            'gps_minlat':gps_rb_lat,
            'gps_maxlat':gps_lt_lat,
            'gps_minlng':gps_lt_lng,
            'gps_maxlng':gps_rb_lng,
            # mercator info
            'mc_minx':mc_lt_x,
            'mc_maxx':mc_rb_x,
            'mc_miny':mc_lt_y,
            'mc_maxy':mc_rb_y,
            # tile total
            'total':(tile_max_x - tile_min_x + 1) * (tile_max_y - tile_min_y + 1)
            }
        
        return result
        
        
        

    def GetTiles(self, top_lat, left_lng, bottom_lat, right_lng, zoom, buff = 0):
        # cal region small tile count
        # top_lat, left_lng, bottom_lat, right_lng: region
        # / top left bottom right
        # / y axis: lat 90 de ~ -90 de
        # / x axis: lng -180 de ~ 180 de
        # zoom: 0 ~ 19
        # buff: tile buffer

        # region -> tile extent
        left, top = self.FromCoordinateToTileXY(top_lat, left_lng, zoom)            # tile
        right, bottom = self.FromCoordinateToTileXY(bottom_lat, right_lng, zoom)    # tile
        tmin_x, tmin_y = self.GetTileMatrixMinXY(zoom)                              # tile matrix size min
        tmax_x, tmax_y = self.GetTileMatrixMaxXY(zoom)                              # tile matrix size max

        # buffer
        left = left - buff
        top = top - buff
        right = right + buff
        bottom = bottom + buff

        tile_min_x = min(max(left, tmin_x), tmax_x)
        tile_max_x = min(max(right, tmin_x), tmax_x)
        tile_min_y = min(max(top, tmin_y), tmax_y)
        tile_max_y = min(max(bottom, tmin_y), tmax_y)
        
        # tile xy -> full pixel xy
        pixel_lt_x = tile_min_x * self.TileSizeWidth
        pixel_lt_y = tile_min_y * self.TileSizeHeight
        pixel_rb_x = (tile_max_x + 1) * self.TileSizeWidth
        pixel_rb_y = (tile_max_y + 1) * self.TileSizeHeight

        # full pixel xy -> new gps extent
        gps_lt_lat, gps_lt_lng = self.FromPixelToCoordinate(pixel_lt_x, pixel_lt_y, zoom)
        gps_rb_lat, gps_rb_lng = self.FromPixelToCoordinate(pixel_rb_x, pixel_rb_y, zoom)

        # full pixel xy -> mercator coordinate xy
        pixel_full_width = (tmax_x + 1) * self.TileSizeWidth
        pixel_full_height = (tmax_y + 1) * self.TileSizeHeight
        mc_lt_x = (pixel_lt_x * 1.0 / pixel_full_width) * DOMAIN_LEN * 2 - DOMAIN_LEN
        mc_lt_y = DOMAIN_LEN - (pixel_lt_y * 1.0 / pixel_full_height) * DOMAIN_LEN * 2
        mc_rb_x = (pixel_rb_x * 1.0 / pixel_full_width) * DOMAIN_LEN * 2 - DOMAIN_LEN
        mc_rb_y = DOMAIN_LEN - (pixel_rb_y * 1.0 / pixel_full_height) * DOMAIN_LEN * 2

        # make json result
        result = {
            # tile info
            'tile_minx':tile_min_x,
            'tile_maxx':tile_max_x,
            'tile_miny':tile_min_y,
            'tile_maxy':tile_max_y,
            # pixel info
            'pixel_width':(tile_max_x - tile_min_x + 1) * self.TileSizeWidth,
            'pixel_height':(tile_max_y - tile_min_y + 1) * self.TileSizeHeight,
            # gps info
            'gps_minlat':gps_rb_lat,
            'gps_maxlat':gps_lt_lat,
            'gps_minlng':gps_lt_lng,
            'gps_maxlng':gps_rb_lng,
            # mercator info
            'mc_minx':mc_lt_x,
            'mc_maxx':mc_rb_x,
            'mc_miny':mc_lt_y,
            'mc_maxy':mc_rb_y,
            # tile total
            'total':(tile_max_x - tile_min_x + 1) * (tile_max_y - tile_min_y + 1)
            }
        
        return result

##########################################################################

class MAPMetedata:
    # map metedata
    def __init__(self, mappath, tasks):
        # init
        self.mappath = mappath  # the map path
        self.tasks = tasks      # all tasks

    def SaveTask(self):
        # save tasks to json file
        ftask = open(self.mappath + 'tasks.json', 'w')
        ftask.write(json.dumps(self.tasks))
        ftask.close()
        ShowInfo('write tasks.json complete.')

    def SaveTfw(self):
        # save tfw file
        for zoom in tasks:
            # WLD -- ESRI World File
            # A world file file is a plain ASCII text file consisting of six values separated by newlines. The format is:
            # . pixel X size (m/px)
            # . rotation about the Y axis (usually 0.0)
            # . rotation about the X axis (usually 0.0)
            # . negative pixel Y size (-m/px)
            # . X coordinate of upper left pixel center (m)
            # . Y coordinate of upper left pixel center (m)
            pixX = (tasks[zoom]['mc_maxx'] - tasks[zoom]['mc_minx']) * 1.0 / tasks[zoom]['pixel_width']
            pixY = (tasks[zoom]['mc_maxy'] - tasks[zoom]['mc_miny']) * 1.0 / tasks[zoom]['pixel_height']
            roX = 0
            roY = 0
            offsetX = tasks[zoom]['mc_minx']
            offsetY = tasks[zoom]['mc_miny']
            
            ftfw = open(self.mappath + 'L%02d.tfw' % zoom, 'w')
            ftfw.write('%.12f\n' % pixX)
            ftfw.write('%.10f\n' % roX)
            ftfw.write('%.10f\n' % roY)
            ftfw.write('%.12f\n' % pixY)
            ftfw.write('%.8f\n' % offsetX)
            ftfw.write('%.8f\n' % offsetY)
            ftfw.write('\n')
            ftfw.close()
            ShowInfo('write L%02d.tfw complete.' % zoom)

    def SaveConf(self):
        # save conf.cdi conf.xml
        # ----conf.xml
        lodinfos = ''
        gmap = GMap()
        xMin = None
        yMin = None
        xMax = None
        yMax = None
        for zoom in tasks:
            # fill lodinfo
            if (xMin == None): xMin = tasks[zoom]['mc_minx']
            if (yMin == None): yMin = tasks[zoom]['mc_miny']
            if (xMax == None): xMax = tasks[zoom]['mc_maxx']
            if (yMax == None): yMax = tasks[zoom]['mc_maxy']
            # scale resolution
            scale = gmap.GetMAPScale(zoom)
            resolution = gmap.GetGroundResolution(zoom)
            lodinfos += """
      <LODInfo xsi:type="typens:LODInfo">
        <LevelID>%d</LevelID>
        <Scale>%d</Scale>
        <Resolution>%.15f</Resolution>
      </LODInfo>
            """ % (zoom, scale, resolution)
            
        # LODInfos
        lodinfos = """
    <LODInfos xsi:type="typens:ArrayOfLODInfo">
    %s
    </LODInfos>
        """ % lodinfos
        # TileImageInfo
        tileimageinfo = """
  <TileImageInfo xsi:type="typens:TileImageInfo">
    <CacheTileFormat>JPEG</CacheTileFormat>
    <CompressionQuality>75</CompressionQuality>
    <Antialiasing>false</Antialiasing>
  </TileImageInfo>
        """
        # CacheStorageInfo
        cachestorageinfo = """
  <CacheStorageInfo xsi:type="typens:CacheStorageInfo">
    <StorageFormat>esriMapCacheStorageModeExploded</StorageFormat>
    <PacketSize>0</PacketSize>
  </CacheStorageInfo>
        """
        # SpatialReference
        spatialreference = """
    <SpatialReference xsi:type="typens:ProjectedCoordinateSystem">
      <WKT>PROJCS["WGS_1984_Web_Mercator",GEOGCS["GCS_WGS_1984_Major_Auxiliary_Sphere",DATUM["D_WGS_1984_Major_Auxiliary_Sphere",SPHEROID["WGS_1984_Major_Auxiliary_Sphere",6378137.0,0.0]],PRIMEM["Greenwich",0.0],UNIT["Degree",0.0174532925199433]],PROJECTION["Mercator"],PARAMETER["false_easting",0.0],PARAMETER["false_northing",0.0],PARAMETER["central_meridian",0.0],PARAMETER["standard_parallel_1",0.0],UNIT["Meter",1.0],AUTHORITY["ESRI",102113]]</WKT>
    </SpatialReference>
        """
        # TileOrigin
        tileorigin = """
    <TileOrigin xsi:type="typens:PointN">
      <X>-20037508.342787001</X>
      <Y>20037508.342787001</Y>
    </TileOrigin>
        """
        # TileCols
        tilecols = """
    <TileCols>256</TileCols>
        """
        # TileRows
        tilerows = """
    <TileRows>256</TileRows>
        """
        # DPI
        dpi = """
    <DPI>96</DPI>
        """
        # TileCacheInfo
        tilecacheinfo = """
  <TileCacheInfo xsi:type="typens:TileCacheInfo">
{SpatialReference}
{TileOrigin}
{TileCols}
{TileRows}
{DPI}
{LODInfos}
  </TileCacheInfo>
        """.format(SpatialReference = spatialreference,
                   TileOrigin = tileorigin,
                   TileCols = tilecols,
                   TileRows = tilerows,
                   DPI = dpi,
                   LODInfos = lodinfos)
        # CacheInfo
        cacheinfo = """<?xml version="1.0" encoding="utf-8"?>

<CacheInfo xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
            xmlns:xs="http://www.w3.org/2001/XMLSchema"
            xmlns:typens="http://www.esri.com/schemas/ArcGIS/10.0"
            xsi:type="typens:CacheInfo">
{TileCacheInfo}
{TileImageInfo}
{CacheStorageInfo}
</CacheInfo>
        """.format(TileCacheInfo = tilecacheinfo,
                   TileImageInfo = tileimageinfo,
                   CacheStorageInfo = cachestorageinfo)

        fxml = open(self.mappath + 'conf.xml', 'w')
        fxml.write(cacheinfo.encode('utf8'))
        fxml.close()
        ShowInfo('write conf.xml complete.')
        
        # ----conf.cdi
        fcdi = open(self.mappath + 'conf.cdi', 'w')
        cditxt = """<?xml version="1.0" encoding="utf-8"?>

<EnvelopeN xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
           xmlns:xs="http://www.w3.org/2001/XMLSchema"
           xmlns:typens="http://www.esri.com/schemas/ArcGIS/10.0"
           xsi:type="typens:EnvelopeN">
  <XMin>%.9f</XMin>
  <YMin>%.9f</YMin>
  <XMax>%.9f</XMax>
  <YMax>%.9f</YMax>
</EnvelopeN>
            """ % (-20037500, -20037500, 20037500, 20037500)
        
        fcdi.write(cditxt.encode('utf8'))
        fcdi.close()
        ShowInfo('write conf.cdi complete.')
        



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
    #    0: {'tile_minx':0, 'tile_maxx':0, 'tile_miny':0, 'tile_maxy':0,            < tile info
    #        'pixel_width':0, 'pixel_height':0,                                     < tile pixel size
    #        'gps_minlat':24, 'gps_maxlat':24, 'gps_minlng':109, 'gps_maxlng':109,  < gps info
    #        'mc_minx':0, 'mc_maxx':0, 'mc_miny':0, 'mc_maxy':0,                    < mercator info
    #        'total':0
    #        },
    #    1: {'tile_minx':0, 'tile_maxx':0, 'tile_miny':0, 'tile_maxy':0,            < tile info
    #        'pixel_width':0, 'pixel_height':0,                                     < tile pixel size
    #        'gps_minlat':24, 'gps_maxlat':24, 'gps_minlng':109, 'gps_maxlng':109,  < gps info
    #        'mc_minx':0, 'mc_maxx':0, 'mc_miny':0, 'mc_maxy':0,                    < mercator info
    #        'total':0
    #        }
    #   }
    text = open(fname, 'r').read().encode('utf8')
    decodejson = json.loads(text)
    tasks = {}

    gmap = GMap()

    top_lat = decodejson['top_lat']
    left_lng = decodejson['left_lng']
    bottom_lat = decodejson['bottom_lat']
    right_lng = decodejson['right_lng']

    
    for tile in decodejson['tiles']:
        zoom = int(tile['zoom'])
        buff = int(tile['buffer'])

        task = gmap.GetBDTiles(top_lat, left_lng, bottom_lat, right_lng, zoom, buff)
        tasks[zoom] = task
    return tasks



##########################################################################

class BaiduMercator:
    # 百度墨卡托坐标
    EARTHRADIUS = 6370996.81
    MCBAND = [12890594.86, 8362377.87, 5591021, 3481989.83, 1678043.12, 0]
    LLBAND = [75, 60, 45, 30, 15, 0]
    MC2LL = [
        [1.410526172116255e-8, 0.00000898305509648872, -1.9939833816331, 200.9824383106796, -187.2403703815547, 91.6087516669843, -23.38765649603339, 2.57121317296198, -0.03801003308653, 17337981.2],
        [ - 7.435856389565537e-9, 0.000008983055097726239, -0.78625201886289, 96.32687599759846, -1.85204757529826, -59.36935905485877, 47.40033549296737, -16.50741931063887, 2.28786674699375, 10260144.86], 
        [ - 3.030883460898826e-8, 0.00000898305509983578, 0.30071316287616, 59.74293618442277, 7.357984074871, -25.38371002664745, 13.45380521110908, -3.29883767235584, 0.32710905363475, 6856817.37],
        [ - 1.981981304930552e-8, 0.000008983055099779535, 0.03278182852591, 40.31678527705744, 0.65659298677277, -4.44255534477492, 0.85341911805263, 0.12923347998204, -0.04625736007561, 4482777.06],
        [3.09191371068437e-9, 0.000008983055096812155, 0.00006995724062, 23.10934304144901, -0.00023663490511, -0.6321817810242, -0.00663494467273, 0.03430082397953, -0.00466043876332, 2555164.4],
        [2.890871144776878e-9, 0.000008983055095805407, -3.068298e-8, 7.47137025468032, -0.00000353937994, -0.02145144861037, -0.00001234426596, 0.00010322952773, -0.00000323890364, 826088.5]
    ]
    LL2MC = [
        [ - 0.0015702102444, 111320.7020616939, 1704480524535203, -10338987376042340, 26112667856603880, -35149669176653700, 26595700718403920, -10725012454188240, 1800819912950474, 82.5],
        [0.0008277824516172526, 111320.7020463578, 647795574.6671607, -4082003173.641316, 10774905663.51142, -15171875531.51559, 12053065338.62167, -5124939663.577472, 913311935.9512032, 67.5],
        [0.00337398766765, 111320.7020202162, 4481351.045890365, -23393751.19931662, 79682215.47186455, -115964993.2797253, 97236711.15602145, -43661946.33752821, 8477230.501135234, 52.5],
        [0.00220636496208, 111320.7020209128, 51751.86112841131, 3796837.749470245, 992013.7397791013, -1221952.21711287, 1340652.697009075, -620943.6990984312, 144416.9293806241, 37.5],
        [ - 0.0003441963504368392, 111320.7020576856, 278.2353980772752, 2485758.690035394, 6070.750963243378, 54821.18345352118, 9540.606633304236, -2710.55326746645, 1405.483844121726, 22.5],
        [ - 0.0003218135878613132, 111320.7020701615, 0.00369383431289, 823725.6402795718, 0.46104986909093, 2351.343141331292, 1.58060784298199, 8.77738589078284, 0.37238884252424, 7.45]
    ]


    def PixelToPoint(sefl, point, zoom, center, bounds):
        # 像素到坐标
        zoomUnits = self.GetZoomUnits(zoom)
        mercatorx = center.lng + zoomUnits * (point.x - bounds.width / 2)
        mercatory = center.lat - zoomUnits * (point.y - bounds.height / 2)
        return self.MercatorToLngLat(mercatorx, mercatory)

    def PointToPixel(self, coord, zoom, center, bounds):
        # 坐标到像素
        point = self.LngLatToMercator(coord)
        units = self.GetZoomUnits(zoom)
        x = round((point.lng - center.lng) / units + bounds.width / 2)
        y = round((center.lat - point.lat) / units + bounds.height / 2)
        return BDPoint(x, y)

    def GetZoomUnits(self, zoom):
        # 获取分辨率
        return pow(2, (18-zoom))



##################################################################################################
    def MercatorToPixel(self, x, y, zoom):
        # 墨卡托坐标转像素坐标
        pixelX = math.floor(x * math.pow(2, zoom - 18))
        pixelY = math.floor(y * math.pow(2, zoom - 18))
        return pixelX, pixelY

    def PixelToTile(self, pixelX, pixelY):
        # 像素坐标转瓦片XY 图块坐标
        tileX = int(math.floor(pixelX / 256))
        tileY = int(math.floor(pixelY / 256))
        return tileX, tileY
    
    def MercatorToLngLat(self, x, y):
        # 墨卡托坐标转经纬度
        # x: X坐标 (经度)
        # y: Y坐标 (纬度)
        # return: lng, lat 经度, 纬度
        mc = None
        absx = abs(x)
        absy = abs(y)
        for i in range(0, len(self.MCBAND)):
            if (absy >= self.MCBAND[i]):
                mc = self.MC2LL[i]
                break
        lng, lat = self.Convertor(x, y, mc)
        lng = round(lng, 6)
        lat = round(lat, 6)
        return lng, lat

    def LngLatToMercator(self, lng, lat):
        # 经纬度转墨卡托坐标
        # lng: 经度
        # lat: 纬度
        # return: X, Y 墨卡托平面坐标XY
        mc = None
        lng = self.GetLoop(lng, -180, 180)
        lat = self.GetRange(lat, -74, 74)

        for i in range(0, len(self.LLBAND)):
            if lat > self.LLBAND[i]:
                mc = self.LL2MC[i]
                break
        if mc == None:
            for i in range(len(self.LLBAND)-1, -1, -1):
                if (lat <= -self.LLBAND[i]):
                    mc =  self.LL2MC[i]
                    break
        x, y = self.Convertor(lng, lat, mc)
        x = round(x, 2)
        y = round(y, 2)
        return x, y

    def LngLatToTile(self, lng, lat, zoom):
        # 经纬度转百度瓦片XY
        mx, my = self.LngLatToMercator(lng, lat)
        px, py = self.MercatorToPixel(mx, my, zoom)
        tx, ty = self.PixelToTile(px, py)
        return tx, ty

    def GetLoop(self, lng, a, b):
        # 经度范围
        while (lng > b):
            lng -= b - a
        while (lng < a):
            lng += b - a
        return lng

    def GetRange(self, lat, a, b):
        # 纬度范围
        lat = max(lat, a)
        lat = min(lat, b)
        return lat

    def Convertor(self, xlng, ylat, mc):
        # 数据转换
        # xlng:     X 或者 经度
        # ylat:     Y 或者 纬度
        # mc:       转换对照表
        # return:   经纬度 或 XY
        newxlng = mc[0] + mc[1] * abs(xlng)
        c = abs(ylat) / mc[9]
        newylat = mc[2] + mc[3] * c + mc[4] * c * c + mc[5] * c * c * c + mc[6] * c * c * c * c + mc[7] * c * c * c * c * c + mc[8] * c * c * c * c * c * c
        if(xlng < 0): newxlng *= -1
        if(ylat < 0): newylat *= -1
        return newxlng, newylat

    def BaiduTileToStandard(self, bdTileX, bdTileY, zoom):
        # 百度瓦片转标准瓦片坐标
        # 1. (0,0)点移到左上角
        # 2. Y轴取反
        size = 1 << zoom
        tileX = bdTileX + size / 2
        tileY = bdTileY + size / 2
        tileY = size - tileY - 1
        return tileX, tileY

    def StandardTileToBaidu(self, tileX, tileY, zoom):
        # 标准瓦片坐标转百度瓦片坐标
        # 1. (0,0)点移到中心
        # 2. Y轴取反
        if (zoom == 0): return 0, 0
        size = 1 << zoom
        bdTileX = tileX - size / 2
        bdTileY = tileY - size / 2
        bdTileY = - bdTileY - 1
        return bdTileX, bdTileY
    
##################################################################################################
    


if __name__ == '__main__':
    #
    print '[==DoDo==]'
    print 'Tile Maker.'
    print 'Encode: %s' %  sys.getdefaultencoding()

    # test
    # top_lat, left_lng, bottom_lat, right_lng, zoom, buff = 0
    #gmap = GMap()
    #result = gmap.GetTiles(24.305860391780953, 109.43051218986511,
    #                       24.302868336020282, 109.43383812904358,
    #                       17, 0)
    #print json.dumps(result)

    # !!!! bundle file !!!!
    # http://www.cnblogs.com/yuantf/p/3320876.html
    
    # init
    maxThreads = 16                         # the num of thread
    outpath = './outbaidu/'                 # output path
    jsonfile = 'task.json'                  # task json file
    mapname = 'MAP'                         # map name

    #gmap = GMap()
    #gmap.GetBDTiles(24.76429, 109.10797, 24.03267, 109.95529, 10)
    
    map_path = outpath + mapname + '/'
    lay_path = map_path + '_alllayers/'
    
    # make output dir
    
    if (os.path.exists(lay_path)==False):
        os.makedirs(lay_path)

    # load task
    tasks = GetTask(jsonfile)

    #print '#################'
    #test = BaiduMercator()
    #print test.BaiduTileToStandard(0, -1, 2)
    
    # do work
    success = True
    try:
        # save metedata
        mmetedata = MAPMetedata(map_path, tasks)
        mmetedata.SaveTask()
        mmetedata.SaveTfw()
        mmetedata.SaveConf()
        
        for zoom in tasks:
            # each zoom
            minX = tasks[zoom]['tile_minx']     # the left X index
            maxX = tasks[zoom]['tile_maxx']     # the right X index
            minY = tasks[zoom]['tile_miny']     # the buttom Y index
            maxY = tasks[zoom]['tile_maxy']     # the top Y index
            
            # list of tile
            tiles = []
            for y in range(minY, maxY + 1):
                for x in range(minX, maxX + 1):
                    tiles.append([x, y])

            print '============================='
            print '{0} -> [{1}, {2}, {3}, {4}] / zoom: {5} ...'.format(time.strftime(r'%m/%d %H:%M:%S'), minX, maxX, minY, maxY, zoom)
            print '{0} -> total: {1} ...\n'.format(time.strftime(r'%m/%d %H:%M:%S'), (maxX - minX + 1) * (maxY - minY + 1))
            
            # one of zooms
            spider = Spider(lay_path)
            spider.Work(maxThreads, tiles, zoom)
        
            
    except Exception, ex:
        print ex
        success = False
        
    print 'Finish', success
    

    
