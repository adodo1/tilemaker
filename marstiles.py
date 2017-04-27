#!/usr/bin/env python
# encoding: utf-8

import os, sys, math, json, struct, re, shutil
import numpy as np
from PIL import Image

# 火星瓦片转WGS瓦片
# WGS瓦片转火星瓦片

# > 火星瓦片 marsTileX,marsTileY -> 火星像素 marsPixX,marsPixY
# > 火星像素 marsPixX,marsPixY   -> 

# 1. 设置512张图片的缓冲区
# 2. 火星坐标转GPS坐标
# 3. 建立两者之间的联系

PI              = 3.14159265358979323846                    # PI
EARTH_RADIUS    = 6378245.0                                 # 地球半径
EE              = 0.00669342162296594323                    # 扁率
X_PI            = 3.14159265358979324 * 3000.0 / 180.0      # 

class BaiduCoor:
    # 百度坐标
    def BD_encrypt(self, gglat, gglng):
        # 火星坐标转换为百度坐标
        # gglat 火星纬度
        # gglng 火星经度
        x = gglng
        y = gglat
        z = math.sqrt(x*x + y*y) + 0.00002 * math.sin(y * X_PI)
        theta = math.atan2(y, x) + 0.000003 * math.cos(x * X_PI)
        bdlng = z * math.cos(theta) + 0.0065
        bdlat = z * math.sin(theta) + 0.006
        return bdlat, bdlng

    def BD_decrypt(self, bdlat, bdlng):
        # 百度坐标转火星坐标
        x = bdlng - 0.0065
        y = bdlat - 0.006
        z = math.sqrt(x*x + y*y) - 0.00002 * math.sin(y * X_PI)
        theta = math.atan2(y, x) - 0.000003 * math.cos(x * X_PI)
        gglng = z * math.cos(theta)
        gglat = z * math.sin(theta)
        return gglat, gglng

class MarsCoor:
    # 火星坐标
    def OutOfChina(self, lat, lng):
        # 坐标是否在中国外
        if (lng < 72.004 or lng > 137.8347):
            return True
        if (lat < 0.8293 or lat > 55.8271):
            return True
        return False

    def TransformLat(self, x, y):
        # 纬度转换
        ret = -100.0 + 2.0 * x + 3.0 * y + 0.2 * y * y + 0.1 * x * y + 0.2 * math.sqrt(abs(x))
        ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
        ret += (20.0 * math.sin(y * PI) + 40.0 * math.sin(y / 3.0 * PI)) * 2.0 / 3.0
        ret += (160.0 * math.sin(y / 12.0 * PI) + 320 * math.sin(y * PI / 30.0)) * 2.0 / 3.0
        return ret

    def TransformLng(self, x, y):
        # 经度转换
        ret = 300.0 + x + 2.0 * y + 0.1 * x * x + 0.1 * x * y + 0.1 * math.sqrt(abs(x))
        ret += (20.0 * math.sin(6.0 * x * PI) + 20.0 * math.sin(2.0 * x * PI)) * 2.0 / 3.0
        ret += (20.0 * math.sin(x * PI) + 40.0 * math.sin(x / 3.0 * PI)) * 2.0 / 3.0
        ret += (150.0 * math.sin(x / 12.0 * PI) + 300.0 * math.sin(x / 30.0 * PI)) * 2.0 / 3.0
        return ret

    def GPS2Mars(self, wglat, wglng):
        # 地球坐标转换为火星坐标
        # wglat WGS纬度
        # wglng WGS经度
        # 返回近似火星坐标系
        if (self.OutOfChina(wglat, wglng)):
            return wglat, wglng
        dlat = self.TransformLat(wglng - 105.0, wglat - 35.0)
        dlng = self.TransformLng(wglng - 105.0, wglat - 35.0)
        radlat = wglat / 180.0 * PI
        magic = math.sin(radlat)
        magic = 1 - EE * magic * magic
        sqrtmagic = math.sqrt(magic)
        dlat = (dlat * 180.0) / ((EARTH_RADIUS * (1 - EE)) / (magic * sqrtmagic) * PI)
        dlng = (dlng * 180.0) / (EARTH_RADIUS / sqrtmagic * math.cos(radlat) * PI)
        mglat = wglat + dlat
        mglng = wglng + dlng
        return mglat, mglng

    def Mars2GPS(self, gclat, gclng):
        # 采用二分法 火星坐标反算地球坐标
        # gclat 火星坐标纬度
        # gclng 火星坐标经度
        initDelta = 0.01
        threshold = 0.000000001
        dlat = initDelta
        dlng = initDelta
        mlat = gclat - dlat
        mlng = gclng - dlng
        plat = gclat + dlat
        plng = gclng + dlng
        wgslat = 0
        wgslng = 0
        i = 0
        while (True):
            wgslat = (mlat + plat) / 2.0
            wgslng = (mlng + plng) / 2.0
            tmplat, tmplng = self.GPS2Mars(wgslat, wgslng)
            dlat = tmplat - gclat
            dlng = tmplng - gclng

            if ((abs(dlat)<threshold) and (abs(dlng)<threshold)):
                break
            if (dlat > 0): plat = wgslat
            else: mlat = wgslat
            if (dlng > 0): plng = wgslng
            else: mlng = wgslng

            i += 1
            if (i>10000): break
        return wgslat, wgslng


class MercatorCoor:
    # 墨卡托投影
    def __init__(self):
        # 初始化
        self.MinLatitude = -85.05112878     # min latitude
        self.MaxLatitude = 85.05112878      # max latitude
        self.MinLongitude = -180            # min longitude
        self.MaxLongitude = 180             # max longitude
        self.TileSizeWidth = 256            # tile width
        self.TileSizeHeight = 256           # tile height

    def FromLatLngToPixel(self, lat, lng, zoom):
        # 经纬度转墨卡托像素坐标
        lat = self.Clip(lat, self.MinLatitude, self.MaxLatitude)
        lng = self.Clip(lng, self.MinLongitude, self.MaxLongitude)

        x = (lng + 180) / 360.0
        sinlat = math.sin(lat * PI / 180.0)
        y = 0.5 - math.log((1 + sinlat) / (1 - sinlat)) / (4 * PI)

        fullPixelWidth, fullPixelHeight = self.GetTileMatrixSizePixel(zoom)

        x = int(self.Clip(x * fullPixelWidth + 0.5, 0, fullPixelWidth - 1))
        y = int(self.Clip(y * fullPixelHeight + 0.5, 0, fullPixelHeight - 1))

        return x, y

    def FromPixelToLatLng(self, x, y, zoom):
        # 像素坐标转经纬度
        fullPixelWidth, fullPixelHeight = self.GetTileMatrixSizePixel(zoom)
        
        xx = (self.Clip(x, 0, fullPixelWidth - 1) * 1.0 / fullPixelWidth) - 0.5
        yy = 0.5 - (self.Clip(y, 0, fullPixelHeight - 1) * 1.0 / fullPixelHeight)
        
        lat = 90 - 360 * math.atan(math.exp(-yy * 2 * PI)) / PI
        lng = 360 * xx
        return lat, lng

    def FromPixelToTileXY(self, pixX, pixY):
        # 像素坐标XY转瓦片块坐标XY
        tileX = int(pixX / self.TileSizeWidth)
        tileY = int(pixY / self.TileSizeHeight)
        return tileX, tileY

    def FromTileXYToPixel(self, tileX, tileY):
        # 计算瓦片块XY左上角的像素坐标XY
        pixX = tileX * self.TileSizeWidth
        pixY = tileY * self.TileSizeHeight
        return pixX, pixY

    def Clip(self, num, minval, maxval):
        # 裁剪
        return min(max(num, minval), maxval)

    def GetTileMatrixSizePixel(self, zoom):
        # zoom等级下总瓦片像素大小
        tileWidth, tileHeight = self.GetTileMatrixSizeXY(zoom)
        return tileWidth * self.TileSizeWidth, tileHeight * self.TileSizeHeight

    def GetTileMatrixSizeXY(self, zoom):
        # 总瓦片-块-
        minWidth, minHeight = self.GetTileMatrixMinXY(zoom)
        maxWidth, maxHeight = self.GetTileMatrixMaxXY(zoom)
        return maxWidth - minWidth + 1, maxHeight - minHeight + 1

    def GetTileMatrixMinXY(self, zoom):
        # zoom等级下瓦片-块-最小坐标
        return 0,0

    def GetTileMatrixMaxXY(self, zoom):
        # zoom等级下瓦片-块-最大坐标
        xy = 1 << zoom
        return xy - 1, xy - 1
    
class MarsTiles:
    # 火星瓦片
    def __init__(self, tilesfrom, tilesto):
        # 初始化
        self.TileSizeWidth = 256            # tile width
        self.TileSizeHeight = 256           # tile height
        self.tilesfrom = tilesfrom          # tile from
        self.tilesto = tilesto              # tile to
        self.nullim = Image.new('RGB', (256,256), (255,255,255))

    def TileMarsToStandard(self, tileX, tileY, zoom):
        # 火星瓦片-块-转标准瓦片-块-
        # 用左上角点求解
        mercator = MercatorCoor()
        marPixX, marPixY = mercator.FromTileXYToPixel(tileX, tileY)
        staPixX, staPixY = self.PixMarsToStandard(marPixX, marPixY, zoom)
        newTileX, newTileY = mercator.FromPixelToTileXY(staPixX, staPixY)
        return newTileX, newTileY

    def TileStandardToMars(self, tileX, tileY, zoom):
        # 标准瓦片-块-转火星瓦片-块-
        # 用左上角点求解
        mercator = MercatorCoor()
        staPixX, staPixY = mercator.FromTileXYToPixel(tileX, tileY)
        marPixX, marPixY = self.PixStandardToMars(staPixX, staPixY, zoom)
        newTileX, newTileY = mercator.FromPixelToTileXY(marPixX, marPixY)
        return newTileX, newTileY
        
    def PixMarsToStandard(self, pixX, pixY, zoom):
        # 火星瓦片像素坐标转标准瓦片像素坐标
        mercator = MercatorCoor()
        mars = MarsCoor()
        malat, malng = mercator.FromPixelToLatLng(pixX, pixY, zoom)         # 像素坐标转火星坐标
        stlat, stlng = mars.Mars2GPS(malat, malng)                          # 火星坐标转标准坐标
        newpixX, newpixY = mercator.FromLatLngToPixel(stlat, stlng, zoom)   # 标准坐标转像素坐标
        return newpixX, newpixY
        
    def PixStandardToMars(self, pixX, pixY, zoom):
        # 标准瓦片像素坐标转火星瓦片像素坐标
        mercator = MercatorCoor()
        mars = MarsCoor()
        stlat, stlng = mercator.FromPixelToLatLng(pixX, pixY, zoom)         # 像素坐标转标准坐标
        malat, malng = mars.GPS2Mars(stlat, stlng)                          # 火星坐标转标准坐标
        newpixX, newpixY = mercator.FromLatLngToPixel(malat, malng, zoom)   # 标准坐标转像素坐标
        return newpixX, newpixY
        
    def GetExtentPixs(self, minPixX, minPixY, maxPixX, maxPixY, zoom):
        # 获取指定范围内的像素 [包含起始像素 包含结束像素]
        # minPixX, minPixY 左上角
        # maxPixX, maxPixY 右下角
        # zoom 等级
        mc = MercatorCoor()
        ltTileX, ltTileY = mc.FromPixelToTileXY(minPixX, minPixY)   # 左上
        rbTileX, rbTileY = mc.FromPixelToTileXY(maxPixX, maxPixY)   # 右下

        # 最简单的方法是取出相关瓦片拼起来
        fullw = (rbTileX - ltTileX + 1) * self.TileSizeWidth        # 拼图总宽度
        fullh = (rbTileY - ltTileY + 1) * self.TileSizeHeight       # 拼图总高度

        # 读取相关瓦片信息开始拼图
        #newim = Image.new('RGB', (fullw,fullh), (255,255,255))
        #newarr = np.array(newim)
        imrows = []
        for xx in range(ltTileX, rbTileX+1):
            imcols = []
            for yy in range(ltTileY, rbTileY+1):
                arr = self.GetTile(xx, yy, zoom)
                imcols.append(arr)
            imrows.append(np.row_stack(imcols))
        newarr = np.column_stack(imrows)


        # 计算相对坐标取出
        oriX = minPixX - ltTileX * 256
        oriY = minPixY - ltTileY * 256
        w = maxPixX - minPixX + 1
        h = maxPixY - minPixY + 1

        # 矩阵中取出
        outarr = newarr[oriY:oriY+h, oriX:oriX+w]

        # 导出
        return outarr

    def SaveTile(self, nn, tileX, tileY, zoom):
        # 保存新瓦片
        savefile = '%s/L%02d/R%08x/C%08x.JPG' % (self.tilesto, zoom, tileY, tileX)
        path, name = os.path.split(savefile)
        if (os.path.exists(path)==False): os.makedirs(path)
        
        newim = Image.fromarray(nn, 'RGB')
        newim = newim.resize((self.TileSizeWidth, self.TileSizeHeight), Image.ANTIALIAS)
        newim.save(savefile, 'JPEG')

    def GetTile(self, tileX, tileY, zoom):
        # 获取指定瓦片 如果没有返回空白图片
        tileFile = '%s/L%02d/R%08x/C%08x.JPG' % (self.tilesfrom, zoom, tileY, tileX)
        if (os.path.exists(tileFile)):
            im = Image.open(tileFile).convert('RGB')
        else:
            print 'no found file: ', tileFile
            im = self.nullim
        arr = np.array(im)
        return arr

if __name__ == '__main__':
    #
    print '[==DoDo==]'
    print 'Bundle Maker.'
    print 'Encode: %s' %  sys.getdefaultencoding()

    
    inpath = u'E:/DoDo/Python/瓦片图制作/out/MAP_MARS/_alllayers'
    outpath = u'E:/DoDo/Python/瓦片图制作/out/MAP_OUT/_alllayers'

    if os.path.exists(outpath) == False:
        os.makedirs(outpath)


    #mc = MercatorCoor()
    #mt = MarsTiles('./', './')
    #pixX, pixY = mc.FromLatLngToPixel(24.306875, 109.431092, 19)
    #print mc.FromPixelToTileXY(pixX,pixY)
    #print mc.FromPixelToLatLng(53955375, 28880131, 18)

    #print mt.PixStandardToMars(107907796.5, 57761797.5, 19)
    #print mt.PixMarsToStandard(107909510.5, 57762899.5, 19)
    

    
    #ff = u'E:/DoDo/Python/瓦片图制作/out/MAP_OUT/_alllayers/L19/R0003715f/C00066e8a.JPG'
    #im = Image.open(ff).convert('RGB')
    #print im.size
    

    '''
    mc = MercatorCoor()
    mt = MarsTiles()
    print mt.PixMarsToStandard(53955375, 28880131, 18)
    print mt.PixStandardToMars(53954519, 28879578, 18)

    infile = u"E:\\DoDo\\Python\\瓦片图制作\\out\\MAP\\_alllayers\\L03\\R00000002\\C00000007.JPG"
    infile = u"E:\\DoDo\\Python\\瓦片图制作\\outbaidu\\MAP\\_alllayers\\L17\\R0000eae6\\C00015cfd.PNG"
    im = Image.open(infile).convert('RGB')
    aa = np.array(im)

    mt = MarsTiles()
    mt.GetExtentPixs(50,50, 280,280, 1)
    '''

    
    print 'scan files'
    dirs = []
    files = []
    # 遍历目录
    for parent,dirnames,filenames in os.walk(inpath):
        for dirname in dirnames:
            dirs.append(os.path.join(parent, dirname))
        for filename in filenames:
            files.append(os.path.join(parent, filename))
    
    print 'file count: %s' % len(files)

    mt = MarsTiles(inpath, outpath)
    mc = MercatorCoor()

    #nn = mt.GetExtentPixs(512, 0, 767, 255, 2)
    #print nn.size

    index = 0
    for fname in files:
        # ./out/MAP/_alllayers/L03/R00000000/C00000004.JPG
        fname = fname.replace('\\', '/')
        pattern = '/L(?P<level>[0-9a-fA-F]+?)/R(?P<row>[0-9a-fA-F]+?)/C(?P<col>[0-9a-fA-F]+?)\.'
        matchdata = re.search(pattern, fname, re.I)
        if matchdata == None: continue
        # 
        level = int(matchdata.group('level'))
        row = int(matchdata.group('row'), 16)
        col = int(matchdata.group('col'), 16)

        marTileX = col
        marTileY = row
        zoom = level

        if (marTileX == 421521 and marTileY == 225636):
            print "aaaaaaaaaaa"
            print "bbbbbbbbbbb"

        try:
            staTileX, staTileY = mt.TileMarsToStandard(marTileX, marTileY, zoom)        # 火星瓦片块转标准块
            staPixX, staPixY = mc.FromTileXYToPixel(staTileX, staTileY)                 # 对应标准瓦片块左上角像素坐标
            marPixLTX, marPixLTY = mt.PixStandardToMars(staPixX, staPixY, zoom)                     # 左上角火星像素
            marPixRBX, marPixRBY = mt.PixStandardToMars(staPixX + 256 - 1, staPixY + 256 - 1, zoom) # 右下角火星像素
            nn = mt.GetExtentPixs(marPixLTX, marPixLTY, marPixRBX, marPixRBY, zoom)     # 取图
            mt.SaveTile(nn, staTileX, staTileY, zoom)

            #print '-------------------------------------------------'
            #print u'/L%02d/R%08x/C%08x.JPG' % (zoom, staTileX, staTileY)
            #print u'尺寸: ', nn.size
            #print u'原始火星块: ', marTileX, marTileY
            #print u'标准瓦片块: ', staTileX, staTileY
            #print u'火星左上角: ', marPixLTX, marPixLTY
            #print u'火星右下角: ', marPixRBX, marPixRBY
            #print u'宽高: ', marPixRBX - marPixLTX + 1, marPixRBY - marPixLTY + 1

            #w = marPixRBX - marPixLTX + 1
            #h = marPixRBY - marPixLTY + 1
            #if (w != 256 or h != 256):
            #    print '/L%02d/R%08x/C%08x.JPG>>: %d x %d' % (zoom, staTileX, staTileY, w, h)
        except Exception, e:  
            print u'bad > {0}/{1} [L{2} R{3} C{4}]: {5}'.format(index, len(files), level, row, col, fname)
            print e
        
        
        index += 1
        if (index % 128 == 0):
            print u'{0}/{1} [L{2} R{3} C{4}]: {5}'.format(index, len(files), level, row, col, fname)
    
    





        
