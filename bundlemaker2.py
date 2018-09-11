#!/usr/bin/env python
# encoding: utf-8

import os, sys, math, json, struct, re, zlib, gzip, StringIO
from io import BytesIO as StringIO

# 1. 将碎瓦片生成ESRI的紧凑数据
# 2. 将ESRI紧凑数据解包成零碎数据
# 3. 解析ESRI切片元数据

# PS:
# 瓦片索引从左上角开始(0,0)
# 先列 再行

# 参考:
# https://github.com/andrewmagill/unbundler
# http://www.cnblogs.com/yuantf/p/3320876.html
# https://github.com/sainsb/tilecannon > TileController.cs
# https://github.com/F-Sidney/SharpMapTileLayer > LocalTileCacheLayer.cs

# bundle瓦片定义 2.0 版本
#
# 前64字节 =头部定义=
# 00-03: 固定 03000000
# 04-05: 内部瓦片数量
# 06-23: 未知 全部用00代替好了 000000000000050000000000000000000000
# 24-27: bundle文件大小
# 28-51: 未知 00000000 28000000 00000000 14000200 03000000 00000000
# 52-55: 瓦片个数 128*128个 
# 56-63: 未知 05000000 00000200

# 64-131135:
#   每组8字节 总共 128 * 128 组
#   每组 0-4字节(5字节): 瓦片数据偏移量
#   每组 5-7字节(3字节): 文件大小

# 131136-:
#   真实瓦片数据
#   4字节瓦片大小
#   瓦片数据
#   4字节瓦片大小
#   瓦片数据
#   ...


PACKET_SIZE = 128       # 包大小

class BundleClass:
    # 存储瓦片的文件类
    def __init__(self, fname):
        self.fname = fname
        pass

    def HexToInt5(self, value):
        # 字节转整形
        # 例如: 0xFF00000000
        # 反序: 0x00000000FF
        # 再转成整数: 255

        #value = value + '000000'.decode('hex')
        #result = struct.unpack('q', value)[0]
        result = (ord(value[4]) & 0xFF) << 32 | \
                 (ord(value[3]) & 0xFF) << 24 | \
                 (ord(value[2]) & 0xFF) << 16 | \
                 (ord(value[1]) & 0xFF) << 8 | \
                 (ord(value[0]) & 0xFF)
        return int(result)

    def HexToInt3(self, value):
        # 字节转整形
        # 例如: 0xFF0000
        # 反序: 0x0000FF
        # 再转成整数: 255
        result = (ord(value[2]) & 0xFF) << 16 | \
                 (ord(value[1]) & 0xFF) << 8 | \
                 (ord(value[0]) & 0xFF)
        return int(result)

    def IntToHex5(self, value):
        # 整形转5字节
        result = struct.pack('q', value)[0:5]
        return result

    def IntToHex3(self, value):
        # 整形转3字节
        result = struct.pack('q', value)[0:3]
        return result
    
    '''
    def GetTileImage(self, position):
        # 获取单张瓦片图像
        # position 偏移量
        # returns the binary array of the image from the bundle file
        # given the path of the bundle file, and the row and column
        # of the image
        fbundle = open(self.fname, 'rb')
        fbundle.seek(position)
        value = fbundle.read(4)
        size = struct.unpack('i', value)[0]
        if (size == 0): image = None
        else: image = fbundle.read(size)
        fbundle.close()
        return image
    '''
    
    def CreateNew(self, start_row, start_col):
        # 创建新的瓦片存储文件
        bundleHead = {
            'fixed_00_03': '03000000'.decode('hex'),        # 固定 03000000
            'tilecount_04_05': '0000'.decode('hex'),        # 内部瓦片数量 <<
            'unknow_06_23': '000000000000050000000000000000000000'.decode('hex'),
            'filesize_24_27': '00000000'.decode('hex'),     # bundle文件大小 <<
            'unknow_28_51': '000000002800000000000000140002000300000000000000'.decode('hex'),
            'tileall_52_55': struct.pack('i', 128*128),     # 可以容纳的总瓦片数量
            'unknow_56_63': '0500000000000200'.decode('hex')
            }
        # <<
        fbundle = open(self.fname, 'wb')
        fbundle.write(bundleHead['fixed_00_03'])
        fbundle.write(bundleHead['tilecount_04_05'])
        fbundle.write(bundleHead['unknow_06_23'])
        fbundle.write(bundleHead['filesize_24_27'])
        fbundle.write(bundleHead['unknow_28_51'])
        fbundle.write(bundleHead['tileall_52_55'])
        fbundle.write(bundleHead['unknow_56_63'])

        # 写空索引
        for n in range(0, 128*128):
            offset = '0400000000'.decode('hex')
            size = '000000'.decode('hex')
            fbundle.write(offset)
            fbundle.write(size)
            
        fbundle.close()


    def InsertData(self, data, row, col):
        # 插入数据
        datasize = len(data)
        filesize = os.path.getsize(self.fname)

        fbundle = open(self.fname, 'rb+')
        # 处理内部瓦片数量
        fbundle.seek(4)
        tilecount = struct.unpack('i', fbundle.read(4))[0]
        tilecount = tilecount + 1
        fbundle.seek(4)
        fbundle.write(struct.pack('i', tilecount))
        
        # 处理文件总大小 文件大小+4字节
        fbundle.seek(24)
        fbundle.write(struct.pack('i', filesize + datasize + 4))

        # 处理索引
        postion = self.GetIndexPostion(row, col)
        fbundle.seek(postion)
        fbundle.write(self.IntToHex5(filesize + 4))
        fbundle.write(self.IntToHex3(datasize))

        # 写入图片数据
        fbundle.seek(0, 2)          # 跳到文件末尾
        position = fbundle.tell()   # 查询当前文件大小
        fbundle.write(struct.pack('i', datasize))
        fbundle.write(data)
        #
        fbundle.close()
        

    def GetIndexPostion(self, row, col):
        # 计算row行col列在索引文件bundle的偏移量
        # row 总体行号
        # col 总体列号
        # given a row and column, returns the position in the index
        # file where you can find the position of the actual image
        # in the bundle file
        row = row % 128
        col = col % 128
        # 
        base_pos = 64 + col * 8 * 128
        offset = row * 8
        # 
        position = base_pos + offset
        return position
    

class TileData:
    # 瓦片数据处理类
    def __init__(self, tiledir):
        # 初始化瓦片字典和索引字典
        self.bundles = {}
        self.tiledir = tiledir
        pass

    def GetBundleName(self, level, row, col):
        # 通过等级 行号 列号获取集合名字
        # row 总体行号
        # col 总体列号
        # returns the name of the bundle that will hold the image
        # if it exists given the row and column of that image
        # round down to nearest 128
        row = int(row / 128)
        row = row * 128
        col = int(col / 128)
        col = col * 128
        row = '%04x' % row
        col = '%04x' % col
        filename = 'R{}C{}'.format(row, col)
        # 
        dirname = 'L%02d' % int(level)
        
        bundlename = dirname + '/' + filename
        return bundlename

    def GetBundleRowCol(self, row, col):
        # 获取起始的行号和列号
        row = int(row / 128)
        row = row * 128
        col = int(col / 128)
        col = col * 128
        return row, col

    '''
    def ReadTile(self, level, row, col):
        # 读取瓦片数据
        # level 等级
        # row 总体行号
        # col 总体列号
        # returns the binary array of the image from the bundle file
        # given the path of the bundle file, and the row and column
        # of the image

        name = self.GetBundleName(level, row, col)
        bundlename = os.path.join(self.tiledir, name + '.bundle')
        bundlxname = os.path.join(self.tiledir, name + '.bundlx')

        if (os.path.exists(bundlename) == False or
            os.path.exists(bundlxname) == False):
            return None

        if bundlename not in self.bundles.keys():
            self.bundles[bundlename] = BundleClass(bundlename)
        if bundlxname not in self.bundlxs.keys():
            self.bundlxs[bundlxname] = BundlxClass(bundlxname)

        bundle_class = self.bundles[bundlename]
        bundlx_class = self.bundlxs[bundlxname]

        position = bundlx_class.GetTilePosition(row, col)
        image = bundle_class.GetTileImage(position)

        return image
    '''
    
    def WriteTile(self, level, row, col, data):
        # 写入瓦片数据
        name = self.GetBundleName(level, row, col)
        bundlename = os.path.join(self.tiledir, name + '.bundle')

        basedir = os.path.dirname(bundlename)
        if (os.path.exists(basedir) == False):
            os.makedirs(basedir)

        if bundlename not in self.bundles.keys():
            self.bundles[bundlename] = BundleClass(bundlename)

        bundle_class = self.bundles[bundlename]

        if (os.path.exists(bundlename) == False):
            # 创建新的瓦片集
            startrow, startcol = self.GetBundleRowCol(row, col)
            bundle_class.CreateNew(startrow, startcol)

        # 压缩数据        
        buf = StringIO()
        gzip.GzipFile(mode='wb', fileobj=buf).write(data)
        zdata = buf.getvalue()

        # 插入瓦片
        bundle_class.InsertData(zdata, row, col)

        
        

if __name__ == '__main__':
    #
    print '[==DoDo==]'
    print 'Bundle Maker 2.0.'
    print 'Encode: %s' %  sys.getdefaultencoding()
    
    
    inpath = './out/MAP/_alllayers/'
    outpath = './out/MAP/_alllayers_compact/'

    if os.path.exists(outpath) == False:
        os.makedirs(outpath)

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

    index = 0
    tiles = TileData(outpath)
    for fname in files:
        # ./out/MAP/_alllayers/L03/R00000000/C00000004.JPG
        fname = fname.replace('\\', '/')
        #pattern = '/L(?P<level>[0-9a-fA-F]+?)/R(?P<row>[0-9a-fA-F]+?)/C(?P<col>[0-9a-fA-F]+?)\.'
        pattern = '/(?P<level>[0-9]+?)/(?P<row>[0-9]+?)/(?P<col>[0-9]+?)\.'
        matchdata = re.search(pattern, fname, re.I)
        if matchdata == None: continue
        # 
        level = int(matchdata.group('level'))
        row = int(matchdata.group('row'), 10)
        col = int(matchdata.group('col'), 10)

        imfile = open(fname, 'rb')
        imdata = imfile.read()
        imfile.close()
        
        tiles.WriteTile(level, row, col, imdata)
        index += 1
        if (index % 128 == 0):
            print '{0}/{1} [L{2} R{3} C{4}]: {5}'.format(index, len(files), level, row, col, fname)

    print 'OK.'

    
                              

    '''
    bec = BundleClass(u'c:/Users/Administrator/Desktop/影像切片/切片测试/_alllayers/L02/R0300C0280.bundle')
    im = bec.GetTileImage(144476)
    ne = 144476 + len(im)+4
    print ne
    im = bec.GetTileImage(ne)
    f = open('aa.png', 'wb')
    f.write(im)
    f.close()
    print 'ok.'
    '''

    #tiles = TileData(u'D:/DODO/PYTHON/瓦片制作/out')
    #im = tiles.ReadTile(2, 768, 640)
    #print len(im)

    '''
    start_row = 768
    start_col = 640

    bundleHead = {
        'fixed_00_07': '0300000000400000'.decode('hex'),
        'maxsize_08_11': '00000000'.decode('hex'),
        'fixed_12_15': '05000000'.decode('hex'),
        'nonull_16_19': '00000000'.decode('hex'),
        'unknow_20_23': '00000000'.decode('hex'),
        'filesize_24_27': struct.pack('i', 60 + 4*128*128),
        'unknow_28_31': '00000000'.decode('hex'),
        'fixed_32_43': '280000000000000010000000'.decode('hex'),
        'startrow_44_47': struct.pack('i', start_row),
        'endrow_48_51': struct.pack('i', start_row + 128),
        'startcol_52_55': struct.pack('i', start_col),
        'endcol_56_59': struct.pack('i', start_col + 128),
        'nullindex_60_65596': ''.zfill(4*128*128).decode('hex')
    }

    bec = BundleClass('aa.bundle')
    bec.CreateNew(start_row, start_col)
    print 'ok'
    '''

    '''
    tiles = TileData(u'C:/Users/Administrator/Desktop/影像切片/切片测试/_alllayers')
    tileswrite = TileData(u'C:/Users/Administrator/Desktop/影像切片/切片测试/_alllayers/OUT')
    for row in range(768, 768+128):
        for col in range(640, 640+128):
            im = tiles.ReadTile(2, row, col)
            if (im == None): continue
            size = len(im)
            if (size > 0):
                print row, col, size
                tileswrite.WriteTile(2, row, col, im)
                
                #fname = './out/data/%s-%s.png' % (row, col)
                #print fname
                #f = open(fname, 'wb')
                #f.write(im)
                #f.close()
    print 'OK.'
    '''
    
    '''
    tiles = TileData(u'D:/DODO/PYTHON/瓦片制作/out')
    f = open('./out/files.txt', 'r')
    for line in f:
        line = line.replace('\n', '')
        imfile = open(line, 'rb')
        imdata = imfile.read()
        imfile.close()
        

        row = int(line[11:16])
        col = int(line[17:22])
        print line, row, col
        tiles.WriteTile(6, row, col, imdata)
    '''
    


        
