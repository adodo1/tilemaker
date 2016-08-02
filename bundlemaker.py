#!/usr/bin/env python
# encoding: utf-8

import os, sys, math, json, struct

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

# bundle文件
# 前60字节
# 00-07: 固定 0300000000400000
# 08-11: 最大的一块瓦片大小
# 12-15: 固定 05000000
# 16-19: 非空瓦片数量 * 4
# 20-23: 未知 00000000
# 24-27: 文件大小
# 28-31: 未知 00000000
# 32-43: 固定 280000000000000010000000
# 44-47: 开始行
# 48-51: 结束行
# 52-55: 开始列
# 56-59: 结束列
# 中间伪索引 全0
# 4字节 * 128行 * 128列
# 4字节图片大小
# 图片数据
# 4字节图片大小
# 图片数据
# 4字节图片大小
# 图片数据
# ...

# bundlx文件
# 前16字节 03000000100000000040000005000000
# 中间每个瓦片对应偏移量 5字节 * 128行 * 128列
# 后16字节 00000000100000001000000000000000

# 1. 通过level row col 计算出bundle文件名
# 2. 

PACKET_SIZE = 128       # 包大小

class BundleClass:
    # 存储瓦片的文件类
    def __init__(self, fname):
        self.fname = fname
        pass

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

    def CreateNew(self, start_row, start_col):
        # 创建新的瓦片存储文件
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
            'nullindex_60_65596': ''.zfill(4*128*128*2).decode('hex')
            }
        #
        fbundle = open(self.fname, 'wb')
        fbundle.write(bundleHead['fixed_00_07'])
        fbundle.write(bundleHead['maxsize_08_11'])
        fbundle.write(bundleHead['fixed_12_15'])
        fbundle.write(bundleHead['nonull_16_19'])
        fbundle.write(bundleHead['unknow_20_23'])
        fbundle.write(bundleHead['filesize_24_27'])
        fbundle.write(bundleHead['unknow_28_31'])
        fbundle.write(bundleHead['fixed_32_43'])
        fbundle.write(bundleHead['startrow_44_47'])
        fbundle.write(bundleHead['endrow_48_51'])
        fbundle.write(bundleHead['startcol_52_55'])
        fbundle.write(bundleHead['endcol_56_59'])
        fbundle.write(bundleHead['nullindex_60_65596'])
        fbundle.close()


    def InsertData(self, image):
        # 插入图片 并返回插入位置
        size = len(image)
        file_size = os.path.getsize(self.fname) + size + 4
                
        fbundle = open(self.fname, 'rb+')
        # 处理最大数据大小
        fbundle.seek(8)
        maxsize = struct.unpack('i', fbundle.read(4))[0]
        if (maxsize < size): maxsize = size
        fbundle.seek(8)
        fbundle.write(struct.pack('i', maxsize))
        # 处理非空数量
        fbundle.seek(16)
        nonullcount = struct.unpack('i', fbundle.read(4))[0]
        nonullcount = ((nonullcount / 4) + 1) * 4
        fbundle.seek(16)
        fbundle.write(struct.pack('i', nonullcount))
        # 处理文件大小
        fbundle.seek(24)
        fbundle.write(struct.pack('i', file_size))

        # 写入图片数据
        fbundle.seek(0, 2)
        position = fbundle.tell()
        fbundle.write(struct.pack('i', file_size))
        fbundle.write(image)

        fbundle.close()
        
        return position
        

class BundlxClass:
    # 存储索引类
    def __init__(self, fname):
        self.fname = fname
        self.tile_pos_dic = {}
        pass

    def GetTilePosition(self, row, col):
        # 计算row行col列图像的偏移量
        # row 总体行号
        # col 总体列号
        # reads from the index file and returns the position of the
        # image in the bundle file, given the path of the index file
        # and the row and column fo the image
        if (row, col) in self.tile_pos_dic.keys():
            # 如果字典里有偏移量就直接返回
            return self.tile_pos_dic[(row, col)]
        # 打开索引文件读取偏移量
        position = self.GetIndexPostion(row, col)
        fbundlx = open(self.fname, 'rb')
        fbundlx.seek(position)
        value = fbundlx.read(5)
        fbundlx.close()
        # 保存字典
        result = self.HexToInt(value)
        self.tile_pos_dic[(row, col)] = result
        
        return result

    def GetIndexPostion(self, row, col):
        # 计算row行col列在索引文件bundlx的偏移量
        # row 总体行号
        # col 总体列号
        # given a row and column, returns the position in the index
        # file where you can find the position of the actual image
        # in the bundle file
        row = row % 128
        col = col % 128
        # 
        base_pos = 16 + col * 5 * 128
        offset = row * 5
        # 
        position = base_pos + offset
        return position

    def HexToInt(self, value):
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

    def IntToHex(self, value):
        # 整形转5字节
        result = struct.pack('q', value)[0:5]
        return result

    def CreateNew(self):
        # 创建新的索引文件
        bundlxData = '03000000100000000040000005000000'.decode('hex')
        for n in range(0, 128*128):
            offset = 60 + n * 4
            offset = self.IntToHex(offset)
            bundlxData += offset
        bundlxData += '00000000100000001000000000000000'.decode('hex')
        fbundlx = open(self.fname, 'wb')
        fbundlx.write(bundlxData)
        fbundlx.close()


    def InsertData(self, row, col, offset):
        # 写入索引
        position = self.GetIndexPostion(row, col)
        fbundlx = open(self.fname, 'rb+')
        fbundlx.seek(position)
        value = self.IntToHex(offset)
        fbundlx.write(value)
        fbundlx.close()
        pass
        
class TileData:
    # 瓦片数据处理类
    def __init__(self, tiledir):
        # 初始化瓦片字典和索引字典
        self.bundles = {}
        self.bundlxs = {}
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

    def WriteTile(self, level, row, col, data):
        # 写入瓦片数据
        name = self.GetBundleName(level, row, col)
        bundlename = os.path.join(self.tiledir, name + '.bundle')
        bundlxname = os.path.join(self.tiledir, name + '.bundlx')

        basedir = os.path.dirname(bundlename)
        if (os.path.exists(basedir) == False):
            os.makedirs(basedir)

        if bundlename not in self.bundles.keys():
            self.bundles[bundlename] = BundleClass(bundlename)
        if bundlxname not in self.bundlxs.keys():
            self.bundlxs[bundlxname] = BundlxClass(bundlxname)

        bundle_class = self.bundles[bundlename]
        bundlx_class = self.bundlxs[bundlxname]

        if (os.path.exists(bundlename) == False):
            # 创建新的瓦片集
            startrow, startcol = self.GetBundleRowCol(row, col)
            bundle_class.CreateNew(startrow, startcol)
            
        if (os.path.exists(bundlxname) == False):
            # 创建新的索引
            bundlx_class.CreateNew()

        # 先写瓦片
        offset = bundle_class.InsertData(data)
        # 修改索引
        bundlx_class.InsertData(row, col, offset)            
        

if __name__ == '__main__':
    #
    print '[==DoDo==]'
    print 'Bundle Maker.'
    print 'Encode: %s' %  sys.getdefaultencoding()
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
    tiles = TileData(u'D:/DODO/PYTHON/瓦片制作/out')
    for row in range(13440, 13440+128):
        for col in range(10368, 10368+128):
            im = tiles.ReadTile(6, row, col)
            if (im == None): continue
            size = len(im)
            if (size > 0):
                #print row, col, size
                fname = './out/data/%s-%s.png' % (row, col)
                print fname
                f = open(fname, 'wb')
                f.write(im)
                f.close()
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
    


        
