#!/usr/bin/env python
# encoding: utf-8

import os, sys, math, json

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
        if (row, col) in tile_pos_dic.keys():
            # 如果字典里有偏移量就直接返回
            return tile_pos_dic[(row, col)]
        # 打开索引文件读取偏移量
        position = self.GetIndexPostion(row, col)
        fbundlx = open(self.fname, 'rb')
        fbundlx.seek(position)
        value = fbundlx.read(5)
        fbundlx.close()
        # 保存字典
        result = self.HexToInt(value)
        tile_pos_dic[(row, col)] = result
        
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
        result = (value[4] & 0xFF) << 32 |
                 (value[3] & 0xFF) << 24 |
                 (value[2] & 0xFF) << 16 |
                 (value[1] & 0xFF) << 8 |
                 (value[0] & 0xFF)
                 
        return int(result)
        
        
class TileData:
    # 瓦片数据处理类
    def __init__(self, tiledir):
        
        pass

    def GetBundleName(self, level, row, col):
        # 通过等级 行号 列号获取集合名字
        # row 总体行号
        # col 总体列号
        bundlename = ''

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
        
        return dirname + '/' + filename

    
        
    def GetTile(self, level, row, col):
        # 获取瓦片图形
        # returns the binary array of the image from the bundle file
        # given the path of the bundle file, and the row and column
        # of the image
        
        
        
        pass

if __name__ == '__main__':
    #
    print '[==DoDo==]'
    print 'Bundle Maker.'
    print 'Encode: %s' %  sys.getdefaultencoding()

    
