#!/usr/bin/env python
# encoding: utf-8

import os, sys, math, json, struct, re

# 火星瓦片转WGS瓦片
# WGS瓦片转火星瓦片

# > 火星瓦片 marsTileX,marsTileY -> 火星像素 marsPixX,marsPixY
# > 火星像素 marsPixX,marsPixY   -> 

# 1. 设置512张图片的缓冲区
# 2. 火星坐标转GPS坐标
# 3. 建立两者之间的联系

class MarsTiles:
    # 火星瓦片
    pass


if __name__ == '__main__':
    #
    print '[==DoDo==]'
    print 'Bundle Maker.'
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

        index += 1
        if (index % 128 == 0):
            print '{0}/{1} [L{2} R{3} C{4}]: {5}'.format(index, len(files), level, row, col, fname)









        
