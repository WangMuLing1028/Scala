'''''
下载豆瓣首页的图片

采用伪装浏览器的方式爬取豆瓣网站首页的图片，保存到指定的文件夹下
'''''
import os

targetPath = "G:\PythonOutTest"

def saveFile(path):
    if not os.path.isdir(targetPath):
        os.mkdir(targetPath)

        pos = path.rindex('/')
        t = os