# 抓取B站视频存放在数据库中
# 爬虫部分
"""
爬虫思路：首先访问https://space.bilibili.com/ajax/member/getSubmitVideos?mid=23172676&pagesize=50&page=1&order=pubdate
来获取选取的up主近期50个视频的aid，url中的mid参数是up主UID（这里直接给定，我选取的是荟小荟），pagesize是指定一次接收多少个视频，page是当前
页码。最后一个参数表示按照时间排序，最新发表的排在第一位。然后再将aid于另外的url组合访问视频的具体信息
"""

import requests
from fake_useragent import UserAgent
import MySQLdb
from datetime import datetime
import os
import traceback


def save_pic_to_file(uid, video_list):
    if not os.path.exists('./pictures/up%s' % uid):
        os.makedirs('./pictures/up%s' % uid)
    for video in video_list:
        with open('./pictures/up%s/%s.jpg' % (uid, video['aid']), 'wb+') as f:
            try:
                response = requests.get(video['picture_path'], headers={'User-Agent': UserAgent().random})
                response.raise_for_status()
                f.write(response.content)
            except:
                traceback.print_exc()


def duration_to_timestr(duration):
    hours = duration // 3600
    minutes = (duration % 3600) // 60
    seconds = duration % 60
    return "%d:%d:%d" % (hours, minutes, seconds)


def get_aid(uid):
    up_url = "https://space.bilibili.com/ajax/member/getSubmitVideos?mid=" + str(uid) + "&pagesize=50&page=1&order=pubdate"
    try:
        headers = {'User-Agent': UserAgent().random}
        response = requests.get(up_url, headers=headers)
        response.raise_for_status()
        res_dict = response.json()
        # 获取具有简略视频信息的视频列表
        video_list = res_dict['data']['vlist']
        aid_list = []
        # 遍历列表，仅仅保留每个视频（一个dict）的aid值
        for video in video_list:
            aid_list.append(video['aid'])
        return aid_list
    except:
        traceback.print_exc()


def get_video_info(aid, video_list):
    raw_url = "https://api.bilibili.com/x/web-interface/view"
    headers = {'User-Agent': UserAgent().random}
    params = {'aid': aid}
    try:
        response = requests.get(raw_url, headers=headers, params=params)
        data_dict = response.json()['data']
        video_dict = {
            'aid': data_dict['aid'],
            'picture_path': data_dict['pic'],
            'title': data_dict['title'],
            'subtitle': data_dict['subtitle']['list'][0] if data_dict['subtitle']['list'] else None,
            'publish_date': datetime.fromtimestamp(data_dict['pubdate']).strftime("%Y-%m-%d %H:%M:%S"),
            'description': data_dict['desc'],
            'duration': duration_to_timestr(data_dict['duration']),
            'num_of_coins': data_dict['stat']['coin'],
            'num_of_likes': data_dict['stat']['like'],
            'num_of_favorites': data_dict['stat']['favorite'],
            'num_of_replies': data_dict['stat']['reply'],
            'num_of_shares': data_dict['stat']['share']
        }
        # 附加到视频详细信息列表
        video_list.append(video_dict)
    except:
        traceback.print_exc()


def save_to_database(uid, video_list):
    # 打开数据库连接
    video_database = MySQLdb.connect('localhost', 'root', '1314', 'Bilibili', charset='utf8')
    # 使用cursor()方法获取操作游标
    cursor = video_database.cursor()
    # 如果数据表已经存在使用 execute() 方法删除表。
    cursor.execute("DROP TABLE IF EXISTS table_%s" % uid)

    # 以up主的uid命名数据库表
    createTable = """
     CREATE TABLE table_%s (
     id BIGINT NOT NULL AUTO_INCREMENT,
     aid BIGINT NOT NULL,
     title VARCHAR(100) NOT NULL,
     subtitle VARCHAR(100) NOT NULL,
     publish_date DATETIME NOT NULL,
     description VARCHAR(1000) NOT NULL,
     duration TIME NOT NULL,
     num_of_coins BIGINT NOT NULL,
     num_of_likes BIGINT NOT NULL,
     num_of_favorites BIGINT NOT NULL,
     num_of_replies BIGINT NOT NULL,
     num_of_shares BIGINT NOT NULL,
     PRIMARY KEY (id));""" % uid
    cursor.execute(createTable)
    print("写入中...")
    for video in video_list:
        try:
            insert = """
            INSERT INTO table_{11} (
            aid,
            title,
            subtitle,
            publish_date,
            description,
            duration,
            num_of_coins,
            num_of_likes,
            num_of_favorites,
            num_of_replies,
            num_of_shares
            ) VALUES 
            (
            {0},
            '{1}',
            '{2}',
            '{3}',
            '{4}',
            '{5}',
            {6},
            {7},
            {8},
            {9},
            {10}
            )
            """.format(video['aid'],
                       video['title'],
                       video['subtitle'],
                       video['publish_date'],
                       video['description'],
                       video['duration'],
                       video['num_of_coins'],
                       video['num_of_likes'],
                       video['num_of_favorites'],
                       video['num_of_replies'],
                       video['num_of_shares'],
                       uid)
            cursor.execute(insert)
            video_database.commit()
        except:
            video_database.rollback()
    cursor.close()
    video_database.close()


if __name__ == "__main__":
    uid_list = []
    up_name_dict = {}

    # 输入想要爬取的up主数量
    num_of_ups = input("请输入想要怕爬取的up主的数量：")

    # 依次输入up主的UID
    print("请依次输入up主的UID：")
    for i in range(int(num_of_ups)):
        temp_uid = input()
        uid_list.append(temp_uid)
    print("Start!!!")
    for up_uid in uid_list:
        print("爬取中...")
        try:
            r = requests.get("https://api.bilibili.com/x/space/acc/info?mid=" + up_uid + "&jsonp=jsonp", headers={'User-Agent': UserAgent().random})
            r.raise_for_status()
            r.encoding = r.apparent_encoding
            up_name_dict[up_uid] = r.json()['data']['name']
        except:
            traceback.print_exc()

        aids = get_aid(up_uid)
        vlist = []
        for single_aid in aids:
            get_video_info(single_aid, vlist)
        
        # 测试语句
        print(vlist)
        print(len(vlist))
        
        # 封面图片存入文件
        save_pic_to_file(up_uid, vlist)

        # 出去图片外的数据存入数据库
        save_to_database(up_uid, vlist)

    video_database = MySQLdb.connect('localhost', 'root', '1314', 'Bilibili', charset='utf8')
    cursor = video_database.cursor()
    cursor.execute("DROP TABLE IF EXISTS upname")
    cursor.execute("""CREATE TABLE upname (
    id BIGINT NOT NULL AUTO_INCREMENT,
    uid BIGINT NOT NULL,
    name VARCHAR(30) NOT NULL,
    PRIMARY KEY (id));""")
    for k, v in up_name_dict.items():
        try:
            cursor.execute("INSERT INTO upname (uid, name) VALUES (%s, '%s')" % (k, v))
            video_database.commit()
        except:
            video_database.rollback()
            traceback.print_exc()
    cursor.close()
    video_database.close()
