# -*- coding: utf-8 -*-：
import requests
from bs4 import BeautifulSoup as bs
from general import *
from database import *
import re
import time
import traceback

DOMAIN_NAME = '1point3acres.com'
PROJECT_ID = 197  # The project name represents each section of the forum

create_project_dir(DOMAIN_NAME + '/' + str(PROJECT_ID))

conn = connect_to_db()

# Create a table for each forum
table_name = 'forum_' + str(PROJECT_ID)
cursor = conn.cursor()
create_table(cursor, table_name)
cursor.execute('use 1point3acres')

# TODO 实现登陆
# TODO 建一个ip池，防止封ip
# write_file(DOMAIN_NAME + '/' + str(PROJECT_ID) + '/exceptions.log', '')
# TODO 自动获取总页数
for page in range(1, 21):
    res = requests.get('http://www.1point3acres.com/bbs/forum-' + str(PROJECT_ID) + '-' + str(page) + '.html')
    soup = bs(res.text, "lxml")
    for post in soup.findAll(name='th', attrs={'class': 'new'}):
        try:
            # Followings are the data to be crawled
            url = ''
            post_name = ''
            author_url = ''
            author_name = ''
            reply = ''
            pv = ''
            date_time = ''
            content = ''

            # Filter apply to 1point3acres.com
            a = post.findAll(name='a', attrs={'onclick': 'atarget(this)'})
            if a:
                url = a[0]['href']
                post_name = a[0].text

            b = post.next_sibling.next_sibling.cite
            if b:
                author = b.a
                author_url = author['href']
                author_name = author.text

            c = post.next_sibling.next_sibling.next_sibling.next_sibling
            if c:
                reply = c.a.text
                pv = c.em.text

            soup_post = bs(requests.get(url).text, 'lxml')
            d = soup_post.select('.authi', limit=1)
            if d:
                tmp = d[0].em
                tmp_list = tmp.select('span')
                if not tmp_list:
                    date_time = re.match(re.compile(r'(.*?) (.*)'), tmp.text).group(2)
                else:
                    date_time = tmp_list[0]['title']

            e = soup_post.select('.t_f', limit=1)
            if e:
                # extract text content of the main post
                [s.extract() for s in e[0](['i', 'span'])]
                [s.extract() for s in e[0].select('.jammer')]
                [s.extract() for s in e[0].select('.a_pr')]
                [s.extract() for s in e[0].select('.attach_nopermission')]
                [s.extract() for s in e[0].select('ignore_js_op')]
                content = e[0].text.strip()

            # Input data into date base
            data_tuple = (url.encode('utf-8'),
                          post_name.encode('utf-8'),
                          author_url.encode('utf-8'),
                          author_name.encode('utf-8'),
                          int(reply),
                          int(pv),
                          date_time.encode('utf-8'),
                          content.encode('utf-8'))
            insert_into_db(cursor, table_name, data_tuple)

            print 'Crawled ' + url

        except Exception, error:
            if error.args[0] == 1062 and type(error) == MySQLdb.IntegrityError:
                print '1062, duplicated primary key | ' + url
            else:
                print post_name, url
                traceback.print_exc()
                append_to_file(DOMAIN_NAME + '/' + str(PROJECT_ID) + '/exceptions.log',
                               url.encode('utf-8') + '\n' + post_name.encode('utf-8') +
                               '\n' + traceback.format_exc() + '\n')
            if type(error) == requests.exceptions.ConnectionError:
                conn.commit()
                time.sleep(5)
                continue
    print 'page ' + str(page) + ' complete'
    time.sleep(3)
    conn.commit()

cursor.close()
conn.close()
