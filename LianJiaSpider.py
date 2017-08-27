# coding = utf-8

import requests, re
import random
from time import sleep
import pandas as pd
from sqlalchemy import create_engine

Engine = create_engine("postgresql://shaw:123456@10.1.10.220:5432/shawdb")


class LianJiaSpider(object):
    def fetch_url(self, url):
        # Some User Agents
        cookies = dict(
                Cookie='all-lj=406fadba61ceb7b8160b979dadec8dfa; lianjia_uuid=fd5cdb76-4635-4120-bc1c-cecc64b5a7bc; UM_distinctid=15e1d83f34b77e-075dfa3f7153b8-31627c01-232800-15e1d83f34c573; _jzqckmp=1; lianjia_token=2.005c5dfad325c55a8d4df0d3e2bc4aef97; select_city=440300; _jzqx=1.1503733741.1503745019.2.jzqsr=google%2Ecom|jzqct=/.jzqsr=sz%2Elianjia%2Ecom|jzqct=/xiaoqu/luohuqu/; _smt_uid=59a127ec.1dff26b4; CNZZDATA1255849469=642219317-1503733166-https%253A%252F%252Fwww.google.com%252F%7C1503754796; Hm_lvt_9152f8221cb6243a53c83b956842be8a=1503733741; Hm_lpvt_9152f8221cb6243a53c83b956842be8a=1503756566; CNZZDATA1254525948=934176206-1503729727-https%253A%252F%252Fwww.google.com%252F%7C1503751703; CNZZDATA1255633284=224208425-1503730164-https%253A%252F%252Fwww.google.com%252F%7C1503751906; CNZZDATA1255604082=1017642929-1503729491-https%253A%252F%252Fwww.google.com%252F%7C1503751915; _qzja=1.1130530474.1503733741276.1503745019418.1503756536779.1503756560877.1503756566546.0.0.0.145.3; _qzjb=1.1503756536778.8.0.0.0; _qzjc=1; _qzjto=145.3.0; _jzqa=1.1562268486751971800.1503733741.1503745019.1503756537.3; _jzqc=1; _jzqb=1.8.10.1503756537.1; _ga=GA1.2.717341839.1503733743; _gid=GA1.2.1989542136.1503733743; lianjia_ssid=a635bb9c-9cd2-4e2e-a4e5-97f0d8d3e8bb')
        hds = [
            {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}, \
            {
                'User-Agent': 'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'}, \
            {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'}, \
            {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'}, \
            {
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'}, \
            {
                'User-Agent': 'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'}, \
            {
                'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'}, \
            {'User-Agent': 'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'}, \
            {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'}, \
            {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'}, \
            {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'}, \
            {'User-Agent': 'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'}, \
            {'User-Agent': 'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}]
        sleep(1)
        print('Start Fetch Url : %s' % url)
        return requests.get(url, headers=random.choice(hds), cookies=cookies)
        

class XiaoQuSpider(LianJiaSpider):
    def __init__(self):
        self.__url_sz_lianjia = 'https://sz.lianjia.com/'
        self.__url_lianjia_xiaoqu = self.__url_sz_lianjia + 'xiaoqu/'
        self.__list_sz_xiaoqu = [#'luohuqu',
                                 #'futianqu',
                                 #'nanshanqu',
                                 #'yantianqu',
                                 #'baoanqu',
                                 'longgangqu',
                                 'longhuaqu',
                                 'guangmingxinqu',
                                 'pingshanqu',
                                 'dapengxinqu']
        self.__url_price_trend = self.__url_sz_lianjia + 'fangjia/priceTrend/c%s'
        super(XiaoQuSpider, self).__init__()

    def get_xiaoqu_id(self):

        for qu in self.__list_sz_xiaoqu:
            url = self.__url_lianjia_xiaoqu + qu + '/'
            rst = self.fetch_url(url)

            yield {'xiaoqu_id': self.spilt_xiaoqu_id(rst.text), 'qu': qu}
            total_page = self.get_total_pg(rst.text)
            for pg in range(2, int(total_page) + 1):
                rst = self.fetch_url(url + 'pg%d/' % pg)
                yield {'xiaoqu_id': self.spilt_xiaoqu_id(rst.text), 'qu': qu}

    def spilt_xiaoqu_id(self, content):
        pattern_id = re.compile(r'xiaoqu/(\d+)/')
        all_ids = set(pattern_id.findall(content))
        return list(all_ids)

    def get_xiaoqu_position(self, xiaoqu_id):
        pattern_position = re.compile(r'resblock\w+:\'(.*?)\'')
        url_xiaoqu_detail = self.__url_lianjia_xiaoqu + xiaoqu_id + '/'
        content = self.fetch_url(url_xiaoqu_detail).text
        xiaoqu_detail = pd.DataFrame(pattern_position.findall(content) + [xiaoqu_id]).T
        return xiaoqu_detail.rename(columns={0: 'gps', 1: 'name', 2: 'xiaoqu_id'})

    def get_total_pg(self, content):
        pattern_pg = re.compile(r'totalPage":(\d+),')
        total_pg = pattern_pg.findall(content)[0]
        return total_pg

    def get_price_trend(self, xiaoqu_id):
        url = self.__url_price_trend % xiaoqu_id
        rst = self.fetch_url(url).json()
        data = self.format_json2df(rst)
        data['dt'] = '_'.join(rst['time'].values())
        data['xiaoqu_id'] = xiaoqu_id
        return data

    def format_json2df(self, rst):
        this_list = []
        for k in ['currentLevel', 'upLevel']:
            for k2 in ['dealPrice', 'quantity', 'listPrice', 'month']:
                df = pd.DataFrame(rst[k][k2])
                colums_name = dict(zip(df.columns, list(map(lambda x: k2 + '_' + str(x) if x else k2, df.columns))))
                this_list.append(df.rename(columns=colums_name))
        return pd.concat(this_list, axis=1)


def clean_data():
    connection = Engine.connect()
    sql = '''
     delete from lianjia.xiaoqu_price_trend t
      where exists ( select 1
                     from lianjia.xiaoqu_price_trend i
                     where i.xiaoqu_id = t.xiaoqu_id
                       and i.month = t.month
                        and i.ctid < t.ctid
                        );

    '''
    result = connection.execute(sql)
    sql = '''
     delete from lianjia.xiaoqu_position t
      where exists ( select 1
                     from lianjia.xiaoqu_position i
                     where i.xiaoqu_id = t.xiaoqu_id
                        and i.ctid < t.ctid
                        );


    '''
    result = connection.execute(sql)


if __name__ == '__main__':
    xq = XiaoQuSpider()


    def insert_all_id_to_db():
        ids = xq.get_xiaoqu_id()
        for id in ids:
            df = pd.DataFrame(id)
            df.to_sql('xiaoqu_ids', Engine, schema='lianjia', index=False, if_exists='append')


    def insert_data_to_db(xiaoqu_id):
        xq.get_xiaoqu_position(xiaoqu_id).to_sql('xiaoqu_position', Engine, schema='lianjia', index=False,
                                                 if_exists='append')
        xq.get_price_trend(xiaoqu_id).to_sql('xiaoqu_price_trend', Engine, schema='lianjia', index=False,
                                             if_exists='append')


    connection = Engine.connect()
    result = connection.execute('''select distinct  xiaoqu_id from lianjia.xiaoqu_ids t1
                                  where not exists (select 1 from lianjia.xiaoqu_position t2
                                  where t1.xiaoqu_id = t2.xiaoqu_id )''').fetchall()
    # insert_all_id_to_db()

    rst = map(lambda x: insert_data_to_db(x[0]), result)
    print(list(rst))

    clean_data()
