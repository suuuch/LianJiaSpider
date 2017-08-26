# coding = utf-8

import requests, re
import random
from time import sleep
import pandas as pd


class LianJiaSpider(object):
    def fetch_url(self, url):
        # Some User Agents
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
        sleep(0.5)
        print('Start Fetch Url : %s' % url)
        return requests.get(url, headers=random.choice(hds))


class XiaoQuSpider(LianJiaSpider):
    def __init__(self):
        self.__url_sz_lianjia = 'https://sz.lianjia.com/'
        self.__url_lianjia_xiaoqu = self.__url_sz_lianjia + 'xiaoqu/'
        self.__list_sz_xiaoqu = ['luohuqu', 'futianqu', 'nanshanqu', 'yantianqu', 'baoanqu', 'longgangqu', 'longhuaqu',
                                 'guangmingxinqu',
                                 'pingshanqu', 'dapengxinqu']
        self.__url_price_trend = 'https://sz.lianjia.com/fangjia/priceTrend/c%s'
        super(XiaoQuSpider, self).__init__()

    def get_xiaoqu_id(self):
        all_ids = []
        for qu in self.__list_sz_xiaoqu:
            url = self.__url_lianjia_xiaoqu + qu + '/'
            rst = self.fetch_url(url)
            all_ids.extend(self.spilt_xiaoqu_id(rst.text))
            total_page = self.get_total_pg(rst.text)
            for pg in range(2, int(total_page) + 1):
                rst = self.fetch_url(url + 'pg%d/' % pg)
                all_ids.extend(self.spilt_xiaoqu_id(rst.text))
                break
        return all_ids

    def spilt_xiaoqu_id(self, content):
        pattern_id = re.compile(r'xiaoqu/(\d+)/')
        all_ids = set(pattern_id.findall(content))
        return all_ids

    def get_xiaoqu_position(self, xiaoqu_id):
        pattern_position = re.compile(r'resblock\w+:\'(.*?)\'')
        url_xiaoqu_detail = self.__url_lianjia_xiaoqu + xiaoqu_id + '/'
        content = self.fetch_url(url_xiaoqu_detail).text
        xiaoqu_detail = pd.DataFrame(pattern_position.findall(content) + [xiaoqu_id]).T
        return xiaoqu_detail.rename(columns={0: 'gps', 1: 'name', 2: 'XiaoQuId'})

    def get_total_pg(self, content):
        pattern_pg = re.compile(r'totalPage":(\d+),')
        total_pg = pattern_pg.findall(content)[0]
        return total_pg

    def get_price_trend(self, xiaoqu_id):
        url = self.__url_price_trend % xiaoqu_id
        rst = self.fetch_url(url).json()
        return self.format_json2df(rst)

    def format_json2df(self, rst):
        this_list = []
        for k in ['currentLevel', 'upLevel']:
            for k2 in ['dealPrice', 'quantity', 'listPrice', 'month']:
                df = pd.DataFrame(rst[k][k2])
                colums_name = dict(zip(df.columns, list(map(lambda x: k2 + '_' + str(x) if x else k2, df.columns))))
                this_list.append(df.rename(columns=colums_name))
        return pd.concat(this_list, axis=1)


if __name__ == '__main__':
    xq = XiaoQuSpider()
    ids = xq.get_xiaoqu_id()
    pt = xq.get_xiaoqu_position('2411048850665')
    pt = xq.get_price_trend('2411049441058')

    print(ids)
