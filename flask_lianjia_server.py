from flask import Flask,jsonify
from flask import render_template
from json import dumps
import math

app = Flask(__name__)

from sqlalchemy import create_engine
Engine = create_engine("postgresql://shaw:123456@10.1.10.220:5432/shawdb")

def format_gps_price(data):
    point = dict(zip(['lng','lat'], data[0].split(',')))
    # point.update({'count':math.log(float(data[1])) })
    point.update({'count':(float(data[1])) })
    return point

def get_shenzhen_lianjia_data():
    connection = Engine.connect()
    sql = '''
        select gps,"quantity_total"
    from lianjia.xiaoqu_price_trend t1
    join lianjia.xiaoqu_position t2 on t1.xiaoqu_id = t2.xiaoqu_id
    where month = '7月' and t1."quantity_total" is not null
    '''
    result = connection.execute(sql).fetchall()
    points = map(format_gps_price, result)
    return list(points)

@app.route('/')
def hello(name=None):
    data = get_shenzhen_lianjia_data()
    return render_template('shenzhen_lianjia.html', data=data)

if __name__ == '__main__':
    app.run(debug=True)

