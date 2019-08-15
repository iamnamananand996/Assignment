from flask import Flask, render_template, request, jsonify
from flaskext.mysql import MySQL

import json

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from firebase_admin import auth

cred = credentials.Certificate('key.json')
firebase_admin.initialize_app(cred)
db = firestore.client()


app = Flask(__name__)

app.config['SECRET_KEY'] = 'ec830e5ae057c5b08f5a435a7b13e891'

# Config MySQL
app.config['MYSQL_DATABASE_HOST'] = "localhost"
#app.config['MYSQL_DATABASE_PORT'] = 3306
app.config['MYSQL_DATABASE_USER'] = 'root'
app.config['MYSQL_DATABASE_PASSWORD'] = ''
app.config['MYSQL_DATABASE_DB'] = 'main_app'
# app.config['MYSQL_CURSORCLASS'] = 'DictCursor'
# init MYSQL
mysql = MySQL()
mysql.init_app(app)

@app.route('/')
def hello_world():
    with open('data.json') as f:
        data = json.loads(f.read())
    res = []
    for x in data['data']['campaign'].keys():   
        for i in data['data']['campaign']['146']['url'].keys():
            for j in data['data']['campaign']['146']['url'][i]['backlink_data']['follow']:
                for k in data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]:
                    res.append([data['data']['owner_id'],i,j,data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['pa'],data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['da']])
                    # print(data['data']['owner_id'],i,j,data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['pa'],data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['da'])
    print(res)
    return render_template('index.html',res=res,count=len(res))

@app.route('/update')
def register():

    with open('data.json') as f:
        data = json.loads(f.read())
        # print(data)

    conn=mysql.connect()
    cursor = conn.cursor()

    if(cursor.execute("INSERT INTO data(json_data) VALUES(%s)",(str(data)))):
        print('inserted')

    if(cursor.execute("INSERT INTO owner(owner_id) VALUES(%s)",(data['data']['owner_id']))):
        print("data gone")
    for x in data['data']['campaign'].keys():
        if(cursor.execute("INSERT INTO campaign(campaign_id,campaign_name) VALUES(%s,%s)",(x,data['data']['campaign'][x]['campaigne_name']))):
            print('data inserted')
    for x in data['data']['campaign'].keys():   
        for i in data['data']['campaign']['146']['url'].keys():
            for j in data['data']['campaign']['146']['url'][i]['backlink_data']['follow']:
                for k in data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]:
                    # print(i,j,data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['pa'],data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['da'])
                    
                    # count = count + 1
                    if(cursor.execute("INSERT INTO urls(owner_id,campaign_id,url_key,url_name,PA,DA) VALUES(%s,%s,%s,%s,%s,%s)",(data['data']['owner_id'],x,i,j,data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['pa'],data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['da']))):
                        print('data inserted')
        
    conn.commit()
    cursor.close()

    return 'data inserted'

@app.route('/data/<owner_id>/<campaign_id>',methods=['GET','POST'])
def data(owner_id,campaign_id):
    # if request.method == 'GET':
    #     x = request.get_json()
    #     print(x)
    conn=mysql.connect()
    cursor = conn.cursor()
    temp_dic = {}
    temp_data = []
    query_string = "SELECT * FROM `urls` WHERE `owner_id` = %s and `campaign_id`=146 and `url_key` = %s"
    
    result_num = cursor.execute(query_string, (owner_id,campaign_id))
    if result_num > 0:
        for value in cursor.fetchall():
            temp_dic = {"url_name":value[3],"pa":value[4], "da":value[5]}
            temp_data.append(temp_dic)
            temp_dic = {}
            # value[3],value[4],value[5])


    response = {
                    "owner_id": owner_id,
                    "camapaign": "value if campaign name",
                    "campaign_id": {campaign_id: temp_data}
                }

    return jsonify(response), 200


@app.route('/update_firebase')
def update_firebase():

    with open('data.json') as f:
        data = json.loads(f.read())
    count = 0
    for x in data['data']['campaign'].keys():   
        for i in data['data']['campaign']['146']['url'].keys():
            for j in data['data']['campaign']['146']['url'][i]['backlink_data']['follow']:
                for k in data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]:
                    # print(i,j,data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['pa'],data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['da'])
                    

                    doc_ref = db.collection(u'url').document(u'owner_id').collection(u''+data['data']['owner_id']).document(u'url_key').collection(u''+i).document()
                    doc_ref.set({
                        u'url': j,
                        u'PA': data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['pa'],
                        u'DA' : data['data']['campaign']['146']['url'][i]['backlink_data']['follow'][j]['da']
                    })
                    count = count + 1
                    print('data',count)

    return "data inserted"


@app.route('/data_firebase/<owner_id>/<campaign_id>',methods=['GET','POST'])
def data_firebase(owner_id,campaign_id):
    temp_dic = {}
    temp_data = []
    doc_ref = db.collection(u'url').document(u'owner_id').collection(u''+owner_id).document(u'url_key').collection(u''+campaign_id)
    
    doc_ref = doc_ref.get()
    temp_data = []
    for doc in doc_ref:
        temp_data.append(doc.to_dict())
    response = {
                    "owner_id": owner_id,
                    "camapaign": "value if campaign name",
                    "campaign_id": {campaign_id:temp_data}
                }

    return jsonify(response), 200


if __name__ == "__main__":
    app.run(debug=True)
