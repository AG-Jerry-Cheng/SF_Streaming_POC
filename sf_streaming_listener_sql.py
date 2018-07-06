import asyncio, json, pymysql
from sshtunnel import SSHTunnelForwarder
from aiosfstream import SalesforceStreamingClient
from jinja2 import Template

environments = {
    "STG":  {"host" : 'put STG host endpoint information here'}
}

async def stream_events():
    # connect to Streaming API
    async with SalesforceStreamingClient(
            consumer_key="put consumer key",
            consumer_secret="put consumer secret",
            username="put your username",
            password="put your password") as client:

        # subscribe to topics
        await client.subscribe("/topic/FranchiseDealerUpdates")

        # listen for incoming messages
        async for message in client:
            topic = message["channel"]
            data = str(json.dumps(message["data"]))
            msg_data = json.loads(data)
            event_type = msg_data["event"]["type"]
            print(f"{topic}: {data}")

            msg_dict = {key_name: msg_data['sobject'][key_name] for key_name in msg_data['sobject']}
            up_insert_del_data_to_dms(event_type, msg_dict)


def up_insert_del_data_to_dms(event_type, msg_dict):
    d_name = msg_dict["Name"]
    d_addr = msg_dict["Dealer_Address__c"]
    d_city = msg_dict["Dealer_City__c"]
    d_phone = msg_dict["Dealer_Phone__c"]
    d_website = msg_dict["Dealer_Website__c"]
    d_code = msg_dict["Dealer_Code__c"]
    d_state = msg_dict["Dealer_State__c"]

    class DmsDataProcess:
        def __init__(self, environment='STG', database='dms'):
            env = environments.get(environment)
            self.db = pymysql.connect(host='127.0.0.1',
                                      port=server.local_bind_port,
                                      user='developer',
                                      passwd='password',
                                      db=database,
                                      cursorclass=pymysql.cursors.DictCursor)
            self.dataProcess()

        def dataProcess(self):
            if event_type == 'updated':
                print (' - event_type : updated - ')
                self.updateDealers()
            elif event_type == 'deleted':
                print (' - event_type : deleted - ')
                self.deleteDealers()
            elif event_type == 'created':
                print (' - event_type : created - ')
                self.insertDealers()

        def insertDealers(self):
            cur = self.db.cursor()
            with open('sql/insert_dms_info.sql') as f:
                sql_template = Template(f.read())
                sql = sql_template.render(dealer_name = d_name, dealer_address = d_addr, dealer_city = d_city,
                                      dealer_state = d_state, dealer_phone = d_phone, dealer_code = d_code)

            cur.execute(sql)
            self.db.commit()
            cur.close()

        def deleteDealers(self):
            cur = self.db.cursor()
            with open('sql/delete_dms_info.sql') as f:
                sql_template = Template(f.read())
                sql = sql_template.render(dealer_code = d_code)

            cur.execute(sql)
            self.db.commit()
            cur.close()

        def updateDealers(self):
            cur = self.db.cursor()
            with open('sql/update_dms_info.sql') as f:
                sql_template = Template(f.read())
                sql = sql_template.render(dealer_name = d_name, dealer_address = d_addr, dealer_city = d_city,
                                      dealer_state = d_state, dealer_phone = d_phone, dealer_code = d_code)

            cur.execute(sql)
            self.db.commit()
            cur.close()

    env = environments.get('STG')
    with SSHTunnelForwarder(
            ssh_address_or_host=('staging-jumpbox.autogravity.com', 22),
            ssh_username="jerry",
            ssh_password="",
            ssh_pkey="/Users/jerrycheng/.ssh/id_rsa",
            remote_bind_address=(env.get('host'), 3306)
    ) as server:
        DmsDataProcess()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(stream_events())