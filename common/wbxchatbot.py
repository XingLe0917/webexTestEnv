import json
import requests
import logging
from pdpyras import APISession

logger = logging.getLogger("DBAMONITOR")


room_id_dict = {
    "ccp dba team": "Y2lzY29zcGFyazovL3VzL1JPT00vZjk1MmVkMjAtOWIyOC0xMWVhLTliMDQtODVlZDBhY2M0ZTNi",
    "ccp project": "Y2lzY29zcGFyazovL3VzL1JPT00vNzFjMGYwZDAtMDVkZS0xMWVhLTg2NmMtZDM2MDY0NjdjZDI2",
    "dbabot": "Y2lzY29zcGFyazovL3VzL1JPT00vZDVlZDExYTAtY2IwNS0xMWVhLThiMWEtYjdhM2Q0NWRjODBl",
    "webdb Metrics build internal group": "Y2lzY29zcGFyazovL3VzL1JPT00vZDIyMzk5NjAtYmFhNS0xMWVhLWFjYTctNWQ3NjJiMjQ3ZThl",
    "ccp dba alert": "Y2lzY29zcGFyazovL3VzL1JPT00vZDA0NzQxYjAtMDMwNi0xMWViLTk2NTktNjNhNGQ0MDY3ODFh",
    "yejfeng": "Y2lzY29zcGFyazovL3VzL1JPT00vNDE0MDJhMDAtODJmMy0xMWViLWI4YTAtNzVhZmQzZDExMjlh"
}

room_group = {
    "PCCP_TEST":[{"CCP DBA team":"Y2lzY29zcGFyazovL3VzL1JPT00vZjk1MmVkMjAtOWIyOC0xMWVhLTliMDQtODVlZDBhY2M0ZTNi"}],
    "PCCP_DBA":[{"ccp project": "Y2lzY29zcGFyazovL3VzL1JPT00vNzFjMGYwZDAtMDVkZS0xMWVhLTg2NmMtZDM2MDY0NjdjZDI2"}]
}

class wbxchatbot:
    def __init__(self):
        self.room_id = room_id_dict["ccp dba alert"]
        self.chatbot_url = "http://sjgrcabt102.webex.com:9000/api/sentAlertToBot"
        self.chatbot_rooms = "https://webexapis.com/v1/rooms"
        self.chatbot_people = "https://webexapis.com/v1/people"
        self.chatbot_message_url = "https://webexapis.com/v1/messages"
        self.chatbot_hearders = {"Authorization": "Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"}
        self.chatbot_authorization = "Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"

    def get_oncall_cec_from_pagerduty(self):
        api_token = "u+EKr6hAANd3NQFExEYg"
        oncall_team_name = "CEO-cwopsdba-Primary"
        session = APISession(api_token)
        oncall_user_list = []
        # if schedule id always be the one,could uncomment follow line to speed up find oncaller process
        for item in session.iter_all('oncalls', {"include[]": "users", "schedule_ids[]": ["PJAF4DE"]}):
        # for item in session.iter_all('oncalls'):
            if item["schedule"] and item["schedule"]["summary"] == oncall_team_name:
                if item["user"] and item["user"]["summary"]:
                    oncall_user_list.append(item["user"]["summary"])
        oncall_user_list = list(set(oncall_user_list))
        if len(oncall_user_list) != 1:
            self.alert_msg_to_person("the oncall list from pagerduty is %s which is abnormal!!" % oncall_user_list, "yejfeng@cisco.com")
        oncall_user_name = oncall_user_list[0]
        oncall_user_email = session.find('users', oncall_user_name, attribute="name").get("email", None)
        logger.info("oncall : %s" % oncall_user_email)
        return oncall_user_email

    def alert_msg_to_dbabot_and_call_oncall(self, msg):
        oncall_email = self.get_oncall_cec_from_pagerduty()
        response = requests.post(
            url=self.chatbot_url, 
            json={"roomId": room_id_dict["dbabot"],"content": msg + "\n<@personEmail:%s>" % oncall_email}, 
            headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"}
        )
        assert response.status_code == 200, "send alert error to: dbabot room"

    def get_people_id_by_email(self, email):
        cec = email.split("@")[0]
        url = self.chatbot_people +"?email=%s@cisco.com"%(cec)
        response = requests.get(url=url, headers=self.chatbot_hearders)
        if response.status_code == 200:
            return response.json()["items"][0]["id"]
        else:
            return response.status_code, response.json()

    def alert_msg(self, msg):
        response = requests.post(url=self.chatbot_url, json={"roomId": self.room_id,"content": msg}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_dbateam(self, msg):
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["ccp dba team"],"content": msg}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_dbabot(self, msg):
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["dbabot"],"content": msg}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_web_metric(self, msg):
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["webdb Metrics build internal group"],"content": msg}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})
        # print(response.status_code, response.content)

    def alert_msg_to_dbabot_by_roomId(self, msg,roomId):
        response = requests.post(url=self.chatbot_url, json={"roomId": roomId,"content": msg}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})

    def alert_msg_to_dbabot_by_group(self,group_name,msg):
        roomIds=room_group[group_name]
        for name in roomIds:
            for (name, roomId) in dict(name).items():
                self.alert_msg_to_dbabot_by_roomId(msg, roomId)

    def alert_msg_to_person(self, msg, email):
        response = requests.post(url=self.chatbot_url, json={"roomId": room_id_dict["yejfeng"],"content": msg + "\n<@personEmail:%s>" % email}, headers={"Authorization": "Basic Y2NwX3Rlc3Q6Tjd3amgyJVlP"})

    def address_alert_list(self, title_list, info_list):
        list_len = len(title_list)
        info_len = len(info_list)
        colume_list = []
        for i in range(0, list_len):
            colume_list.append([title_list[i]])
        for row in info_list:
            for i in range(0, list_len):
                colume_list[i].append(row[i])
        new_colume_list = []
        for i in range(0, list_len):
            row_len = 0
            for item in colume_list[i]:
                row_len = len(str(item)) if len(str(item)) > row_len else row_len
                # row_len = row_len // 4 * 4 + 4
            new_colume_item = []
            for item in colume_list[i]:
                # item_info = str(item) + "\t" * ((row_len - len(str(item))) // 4 + 1)
                item_info = str(item) + " " * (row_len - len(str(item))) + "\t\t"
                new_colume_item.append(item_info)
            new_colume_list.append(new_colume_item)
        rst_list = []
        for i in range(0, info_len + 1):
            rst_list.append([])
        for row in new_colume_list:
            for i in range(info_len + 1):
                rst_list[i].append(row[i])
        msg = ""
        for row in rst_list:
            msg += "\t" + "".join(row) + "\n"
        return msg

    def get_dbabot_rooms(self,type):
        url = self.chatbot_rooms
        if "direct" == type or "group" == type:
            url = self.chatbot_rooms + "?type=%s" %(type)
        response = requests.get(url=url, headers={
            "Authorization": "Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"})
        if response.status_code == 200:
            items = json.loads(response.text)['items']
            return items
        else:
            return "Error!"

    def get_people_by_id(self,id):
        url = self.chatbot_people +"?id=%s"%(id)
        response = requests.get(url=url, headers=self.chatbot_hearders)
        if response.status_code == 200:
            items = json.loads(response.text)['items']
            return items
        else:
            return "Error!"


    def get_people_cec(self,cec):
        url = self.chatbot_people +"?email=%s@cisco.com"%(cec)
        response = requests.get(url=url, headers=self.chatbot_hearders)
        if response.status_code == 200:
            items = json.loads(response.text)['items']
            return items
        else:
            return "Error!"

    def create_message_to_people(self,toPersonId,text):
        response = requests.post(url=self.chatbot_message_url, headers={"Content-Type": "application/json",
                                                   "Authorization": "Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"},
                                 json={"toPersonId": toPersonId, "markdown": text})
        if response.status_code == 200:
            res = json.loads(response.text)
            return res
        else:
            return "Error!"

    def get_roomid_by_roomname(self, roomname):
        roomname = " ".join(roomname.split("-"))
        response = requests.get(url="%s?type=group" % self.chatbot_rooms, headers={"Content-Type": "application/json",
                                                   "Authorization": self.chatbot_authorization})
        if response.status_code != 200:
            return "Error to get roomid by roomname"
        items = json.loads(response.text)["items"]
        room_dict = {}
        for room_item in items:
            # {'id': 'Y2lzY29zcGFyazovL3VzL1JPT00vMDhiYWU1YjAtMDdkOC0xMWVkLTk4M2YtNGQ0MzVlYzIyZWIw',
            #  'title': 'miaoxu', 'type': 'group', 'isLocked': True, 'lastActivity': '2022-07-21T06:28:57.766Z',
            #  'teamId': 'Y2lzY29zcGFyazovL3VzL1RFQU0vMDhiYWU1YjAtMDdkOC0xMWVkLTk4M2YtNGQ0MzVlYzIyZWIw',
            #  'creatorId': 'Y2lzY29zcGFyazovL3VzL1BFT1BMRS85OGM1ZjZmYy0xY2RhLTQ4ZDgtOWM5ZS0xNWY4ODMzOWIyY2I',
            #  'created': '2022-07-20T02:59:55.915Z',
            #  'ownerId': 'Y2lzY29zcGFyazovL3VzL09SR0FOSVpBVElPTi8xZWI2NWZkZi05NjQzLTQxN2YtOTk3NC1hZDcyY2FlMGUxMGY',
             # 'description': '', 'isPublic': False}
            room_item_name = room_item["title"]
            del room_item["title"]
            if room_item_name in room_dict.keys():
                room_dict[room_item_name].append(room_item)
            else:
                room_dict.update({
                    room_item_name: [room_item]
                })
        room_data = room_dict.get(roomname, None)
        if not room_data:
            return "cannot find the roomid by room name : %s" % roomname
        if len(room_data) > 1:
            sorted(room_data, key=lambda _item: _item["created"], reverse=True)
        room_id = room_data[0]["id"]
        return room_id


if __name__ == '__main__':

    job = wbxchatbot()
    # job.alert_msg("hello room")
    # job.alert_msg_to_dbabot_by_group("PCCP_TEST","ddd")
    res = job.get_dbabot_rooms("direct")
    for item in res:
        print(item)
    # people = job.get_people_by_id("Y2lzY29zcGFyazovL3VzL1BFT1BMRS8zMDgxOTk1OS05YWU4LTRlNWItYjhhZS0yNWUwZGZlZTdhNDI")
    # print(people)
    # people = job.get_people_cec("lexing")
    # print(people)
    # people2 = job.get_people_cec("lexing")
    # print(people2)
    # peopleid = ""
    # if len(people2)>0:
    #     peopleid = people2[0]['id']
    # print(peopleid)
    # res = job.create_message_to_people(peopleid,"### Hi, I am testing. If you receive this message, please tell me. Thank you. \n ~ From Le Xing")


