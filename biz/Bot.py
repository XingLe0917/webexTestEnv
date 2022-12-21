import re
import logging
import requests
import functools
from common.wbxchatbot import wbxchatbot

logger = logging.getLogger("DBAMONITOR")

# def sentAlertToBot(content, roomId):
#     logger.info("sentAlertToBot, roomId=%s content=%s  " % (roomId, content))
#     headers = {"Content-Type":"application/json",
#                "Authorization":"Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"}
#     resp_room = requests.post(
#         'https://webexapis.com/v1/messages',
#         json={"roomId": roomId, "markdown":content},
#         headers=headers
#     )
#     return resp_room.json()

@functools.lru_cache(maxsize=128)
def checkCec(cec):
    bot = wbxchatbot()
    res = bot.get_people_cec(cec)
    if res == "Error!":
        return False
    return True

def sentAlertToBot(content, room_id, to_person, optional_flag):
    """
    send alert message to webex room(@person) or person.

    @Requirements:
        - to room
        - to [person | oncall]
        - to room and @ [person | oncall]
        - [optional] to room and to [person | oncall]
        - [optional] to room and @/to [person | oncall]

    @params:
        :param content        : send message, not null
        :param room_id        : room id, like: Y2lzY29zcGFyazovL3VzL1JPT00vYTc4ZjllZDAtMDY2ZS0xMWVkLWE5YmEtYjNkOTQ4ODI2YTJm
        :param to_person      : person cec name or 'oncall'
        :param optional_flag  : Act on the previous parameter, [direct/at], means send the message
                                to the person or '@' the person in the room.

    @return: {"status", "SUCCESS/FAILED", "errormsg": "...", "data": None/[...]}
    """
    # logger.info("sentAlertToBot():: room_id=%s,to_person=%s,optional_flag=%s " % (room_id, to_person,optional_flag))
    flags = ["at", "direct"]
    is_sent_to_room = False
    is_sent_to_person = False
    res = {"status": "SUCCESS", "errormsg": "", "data": None}
    # check params
    if optional_flag is not None and optional_flag not in flags:
        res["status"] = "FAILED"
        res["errormsg"] = "not support params on optional_flag, should be 'at' or 'direct'"
        logger.error(res["errormsg"])
        return res

    bot = wbxchatbot()
    create_message_url = "https://webexapis.com/v1/messages"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": "Bearer YWMxZDEyNzUtMTQxYi00MjRiLWFkYzktM2U1M2ZmMzc3NzQyNTNmZWY0MGEtNTdk_PF84_1eb65fdf-9643-417f-9974-ad72cae0e10f"
    }
    
    room_id_pattern = r"[a-zA-Z0-9]{76}"

    if not content:
        res["status"] = "FAILED"
        res["errormsg"] = "content must be non-empty"
        logger.error(res["errormsg"])
        return res

    # confirm room id
    if room_id is not None:
        is_sent_to_room = True
        if re.match(room_id_pattern, room_id) is None:
            res["status"] = "FAILED"
            res["errormsg"] = "room ID:[{}] is not correct, should be a string with 76 char".format(room_id)
            logger.error(res["errormsg"])
            return res

    # confirm send to person
    if to_person is not None and (optional_flag is None or "direct" == optional_flag):
        is_sent_to_person = True

    # confirm person email
    if to_person is not None:
        if "oncall" == to_person:
            to_person = bot.get_oncall_cec_from_pagerduty()
        else:
            if checkCec(to_person):
                to_person = "%s@cisco.com" % to_person
            else:
                res["status"] = "FAILED"
                res["errormsg"] = "such person cec:[%s] not exists" % to_person
                logger.error(res["errormsg"])
                return res

    # edit content
    if "at" == optional_flag:
        content = "%s\n<@personEmail:%s>" % (content, to_person)

    if is_sent_to_room:
        resp = requests.post(create_message_url, json={"roomId": room_id, "markdown": content}, headers=headers)
        if resp.status_code != 200:
            res["status"] = "FAILED"
            res["errormsg"]+=resp.text
            logger.warning(res["errormsg"])

    if is_sent_to_person:
        resp = requests.post(create_message_url, json={"toPersonEmail": to_person, "markdown": content}, headers=headers)
        if resp.status_code != 200:
            res["status"] = "FAILED"
            res["errormsg"]+=resp.text
            logger.warning(res["errormsg"])

    return res