import sys
import traceback

import requests
#from slackclient import SlackClient

from config import *


def send_notification(subject, message):
    """

    :param subject: the subject of the notification
    :param message: the body/message of the notification
    :return: None
    TODO error handling
    """

    text = "```\n{sub}\n\n{msg}\n```".format(sub=subject, msg=message)
    slack_client = SlackClient(SLACK_BOT_TOKEN)
    response = slack_client.api_call(
        "chat.postMessage",
        channel=SLACK_CHANNEL,
        text=text
    )
    return None


def send_error_notification():
    e_type, e_name, e_traceback = sys.exc_info()
    # subject = DEPLOYMENT_TYPE + "-att-models-error : "
    subject = "-att-models-error : "	
    message = e_type.__name__ + " ( " + str(e_name) + " ) " + "\n\n" + "".join(traceback.format_tb(e_traceback))
    # send_notification(subject=subject, message=message)
    return None
