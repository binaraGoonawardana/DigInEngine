__author__ = 'Thivatharan Jeganathan'
__version__ = '1.0.0.0'

import smtplib
from apiclient import errors


def share_component(params,user_id, domain):
    try:
        sender = params['body'][0]['sender']
        to = params['body'][0]['to']
        subject = params['body'][0]['subject']
        message_text = params['body'][0]['message']
        if params['body'][0]['source'].lower() == 'gmail':
            message = _create_message(sender,to,subject,message_text)
            output = _send_Gmail(sender,to,message)
        return "Message Sent successfully"

    except errors.HttpError, error:
        print 'An error occurred: %s' % error


def _create_message(sender, to, subject, message_text):
  """Create a message for an email.

  Args:
    sender: Email address of the sender.
    to: Email address of the receiver.
    subject: The subject of the email message.
    message_text: The text of the email message.

  Returns:
    An object containing a base64url encoded email object.
  """
  msg = "\r\n".join([
      "From: "+sender,
      "To: "+to,
      "Subject: "+subject,
      "",
      message_text
  ])
  return msg

def _send_Gmail(sender,to,msg):

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.ehlo()
        server.starttls()
        server.login('digin@duosoftware.com', '')
        server.sendmail('digin@duosoftware.com', to, msg=msg)
        server.quit()
        return "mail send sucessfully"

    except errors.HttpError, error:
        print 'An error occurred: %s' % error