import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.header import Header


from ipdb import set_trace

class user():
    def __init__(self, address=None, password=None, name=None, authentification=None):
        if address is None:
            print('Some missing param[s] occurs.')
            print('Please specify the Email address.')
            return
        else:
            self.address = address
        self.password = password
        if name is None:
            self.name = address.split('@')[0]
        else:
            self.name = name
        self.authentification = authentification
    
    def __repr__(self):
        print('Email address: ', self.address)
        print('Email password: ', self.password)
        print('Email name: ', self.name)
        print('Email authentification: ', self.authentification)
        return "------------------------------"


email_pool = {
            'public': user(address='lcmfjinrong@163.com', password='lcmf12110', name='public', authentification='lcmf121101'),
            'yangning': user(address='yangning@licaimofang.com', name='yangning'),
            'jiaoyang': user(address='jiaoyang@licaimofang.com')
        }

def main():
    mail_host = "smtp.163.com"
    mail_user = email_pool['public']
    # from_address must equal to authurized user -- mail_user
    from_address = mail_user.address
    to_address = email_pool['yangning'].address



    message = MIMEMultipart()
    message.attach(MIMEText('Work schedule tomorrow', 'html', 'utf-8'))
    #message = MIMEText('Work schedule tomorrow', 'html', 'utf-8')
    #message = MIMEText('Work schedule tomorrow', 'plain', 'utf-8')

    message['From'] = from_address
    message['To'] = to_address
    subject = 'from Tong Cheng'
    message['Subject'] = subject

    attachment1 = MIMEText(open('./README.md', 'rb').read(), 'base64', 'utf-8')
    attachment1['Content-Type'] = 'application/octet-stream'
    attachment1['Content-Disposition'] = 'attachment; filename=Schedule.md'
    message.attach(attachment1)

    try:
        smtpObj = smtplib.SMTP_SSL()
        # port=25 with non-SSL protocol
        # port=465/994 with SSL protocol
        smtpObj.connect(mail_host, port=465)
        smtpObj.login(mail_user.address, mail_user.authentification)
        smtpObj.sendmail(from_address, to_address, message.as_string() )
        print('Email sent successfully!')
    except smtplib.SMTPException as e:
        print('Error!')
        print(e)

if __name__ == '__main__':
    main()
