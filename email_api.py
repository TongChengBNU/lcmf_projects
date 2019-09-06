import smtplib

smtp = smtplib.SMTP()
response_code, message = smtp.connect(host='smtp.163.com', port=25)
smtp.login(user='18295731118@163.com', password='cc980126')

from_address = '18295731118@163.com'
to_address = 'yangning@licaimofang.com'
message = 'Test.'
tmp2 = smtp.sendmail(from_addr=from_address, to_addrs=to_address, msg=message)

