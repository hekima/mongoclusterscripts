import boto.ses

def send_email(address, subject, text, recipients):
    conn = boto.ses.connect_to_region('us-east-1')
    conn.send_email(address, subject, text, recipients)
