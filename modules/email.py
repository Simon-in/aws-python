import os.path
import re
import smtplib
import ssl
import sys
import uuid
from email.header import Header
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Union

from modules.client import client, logger
from modules.conf import ConfigGlobal
from modules.redshift import redshift_unload_func, redshift_excel_unload_func
from modules.s3 import s3_parser_to_bucket_prefix

s3_client = client("s3", 3)
LOG = logger()
smtp_conn = ConfigGlobal.smtp_connection.split("|")

if len(smtp_conn) != 4:
    LOG.error(f"Invalid smtp connection properties: {ConfigGlobal.smtp_connection}")
    sys.exit(1)

SMTP_HOST = smtp_conn[0]
SMTP_PORT = int(smtp_conn[1])
SMTP_USER = smtp_conn[2]
SMTP_PASSWORD = smtp_conn[3]
DEFAULT_SENDER = "cncdp_notification@bayer.com"
env = ConfigGlobal.env


def is_valid_email(email_addr):
    regex = re.compile(
        r"([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\"([]!#-[^-~ \t]|(\\[\t -~]))+\")"
        r"@([-!#-'*+/-9=?A-Z^-~]+(\.[-!#-'*+/-9=?A-Z^-~]+)*|\[[\t -Z^-~]*])"
    )
    return True if re.fullmatch(regex, email_addr) else False


def read_s3_attachment(s3_uri: str) -> MIMEApplication:
    s3_uri = s3_uri.replace(" ", "").replace("\n", "")
    bucket, key, filename = s3_parser_to_bucket_prefix(s3_uri)
    res = s3_client.get_object(Bucket=bucket, Key=key)
    body = res["Body"].read()
    part = MIMEApplication(body, key)
    part.add_header("Content-Disposition", "attachment", filename=filename)
    return part


def retrieve_addrs(addrs: Union[List[str], str]) -> List[str]:
    if isinstance(addrs, str):
        addrs = addrs.split(",")
    if isinstance(addrs, list):
        addrs = [_.strip() for _ in addrs if is_valid_email(_.strip())]
    if addrs is None:
        addrs = []
    return addrs


def email_sender(
        subject: str,
        main_content: str,
        to_receivers: Union[List[str], str],
        cc_receivers: Union[List[str], str] = None,
        attachments: Union[List[str], str] = None,
        sender: str = DEFAULT_SENDER,
        mime_type="plain",
        host=SMTP_HOST,
        port=SMTP_PORT,
        user=SMTP_USER,
        password=SMTP_PASSWORD,
        debug=False,
):
    """Send email with attachments.

    :param subject: email subject
    :param main_content: main content of the email
    :param to_receivers: list of receivers
    :param cc_receivers: list of cc receivers
    :param attachments: [optional] s3 uri or list of s3 uri
    :param sender: [optional]
    :param mime_type: [optional] mime type of main_content, default is plain
    :param host: [optional] smtp host
    :param port: [optional] smtp port
    :param user: [optional] smtp user
    :param password: [optional] smtp password
    :param debug: [optional] enable debug mode for smtp

    """
    to_receivers = retrieve_addrs(to_receivers)
    cc_receivers = retrieve_addrs(cc_receivers)
    if not to_receivers:
        LOG.error(f"No valid receiver email address found in {to_receivers}")
        raise ValueError(f"Invalid receiver email address: {to_receivers}")

    LOG.info("*****************/Start to send email!/*****************")
    message = MIMEMultipart()
    message["From"] = sender
    message["To"] = ",".join(to_receivers)
    message["Cc"] = ",".join(cc_receivers)
    message["Subject"] = Header(subject)
    message.attach(MIMEText(main_content, mime_type))

    # Add attachments
    if isinstance(attachments, str):
        attachments = attachments.split(",")
    if attachments:
        for attachment in attachments:
            part = read_s3_attachment(attachment)
            LOG.info(f"Attachment {attachment} has been loading")
            message.attach(part)

    with smtplib.SMTP(host, port) as server:
        if debug:
            server.set_debuglevel(1)
        context = ssl.create_default_context()
        server.starttls(context=context)
        server.login(user, password)
        server.sendmail(
            from_addr=sender,
            to_addrs=to_receivers + cc_receivers,
            msg=message.as_string(),
        )
        LOG.info("*****************/End./*****************")


def redshift_to_email_func(
        subject: str,
        main_content: str,
        to_receivers: Union[List[str], str],
        cluster_id: str,
        rs_db: str,
        cc_receivers: Union[List[str], str] = None,
        mime_type="plain",
        attachment_conf=None):
    """Unload redshift query result to s3 and send email with the attachment.
    :param subject: email subject
    :param main_content: main content of the email
    :param to_receivers: list of receivers
    :param cluster_id: redshift cluster id
    :param rs_db: redshift database
    :param cc_receivers: list of cc receivers
    :param mime_type: mime type of main_content
    :param attachment_conf: mime type of main_content, [{"xxx.xlsx": {"sheet1":"select xxx", "sheet2": "select xxx"}},
     {"xxx.csv": "select xxx"}]
    """
    LOG.info("Start to unload redshift")
    s3_uri_list = []
    if attachment_conf.__len__() > 0:
        for attachment_name, detail in attachment_conf.items():
            target_s3_uri = ConfigGlobal.redshift_unload_stage_s3 + str(uuid.uuid4()) + "/" + attachment_name
            if attachment_name.lower().split(".")[-1] in ("xlsx", "xls", "xlsx") and type(detail) == dict:
                redshift_excel_unload_func(
                    cluster_id=cluster_id,
                    rs_db=rs_db,
                    src_map=detail,
                    s3_uri=target_s3_uri
                )
                s3_uri_list.append(target_s3_uri)

            elif attachment_name.lower().split(".")[-1] in ("csv", "parquet", "json") and type(detail) == str:
                redshift_unload_func(
                    cluster_id=cluster_id,
                    rs_db=rs_db,
                    statement=detail,
                    s3_uri=target_s3_uri,
                    fmt=attachment_name.split(".")[-1]
                )
                s3_uri_list.append(target_s3_uri)

            else:
                LOG.error("Invalid email attachment configuration! Format not supported or got wrong detail.")
                raise
    email_sender(
        subject,
        main_content,
        to_receivers,
        attachments=s3_uri_list,
        cc_receivers=cc_receivers,
        mime_type=mime_type,
    )

    # delete temp attached s3 objects
    for s3_uri in s3_uri_list:
        bucket, key, _ = s3_parser_to_bucket_prefix(s3_uri)
        s3_client.delete_object(Bucket=bucket, Key=key)

