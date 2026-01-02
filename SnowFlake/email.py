import ast
import datetime
import io
import uuid
from typing import Union, List


from modules.client import LOG
from modules.email import email_sender
from modules.glue_args import get_glue_args
from modules.conf import ConfigGlobal
from modules.s3 import s3_delete_object, s3_parser_to_bucket_prefix, s3_client
from modules.secret_manager import get_secret
from bayer_cdp_common_utils.snowflake_handler import SnowflakeConnector

"""
        glue 中的参数
        SUBJECT
        BODY
        TO
        ATTACHMENT_CONF
"""


def snowflake_to_email_func(
        subject: str,
        main_content: str,
        to_receivers: Union[List[str], str],
        cc_receivers: Union[List[str], str] = None,
        mime_type="plain",
        attachment_conf=None):
    """Unload snowflake query result to s3 and send email with the attachment.
    :param subject: email subject
    :param main_content: main content of the email
    :param to_receivers: list of receivers
    :param cc_receivers: list of cc receivers
    :param mime_type: mime type of main_content
    :param attachment_conf: mime type of main_content, [{"xxx.xlsx": {"sheet1":"select xxx", "sheet2": "select xxx"}},
     {"xxx.csv": "select xxx"}]
    """
    LOG.info("Start to unload snowflake")
    s3_uri_list = []
    if attachment_conf.__len__() > 0:
        attachment = ast.literal_eval(attachment_conf)
        for attachment_name, detail in attachment.items():
            target_s3_uri = ConfigGlobal.unload_stage_s3 + str(uuid.uuid4()) + "/" + attachment_name
            bucket, prefix, _ = s3_parser_to_bucket_prefix(target_s3_uri)
            if attachment_name.lower().split(".")[-1] in ("xlsx", "xls", "xlsx") and type(detail) == dict:
                for k, v in detail:
                    cur = sf_conn.execute_query(v)
                    df = cur.fetch_pandas_all()
                    excel_buffer = io.BytesIO()
                    df.to_excel(
                        index=False,
                        excel_writer=excel_buffer,
                        sheet_name=k
                    )
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=prefix,
                        Body=excel_buffer.getvalue()
                    )
                s3_uri_list.append(target_s3_uri)

            elif attachment_name.lower().split(".")[-1] in ("csv", "parquet", "json") and type(detail) == str:
                for k, v in detail:
                    cur = sf_conn.execute_query(v)
                    df = cur.fetch_pandas_all()
                    csv_buffer = io.BytesIO()
                    df.to_csv(
                        index=False,
                        sep=',',
                        encoding='UTF-8',
                        path_or_buf=csv_buffer
                    )
                    s3_client.put_object(
                        Bucket=bucket,
                        Key=prefix,
                        Body=csv_buffer.getvalue()
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
        s3_delete_object(s3_uri)


if __name__ == '__main__':
    args = get_glue_args(
        positional=[
            "SUBJECT",
            "BODY",
            "TO"
        ],
        optional={
            "MIME_TYPE": "plain",
            "CC": "",
            "ATTACHMENT_CONF": {},
            "CONTEXT_PARAMS": {},
            "RETRY": 6,
            "DELAY": 10,
            "SOURCE_SYSTEM": "",
            "WAREHOUSE": "",
            "DATABASE": "",
            "SCHEMA": ""
        },
    )
    glue_start_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    subject = args["SUBJECT"]
    main_content = args["BODY"]
    to_receivers = args["TO"]
    cc_receivers = args["CC"].strip()
    mime_type = args["MIME_TYPE"]
    retry = int(args["RETRY"])
    delay = int(args["DELAY"])
    attachment_conf = ast.literal_eval(args["ATTACHMENT_CONF"]) if args["ATTACHMENT_CONF"] else {}
    # context_params = ast.literal_eval(args["CONTEXT_PARAMS"]) if args["CONTEXT_PARAMS"] else {}
    source_system = args["SOURCE_SYSTEM"]

    # 获取连接
    SF_CONN_ID = ConfigGlobal.snowflake_secret
    sf_conn = SnowflakeConnector.from_secret(SF_CONN_ID)

    # for key, sql in context_params.items():  # 功能不需要
    #     query_res = redshift_query_executor(
    #         cluster_id=cluster_id,
    #         db_name=rs_db,
    #         sql=sql,
    #         retry=retry,
    #         delay=delay,
    #     )
    #     assert len(query_res) == 1 and len(
    #         query_res[0]) == 1, f"Exception: Email context_sql returned more than ONE row: {sql}"
    #     main_content = re.sub(r"{{" + key + r"}}", str(list(query_res[0][0].values())[0]), main_content)

    snowflake_to_email_func(
        subject=subject,
        main_content=main_content,
        to_receivers=to_receivers,
        cc_receivers=cc_receivers,
        mime_type=mime_type,
        attachment_conf=attachment_conf
    )
    glue_end_time = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")