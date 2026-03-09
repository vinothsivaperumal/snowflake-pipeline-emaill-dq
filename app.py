import os
import json
import ssl
import smtplib
from pathlib import Path
from datetime import datetime
from email.message import EmailMessage
from typing import Any

import pandas as pd
import snowflake.connector

from dotenv import load_dotenv

load_dotenv()

def get_env(name: str, required: bool = True, default: str | None = None) -> str | None:
    value = os.getenv(name, default)
    if required and not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def connect_snowflake():
    return snowflake.connector.connect(
        user=get_env("SNOWFLAKE_USER"),
        password=get_env("SNOWFLAKE_PASSWORD"),
        account=get_env("SNOWFLAKE_ACCOUNT"),
        warehouse=get_env("SNOWFLAKE_WAREHOUSE"),
        database=get_env("SNOWFLAKE_DATABASE"),
        schema=get_env("SNOWFLAKE_SCHEMA"),
        role=get_env("SNOWFLAKE_ROLE"),
    )


def load_report_config(config_path: str = "reports.json") -> list[dict[str, Any]]:
    with open(config_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    reports = data.get("reports", [])
    if not isinstance(reports, list):
        raise ValueError("'reports' must be a list in reports.json")

    return reports


def run_query_to_dataframe(conn, query: str) -> pd.DataFrame:
    cursor = None
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description] if cursor.description else []
        return pd.DataFrame(rows, columns=columns)
    finally:
        if cursor is not None:
            cursor.close()


def save_dataframe_to_csv(df: pd.DataFrame, file_prefix: str, output_dir: str) -> str:
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = Path(output_dir) / f"{file_prefix}_{timestamp}.csv"
    df.to_csv(file_path, index=False)
    return str(file_path)


def send_email_with_attachment(
    to_addresses: list[str],
    subject: str,
    body: str,
    attachment_path: str,
) -> None:
    smtp_host = get_env("SMTP_HOST")
    smtp_port = int(get_env("SMTP_PORT"))
    smtp_user = get_env("SMTP_USER")
    smtp_password = get_env("SMTP_PASSWORD")
    email_from = get_env("EMAIL_FROM")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = email_from
    msg["To"] = ", ".join(to_addresses)
    msg.set_content(body)

    with open(attachment_path, "rb") as f:
        file_data = f.read()
        file_name = os.path.basename(attachment_path)

    msg.add_attachment(
        file_data,
        maintype="application",
        subtype="octet-stream",
        filename=file_name,
    )

    context = ssl.create_default_context()

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls(context=context)
        server.login(smtp_user, smtp_password)
        server.send_message(msg)


def validate_report_config(report: dict[str, Any]) -> None:
    required_fields = ["name", "subject", "body", "query"]
    missing = [field for field in required_fields if not report.get(field)]
    if missing:
        raise ValueError(
            f"Report config missing required fields {missing} in report: {report.get('name', 'unknown')}"
        )


def process_reports():
    output_dir = get_env("OUTPUT_DIR", required=False, default="output")
    default_email_to = get_env("DEFAULT_EMAIL_TO", required=False, default="")

    reports = load_report_config("reports.json")
    if not reports:
        raise ValueError("No reports found in reports.json")

    conn = None
    try:
        conn = connect_snowflake()

        for report in reports:
            if not report.get("enabled", True):
                print(f"Skipping disabled report: {report.get('name')}")
                continue

            try:
                validate_report_config(report)

                report_name = report["name"]
                subject = report["subject"]
                body = report["body"]
                query = report["query"]
                to_addresses = report.get("to", [])
                file_prefix = report.get("file_name_prefix", report_name)

                if not to_addresses:
                    if default_email_to:
                        to_addresses = [default_email_to]
                    else:
                        raise ValueError(f"No recipients configured for report: {report_name}")

                print(f"Running report: {report_name}")
                df = run_query_to_dataframe(conn, query)

                csv_path = save_dataframe_to_csv(df, file_prefix, output_dir)
                print(f"Generated file: {csv_path}")

                send_email_with_attachment(
                    to_addresses=to_addresses,
                    subject=subject,
                    body=body,
                    attachment_path=csv_path,
                )
                print(f"Email sent for report: {report_name}")

            except Exception as report_error:
                print(f"Report failed [{report.get('name', 'unknown')}]: {report_error}")

    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    process_reports()
