import os
import sys
import re
import django
import pandas as pd

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(BASE_DIR)

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "operator_app.settings")
django.setup()

from django.db import connection

from api.models import (
    L1_PartInfoMaster,
    L2_ProcessReportMaster,
    L3_ParameterDetailMaster,
)

FILE_PATH = r"C:\Users\Dell\Desktop\Updated topre CP\Control_Plan_Database_Ready.xlsx"


def clean(value):
    if pd.isna(value):
        return ""

    value = str(value).replace("\n", " ").strip()
    value = re.sub(r"\s+", " ", value)

    if value.lower() in ["nan", "none", "null", "-", "--", ""]:
        return ""

    return value


def delete_old_l1_l2_l3_data(customer_name, part_name, part_no):
    l1_table = L1_PartInfoMaster._meta.db_table
    l2_table = L2_ProcessReportMaster._meta.db_table
    l3_table = L3_ParameterDetailMaster._meta.db_table

    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            DELETE FROM "{l3_table}"
            WHERE process_report_id IN (
                SELECT l2.id
                FROM "{l2_table}" l2
                INNER JOIN "{l1_table}" l1 ON l1.id = l2.part_info_id
                WHERE l1.customer_name = %s
                AND l1.part_name = %s
                AND l1.part_no = %s
            )
            """,
            [customer_name, part_name, part_no],
        )

        cursor.execute(
            f"""
            DELETE FROM "{l2_table}"
            WHERE part_info_id IN (
                SELECT id FROM "{l1_table}"
                WHERE customer_name = %s
                AND part_name = %s
                AND part_no = %s
            )
            """,
            [customer_name, part_name, part_no],
        )

        cursor.execute(
            f"""
            DELETE FROM "{l1_table}"
            WHERE customer_name = %s
            AND part_name = %s
            AND part_no = %s
            """,
            [customer_name, part_name, part_no],
        )


def upload_excel():
    print("FILE PATH:", FILE_PATH)
    print("FILE EXISTS:", os.path.exists(FILE_PATH))

    if not os.path.exists(FILE_PATH):
        print("File not found. Check file path.")
        return

    df = pd.read_excel(FILE_PATH, sheet_name="Database_Import_Format")
    df.columns = [clean(col) for col in df.columns]

    required_cols = [
        "Customer",
        "Part Name",
        "Part No",
        "Model",
        "Operation",
        "Type",
        "Parameter",
        "Specification",
        "Tolerance",
        "Instrument",
    ]

    for col in required_cols:
        if col not in df.columns:
            print(f"Missing column: {col}")
            return

    for col in required_cols:
        df[col] = df[col].apply(clean)

    df = df[df["Part Name"] != ""]

    parts_df = df[["Part Name", "Part No", "Model"]].drop_duplicates()

    l1_count = 0
    l2_count = 0
    l3_count = 0
    deleted_parts_count = 0

    for _, part_row in parts_df.iterrows():
        customer_name = "TOPRE"
        part_name = clean(part_row["Part Name"])
        part_no = clean(part_row["Part No"]) or "UNKNOWN"
        model_name = clean(part_row["Model"]) or "YTB"

        delete_old_l1_l2_l3_data(customer_name, part_name, part_no)
        deleted_parts_count += 1

        part_obj = L1_PartInfoMaster.objects.create(
            customer_name=customer_name,
            part_name=part_name,
            part_no=part_no,
            model_name=model_name,
        )
        l1_count += 1

        part_data = df[
            (df["Part Name"] == part_name)
            & (df["Part No"] == part_no)
            & (df["Model"] == model_name)
        ]

        operation_map = {}

        for _, row in part_data.iterrows():
            operation = clean(row["Operation"])
            if not operation:
                continue

            if operation not in operation_map:
                process_obj = L2_ProcessReportMaster.objects.create(
                    part_info=part_obj,
                    report_name=operation,
                )
                operation_map[operation] = process_obj
                l2_count += 1
            else:
                process_obj = operation_map[operation]

            category = clean(row["Type"]).upper()
            parameter_name = clean(row["Parameter"])
            specification = clean(row["Specification"])
            tolerance = clean(row["Tolerance"])
            instrument = clean(row["Instrument"])

            if tolerance:
                specification = f"{specification} {tolerance}".strip()

            if not parameter_name and not specification and not instrument:
                continue

            L3_ParameterDetailMaster.objects.create(
                process_report=process_obj,
                category=category,
                parameter_name=parameter_name,
                specification=specification,
                instrument=instrument,
            )
            l3_count += 1

    print("================================")
    print("UPLOAD COMPLETED SUCCESSFULLY")
    print("Customer: TOPRE")
    print("Old L1/L2/L3 deleted for parts:", deleted_parts_count)
    print("L1 inserted:", l1_count)
    print("L2 inserted:", l2_count)
    print("L3 inserted:", l3_count)
    print("================================")


if __name__ == "__main__":
    upload_excel()