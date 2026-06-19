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

FOLDER_PATH = r"C:\Users\Dell\Desktop\Updated Gestamp CP"

POSSIBLE_FILES = [
    "REINFORCEMENT A-PILLAR TOP HINGE-LHRH.xlsx",

]

FILE_PATH = None
FILE_NAME = None

for file in POSSIBLE_FILES:
    path = os.path.join(FOLDER_PATH, file)
    if os.path.exists(path):
        FILE_PATH = path
        FILE_NAME = file
        break


def clean(value):
    if pd.isna(value):
        return ""

    value = str(value).replace("\n", " ").strip()
    value = re.sub(r"\s+", " ", value)

    if value.lower() in ["nan", "none", "null", "-", "--", ""]:
        return ""

    return value


def filename_to_part_name(file_name):
    name = os.path.splitext(file_name)[0]
    name = re.sub(r"\(\d+\)$", "", name).strip()
    return name


def find_right_value(df, keywords, max_rows=25):
    for r in range(min(max_rows, len(df))):
        for c in range(len(df.columns)):
            cell = clean(df.iloc[r, c]).upper()

            if any(k.upper() in cell for k in keywords):
                for offset in range(1, 10):
                    if c + offset < len(df.columns):
                        val = clean(df.iloc[r, c + offset])
                        if val:
                            return val
    return ""


def is_valid_process_no(value):
    value = clean(value)
    if not value:
        return False

    try:
        float(value)
        return True
    except ValueError:
        return False


def delete_old_l2_l3_data(part_obj):
    l2_table = L2_ProcessReportMaster._meta.db_table
    l3_table = L3_ParameterDetailMaster._meta.db_table

    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            DELETE FROM "{l3_table}"
            WHERE process_report_id IN (
                SELECT id FROM "{l2_table}"
                WHERE part_info_id = %s
            )
            """,
            [part_obj.id],
        )

        cursor.execute(
            f"""
            DELETE FROM "{l2_table}"
            WHERE part_info_id = %s
            """,
            [part_obj.id],
        )


def upload_excel():
    print("FILE PATH:", FILE_PATH)
    print("FILE EXISTS:", FILE_PATH is not None and os.path.exists(FILE_PATH))

    if not FILE_PATH:
        print("File not found. Check exact file name.")
        return

    xls = pd.ExcelFile(FILE_PATH)
    print("SHEETS:", xls.sheet_names)

    sheet_name = xls.sheet_names[0]
    print("SHEET USED:", sheet_name)

    df = pd.read_excel(FILE_PATH, sheet_name=sheet_name, header=None)

    customer_name = "GESTAMP"
    part_name = filename_to_part_name(FILE_NAME)

    part_no = find_right_value(df, ["PART NO", "PART NO.", "PART NO / REV NO"]) or "UNKNOWN"
    model_name = find_right_value(df, ["MODEL", "MODEL NAME"]) or "UNKNOWN"

    part_obj, _ = L1_PartInfoMaster.objects.update_or_create(
        customer_name=customer_name,
        part_name=part_name,
        part_no=part_no,
        defaults={"model_name": model_name},
    )

    delete_old_l2_l3_data(part_obj)

    current_process = None
    l2_count = 0
    l3_count = 0
    storage_count = 0

    skip_words = [
        "CONTROL PLAN",
        "PART/PROCESS NO",
        "PROCESS NAME",
        "OPERATION DESCRIPTION",
        "MACHINE/JIG/FIXTURE",
        "CHARACTERISTICS",
        "PRODUCT/ PROCESS SPECIFICATION",
        "EVALUATION",
        "INSP. TECHNIQUE",
        "CHECKING METHOD",
        "SAMPLE",
        "METHOD",
        "REACTION PLAN",
        "DOC NO",
        "REVISION NO",
        "PAGE NO",
        "PREPARED BY",
        "APPROVED BY",
        "CUSTOMER",
        "CORE TEAM",
        "SUPPLER NAME",
        "SUPPLIER NAME",
    ]

    for i in range(len(df)):
        process_no = clean(df.iloc[i, 1]) if df.shape[1] > 1 else ""
        operation_desc = clean(df.iloc[i, 2]) if df.shape[1] > 2 else ""
        machine = clean(df.iloc[i, 3]) if df.shape[1] > 3 else ""

        parameter_no = clean(df.iloc[i, 4]) if df.shape[1] > 4 else ""
        process_char = clean(df.iloc[i, 5]) if df.shape[1] > 5 else ""
        product_char = clean(df.iloc[i, 6]) if df.shape[1] > 6 else ""
        special_char = clean(df.iloc[i, 7]) if df.shape[1] > 7 else ""
        specification = clean(df.iloc[i, 8]) if df.shape[1] > 8 else ""
        instrument = clean(df.iloc[i, 9]) if df.shape[1] > 9 else ""

        joined = " ".join([
            process_no,
            operation_desc,
            machine,
            parameter_no,
            process_char,
            product_char,
            special_char,
            specification,
            instrument,
        ]).upper()

        if any(word in joined for word in skip_words):
            continue

        stop_after_this_row = False

        if is_valid_process_no(process_no) and operation_desc:
            report_name = operation_desc.strip()

            if report_name.upper() == "STORAGE":
                storage_count += 1

                if storage_count == 2:
                    report_name = "STORAGE TWO"
                elif storage_count == 3:
                    report_name = "STORAGE THREE"

            current_process = L2_ProcessReportMaster.objects.create(
                part_info=part_obj,
                report_name=report_name,
            )

            l2_count += 1

            if "DISPATCH" in report_name.upper() or "DESPATCH" in report_name.upper():
                stop_after_this_row = True

        if not current_process:
            continue

        category = ""
        parameter_name = ""

        if product_char:
            category = "PRODUCT"
            parameter_name = product_char
        elif process_char:
            category = "PROCESS"
            parameter_name = process_char
        elif special_char:
            category = "SPECIAL"
            parameter_name = special_char

       
        if parameter_name or specification or instrument:
            L3_ParameterDetailMaster.objects.create(
                process_report=current_process,
                category=category,
                parameter_name=parameter_name,
                specification=specification,
                instrument=instrument,
            )
            l3_count += 1

        if stop_after_this_row:
            break

    print("================================")
    print("UPLOAD COMPLETED SUCCESSFULLY")
    print("File:", FILE_NAME)
    print("Part Name:", part_name)
    print("Part No:", part_no)
    print("Model:", model_name)
    print("L1 ID:", part_obj.id)
    print("Old L2/L3 deleted and new data inserted")
    print("L2 inserted:", l2_count)
    print("L3 inserted:", l3_count)
    print("================================")


if __name__ == "__main__":
    upload_excel()