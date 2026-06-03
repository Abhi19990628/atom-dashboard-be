import os
import django
import pandas as pd
import numpy as np

# ==========================================
# 1. DJANGO ENVIRONMENT SETUP
# ==========================================
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "operator_app.settings") 
django.setup()

from api.models import L1_PartInfoMaster, L2_ProcessReportMaster, L3_ParameterDetailMaster

def aggressive_search(df, search_terms):
    """Poore excel sheet mein ghum kar exact value nikalega"""
    for r in range(min(25, len(df))):  # Top 25 rows tak scan karega
        for c in range(len(df.columns) - 1):
            cell_val = str(df.iloc[r, c]).strip().upper()
            
            if any(term in cell_val for term in search_terms):
                for offset in range(1, 5):
                    if c + offset < len(df.columns):
                        val = str(df.iloc[r, c + offset]).strip()
                        if val and val.lower() not in ['nan', 'none', '-', ':', '']:
                            return val
                
                if r + 1 < len(df):
                    val_below = str(df.iloc[r+1, c]).strip()
                    if val_below and val_below.lower() not in ['nan', 'none', '-', ':', '']:
                        return val_below
    return "Unknown"

def import_master_data(file_path, sheet_name):
    print(f"\n=====================================================")
    print(f"📂 Loading File: {os.path.basename(file_path)} | Sheet: {sheet_name}")
    
    try:
        df_raw = pd.read_excel(file_path, sheet_name=sheet_name, header=None)
    except Exception as e:
        print(f"❌ ERROR: File ya Sheet padhne mein dikkat aayi: {e}")
        return
    
    # ==========================================
    # 2. EXTRACT L1 (PART INFO MASTER)
    # ==========================================
    part_name = aggressive_search(df_raw, ["PART NAME"])
    customer = aggressive_search(df_raw, ["CUSTOMER"])
    part_number = aggressive_search(df_raw, ["PART NO.", "PART NO"])
    model_name = aggressive_search(df_raw, ["MODEL"])
    
    print(f"✅ L1 Data: Customer: {customer} | Part: {part_name} | Model: {model_name} | Part No: {part_number}")
    
    if part_name == "Unknown" and part_number == "Unknown":
        print("⚠️ Warning: Is sheet mein Part Name nahi mila, isko skip kar rahe hain.")
        return

    part_obj, created = L1_PartInfoMaster.objects.get_or_create(
        customer_name=customer,
        part_name=part_name,
        part_no=part_number,
        defaults={'model_name': model_name}
    )

    # ==========================================
    # 3. STRICT COLUMN MAPPING FOR L2 & L3
    # ==========================================
    col_idx = {
        'process_name': -1, 'sr_no': -1, 'product_char': -1, 
        'process_char': -1, 'spec': -1, 'instrument': -1
    }
    
    data_start_row = 10 

    for r in range(5, 15):
        row_vals = [str(x).strip().upper() for x in df_raw.iloc[r, :]]
        
        if any("SPECIFICATION" in x for x in row_vals) or any("TOLERANCE" in x for x in row_vals):
            data_start_row = r + 1 
            
            for c in range(len(df_raw.columns)):
                val = str(df_raw.iloc[r, c]).strip().upper()
                val_above = str(df_raw.iloc[r-1, c]).strip().upper() if r > 0 else ""
                combined = val + " " + val_above 
                
                # 🔥 FIX: Lock System - Agar pehle mil gaya toh doobara overwrite nahi hoga 🔥
                # 🔥 FIX: Instrument me se "METHOD" word nikal diya hai taaki Control Method pick na kare 🔥
                if col_idx['process_name'] == -1 and ("PROCESS NAME" in combined or "OPERATION DESCRIPTION" in combined):
                    col_idx['process_name'] = c
                    
                elif col_idx['spec'] == -1 and ("SPECIFICATION" in combined or "TOLERANCE" in combined):
                    col_idx['spec'] = c
                    
                elif col_idx['instrument'] == -1 and ("EVALUATION" in combined or "TECHNIQUE" in combined or "MEASUREMENT" in combined):
                    col_idx['instrument'] = c
                    
                elif col_idx['product_char'] == -1 and ("PRODUCT" in combined and "SPEC" not in combined):
                    col_idx['product_char'] = c
                    
                elif col_idx['process_char'] == -1 and ("PROCESS" in combined and "SPEC" not in combined and "NAME" not in combined):
                    col_idx['process_char'] = c
                    
                elif col_idx['sr_no'] == -1 and (val == "NO." or val == "NO" or "CHARACTERISTICS NO" in combined):
                    col_idx['sr_no'] = c
            break

    print(f"🔍 Mapped Columns -> Process Name: Col {col_idx['process_name']}, Spec: Col {col_idx['spec']}, Instrument: Col {col_idx['instrument']}")

    # ==========================================
    # 4. EXTRACT DATA & SAVE TO DB
    # ==========================================
    df_data = df_raw.iloc[data_start_row:].copy()
    
    if col_idx['process_name'] != -1:
        df_data.iloc[:, col_idx['process_name']] = df_data.iloc[:, col_idx['process_name']].replace(['', 'nan', 'NaN'], np.nan).ffill()

    print("⏳ Extracting Process and Parameters...")
    
    L2_ProcessReportMaster.objects.filter(part_info=part_obj).delete()

    count_l2 = 0
    count_l3 = 0

    for index, row in df_data.iterrows():
        process_name = str(row.iloc[col_idx['process_name']]).strip() if col_idx['process_name'] != -1 and pd.notna(row.iloc[col_idx['process_name']]) else ""
        process_char = str(row.iloc[col_idx['process_char']]).strip() if col_idx['process_char'] != -1 and pd.notna(row.iloc[col_idx['process_char']]) else ""
        product_char = str(row.iloc[col_idx['product_char']]).strip() if col_idx['product_char'] != -1 and pd.notna(row.iloc[col_idx['product_char']]) else ""
        spec = str(row.iloc[col_idx['spec']]).strip() if col_idx['spec'] != -1 and pd.notna(row.iloc[col_idx['spec']]) else ""
        instrument = str(row.iloc[col_idx['instrument']]).strip() if col_idx['instrument'] != -1 and pd.notna(row.iloc[col_idx['instrument']]) else ""

        if not process_name or process_name.lower() in ['nan', 'none', '']:
            continue
        if process_char.lower() in ['', '-', 'nan'] and product_char.lower() in ['', '-', 'nan']:
            continue
        if "REV. NO" in process_name.upper() or "PREPARED BY" in process_name.upper():
            break 

        process_obj, p_created = L2_ProcessReportMaster.objects.get_or_create(
            part_info=part_obj,
            report_name=process_name
        )
        if p_created:
            count_l2 += 1

        param_name = ""
        category = ""
        
        if process_char and process_char.lower() not in ['-', 'nan', '']:
            category = 'PROCESS'
            param_name = process_char
        elif product_char and product_char.lower() not in ['-', 'nan', '']:
            category = 'PRODUCT'
            param_name = product_char
        else:
            continue 

        # Prevent 'nan' from being saved as string
        spec = "" if spec.lower() == 'nan' else spec
        instrument = "" if instrument.lower() == 'nan' else instrument

        if spec or instrument:
            L3_ParameterDetailMaster.objects.create(
                process_report=process_obj,
                category=category,
                parameter_name=param_name,
                specification=spec,
                instrument=instrument
            )
            count_l3 += 1

    print(f"🎉 {count_l2} Processes & {count_l3} Parameters Saved for {part_name}")
    print("=====================================================\n")

if __name__ == "__main__":
 
    excel_file = r"C:\control plan\TENNECO CP\6. SENSOR BRACKET.xlsx"
    
    try:
        # Excel file open karke saari sheets ke naam nikalega
        xls = pd.ExcelFile(excel_file)
        sheet_names = xls.sheet_names
        print(f"✅ File Mili! Isme {len(sheet_names)} sheets hain: {sheet_names}")
        
        # Ek-ek karke saari sheets ka data upload karega
        for sheet in sheet_names:
            import_master_data(excel_file, sheet_name=sheet)
            
    except FileNotFoundError:
        print(f"❌ ERROR: File nahi mili! Kripya path check karein: {excel_file}")
    except Exception as e:
        print(f"❌ ERROR: {e}")