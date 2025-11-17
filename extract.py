import pandas as pd
import tabula
import numpy as np
import re
import os
import datetime

# --- 1. CONFIGURATION & SETUP ---

# --- Folders ---
base_folder = "Tariff" 
output_file = "all_chapters_extracted.csv"
log_file = "extraction_log.txt"

# --- PDF Column Mapping ---
COLUMNS_TO_MAP = {
    0: 'hdg_4d',
    1: 'sub_hdg_6d',
    3: 'description_raw'
}

# *** NEW: Get a single timestamp for this entire extraction run ***
run_timestamp = pd.Timestamp.now()

# --- 2. VERIFY FOLDERS ---
if not os.path.isdir(base_folder):
    print(f"ERROR: The base folder '{base_folder}' was not found.")
    print("Please create this folder and place your 'Section I', 'Section II', etc. folders inside it.")
    exit()

print(f"--- Starting batch extraction from '{base_folder}' ---")
print(f"Run Time: {run_timestamp}")

# --- 3. BATCH PDF EXTRACTION (FINALIZED) ---
all_dataframes = []
success_log = []
error_log = []

for root, dirs, files in os.walk(base_folder):
    if root == base_folder:
        continue
    
    section_name = os.path.basename(root)
    
    if 'preamble' in section_name.lower():
        print(f"\n--- Skipping Folder: {section_name} (Preamble) ---")
        continue
    
    print(f"\n--- Scanning Folder: {section_name} ---")

    # *** NEW: Clean the section name ***
    clean_section_name = section_name # Default
    match = re.search(r'(Section [IVXLCDM]+)', section_name, re.IGNORECASE)
    if match:
        clean_section_name = match.group(1).title() # "Section I"

    for pdf_file in files:
        if not ('chapter' in pdf_file.lower() and pdf_file.lower().endswith('.pdf')):
            continue
            
        current_file_path = os.path.join(root, pdf_file)
        current_file_name = f"{section_name}/{pdf_file}"
        print(f"  --- Processing: {current_file_name} ---")

        try:
            tables = tabula.read_pdf(
                current_file_path,
                pages='all',
                lattice=True,
                pandas_options={'dtype': str, 'header': None}
            )
            
            if not tables:
                raise Exception("No tables were found in the PDF (File might be empty, e.g., Ch. 77).")
            
            raw_df = pd.concat(tables, ignore_index=True)

            # --- C. Filtering (Core Logic) ---
            pattern_4d_flexible = r'^\d{2}\.\d{2}(\n)?$'
            pattern_6d_flexible = r'^\d{4}\.[0-9.]{2,5}(\n)?$'
            mask_4d = raw_df[0].str.match(pattern_4d_flexible).fillna(False)
            mask_6d = raw_df[1].str.match(pattern_6d_flexible).fillna(False)
            df_filtered = raw_df[mask_4d | mask_6d].copy()

            if df_filtered.empty:
                raise Exception("No valid HS code data rows found after filtering.")

            # --- D. Transformation (Core Logic) ---
            df_renamed = pd.DataFrame()
            df_renamed['hdg_4d'] = df_filtered.get(0)
            df_renamed['sub_hdg_6d'] = df_filtered.get(1)
            df_renamed['description_raw'] = df_filtered.get(3)
            df_renamed['hscode'] = df_renamed['sub_hdg_6d'].fillna(df_renamed['hdg_4d'])
            df_renamed['hscode_clean'] = df_renamed['hscode'].str.replace(r'\.', '', regex=True).str.replace(r'\n', '', regex=False).str.strip()
            df_renamed['description'] = df_renamed['description_raw'].str.strip().str.replace(r'\n', ' ', regex=False).str.replace(r'^[-\s]+', '', regex=True).str.replace(r'\(\+\)$', '', regex=True).str.replace(r'\(\+\)\s*$', '', regex=True)
            df_renamed['level'] = df_renamed['hscode_clean'].str.len()
            
            # Correct Parent Logic
            conditions = [(df_renamed['level'] == 8), (df_renamed['level'] == 6), (df_renamed['level'] == 4)]
            choices = [df_renamed['hscode_clean'].str[:6], df_renamed['hscode_clean'].str[:4], df_renamed['hscode_clean'].str[:2]]
            df_renamed['parent'] = np.select(conditions, choices, default='TOTAL')
            
            # *** USE CLEAN SECTION NAME ***
            df_renamed['section'] = clean_section_name 
            
            # *** ADD TIMESTAMP COLUMN ***
            df_renamed['datetime_extracted'] = run_timestamp

            # --- E. Finalization (Per-File) ---
            # *** ADDED 'datetime_extracted' TO FINAL LIST ***
            final_df = df_renamed[[
                'section',
                'hscode',
                'description',
                'parent',
                'level',
                'datetime_extracted'
            ]].rename(columns={'hscode_clean': 'hscode'})
            
            all_dataframes.append(final_df)
            success_log.append(f"{current_file_name} ({len(final_df)} rows)")
            print(f"  Successfully processed and staged {len(final_df)} rows.")

        except Exception as e:
            print(f"  ERROR: Failed to process file. Details: {e}")
            error_log.append(f"{current_file_name} - ERROR: {e}")

# --- 4. COMBINE, SAVE, AND WRITE LOG ---

if not all_dataframes:
    print("\n--- EXTRACTION FAILED ---")
    print("No data was successfully extracted from any PDF.")
else:
    print("\n--- EXTRACTION COMPLETE ---")
    print("Combining all extracted data...")
    master_df = pd.concat(all_dataframes, ignore_index=True)
    master_df = master_df.sort_values(by='hscode').reset_index(drop=True)
    master_df.to_csv(output_file, index=False)
    
    print(f"\nSuccessfully saved {len(master_df)} total rows to {output_file}")
    # *** REMOVED print(head()) ***

# --- B. Write Log File ---
print(f"\nWriting log to {log_file}...")
with open(log_file, 'w') as f:
    f.write(f"--- EXTRACTION LOG ---\n")
    f.write(f"Run finished on: {run_timestamp}\n\n") # *** ADDED TIMESTAMP ***
    f.write(f"Successfully processed: {len(success_log)}\n")
    f.write(f"Failed to process: {len(error_log)}\n")
    
    f.write("\n--- SUCCESSFUL FILES ---\n")
    if not success_log: f.write("None\n")
    else:
        for log_entry in success_log: f.write(f"- {log_entry}\n")
            
    f.write("\n--- FAILED FILES ---\n")
    if not error_log: f.write("None\n")
    else:
        for log_entry in error_log: f.write(f"- {log_entry}\n")

print("--- All tasks complete. ---")