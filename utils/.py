import pandas as pd
import openai
import os
import json
from tqdm import tqdm
from pypdf import PdfReader
import pyodbc
from getpass import getpass

# --- 1. Configuration Section ---
# --- Database & Table Configuration ---
DB_TABLE_SOURCE = 'Item'
DB_TABLE_DESTINATION = 'ItemAi'
DB_PRIMARY_KEY = 'ItemCode'

# --- Source & Target Column Configuration ---
SOURCE_DESCRIPTION_COLUMN = 'Description'
SOURCE_BRAND_COLUMN = 'Brand'
TARGET_COLUMNS = [
    'AiBrand', 'AiCompany', 'AiDepartment', 'AiCategory', 'AiSubCategory', 
    'AiCoreCategory', 'AiVariant', 'AiWeightPcs', 'AiUom', 'AiDescription', 'AiShortDescription'
]

# --- Script & API Configuration ---
BATCH_SIZE = 10
OPENAI_MODEL = "gpt-4o-mini" 
PDF_GUIDE_FILE = 'categories explained.pdf'

# --- 2. Database Manager Class ---
class DatabaseManager:
    def __init__(self, server, database, username, password):
        self.conn_str = (
            f'DRIVER={{ODBC Driver 17 for SQL Server}};'
            f'SERVER={sto,8070};'
            f'DATABASE={ConsolidatedDb};'
            f'UID={sa};'
            f'PWD={Ipos9898}'
        )
        self.cnxn = None
        self.cursor = None

    def connect(self):
        try:
            self.cnxn = pyodbc.connect(self.conn_str)
            self.cursor = self.cnxn.cursor()
            print("✅ Database connection successful.")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            exit()

    def setup_destination_table(self):
        try:
            self.cursor.execute(f"SELECT 1 FROM sys.tables WHERE Name = N'{DB_TABLE_DESTINATION}'")
            if not self.cursor.fetchone():
                print(f"Creating destination table '{DB_TABLE_DESTINATION}'...")
                ai_cols_sql = " NVARCHAR(255), ".join(TARGET_COLUMNS) + " NVARCHAR(255)"
                create_sql = (
                    f"CREATE TABLE {DB_TABLE_DESTINATION} ("
                    f"{DB_PRIMARY_KEY} NVARCHAR(50) PRIMARY KEY, "
                    f"OriginalDescription_Consolidated NVARCHAR(MAX), "
                    f"{ai_cols_sql})"
                )
                self.cursor.execute(create_sql)
                self.cnxn.commit()
        except Exception as e:
            print(f"Could not create destination table: {e}")

    def fetch_unprocessed_items(self):
        # This query intelligently fetches unprocessed items, consolidating descriptions
        query = f"""
            WITH UnprocessedItems AS (
                SELECT 
                    i.ItemCode,
                    i.Description,
                    MAX(i.Brand) as Brand -- Assumes Brand is consistent for the same ItemCode
                FROM {DB_TABLE_SOURCE} i
                WHERE LEN(i.ItemCode) >= 8 AND NOT EXISTS (
                    SELECT 1 FROM {DB_TABLE_DESTINATION} aid
                    WHERE aid.{DB_PRIMARY_KEY} = i.ItemCode
                )
                GROUP BY i.ItemCode, i.Description
            )
            , ConsolidatedDescriptions AS (
                SELECT
                    ItemCode,
                    Brand,
                    STRING_AGG(Description, ' | ') AS ConsolidatedDescription
                FROM UnprocessedItems
                GROUP BY ItemCode, Brand
            )
            SELECT TOP {BATCH_SIZE} * FROM ConsolidatedDescriptions
        """
        return pd.read_sql(query, self.cnxn)

    def write_cleaned_data(self, df_results):
        for index, row in df_results.iterrows():
            item_code = row[DB_PRIMARY_KEY]
            # Use MERGE for a robust insert/update operation
            merge_sql = f"""
            MERGE {DB_TABLE_DESTINATION} AS target
            USING (SELECT ? AS {DB_PRIMARY_KEY}) AS source
            ON (target.{DB_PRIMARY_KEY} = source.{DB_PRIMARY_KEY})
            WHEN MATCHED THEN
                UPDATE SET {', '.join([f'{col} = ?' for col in TARGET_COLUMNS])}
            WHEN NOT MATCHED THEN
                INSERT ({DB_PRIMARY_KEY}, OriginalDescription_Consolidated, {', '.join(TARGET_COLUMNS)})
                VALUES (?, ?, {', '.join(['?'] * len(TARGET_COLUMNS))});
            """
            
            update_values = [row.get(c) for c in TARGET_COLUMNS]
            insert_values = [item_code, row.get('OriginalDescription_Consolidated')] + update_values
            
            params = update_values + insert_values
            self.cursor.execute(merge_sql, params)
        self.cnxn.commit()

    def close(self):
        if self.cursor: self.cursor.close()
        if self.cnxn: self.cnxn.close(); print("Database connection closed.")

# --- 3. AI Processor Class ---
class AIProcessor:
    def __init__(self, api_key, category_guide):
        openai.api_key = api_key
        self.category_guide = category_guide
        self.dynamic_examples = []

    def add_dynamic_example(self, original_desc, corrected_json):
        self.dynamic_examples.extend([
            {"role": "user", "content": f"<product_data><description>{original_desc}</description></product_data>"},
            {"role": "assistant", "content": json.dumps(corrected_json)}
        ])
        print("🧠 AI training example added for this session.")

    def create_prompt(self, brand, description):
        system_prompt = "You are a highly structured data extraction engine..."
        user_prompt = (
            "<task_instructions>...\n"
            "<rules>\n"
            "  1.  **CRITICAL CLASSIFICATION RULE:** For 'department', 'category', 'subcategory', and 'core_category', you MUST select values **directly and exclusively** from the provided `<guide>`.\n"
            "  2.  **Company vs. Brand:** Identify the parent 'company' and the specific product 'brand'.\n"
            "  3.  **Independent Brands Rule:** If a brand is not a well-known subsidiary of a major corporation, the 'company' field should be the **same as the 'brand' field**.\n"
            "  4.  **Weight/UOM Conversion:** Convert 'g'/'gm' to 'kg' and 'ml' to 'litre'. For 'pc'/'pcs', the UOM is 'each'.\n"
            "  5.  **Variant:** Identify any flavor, color, or type. If none, return an empty string.\n"
            "  6.  **AiDescription:** Create a detailed, clean description.\n"
            "  7.  **AiShortDescription:** Create a simple description using the pattern `[Brand] [Generic Category] [Size]`.\n"
            "</rules>\n\n"
            "<guide>\n"
            f"{self.category_guide}\n"
            "</guide>\n\n"
            "<product_data>\n"
            f"  <brand>{brand}</brand>\n"
            f"  <description>{description}</description>\n"
            "</product_data>\n\n"
            "Generate the complete JSON object now."
        )
        return system_prompt, user_prompt

    def process_data_row(self, brand, description):
        if not description or pd.isna(description): return None
        try:
            system_prompt, user_prompt = self.create_prompt(brand, description)
            messages = [{"role": "system", "content": system_prompt}] + self.dynamic_examples + [{"role": "user", "content": user_prompt}]
            
            tqdm.write(f"Sending request for '{description[:60]}...'")
            response = openai.chat.completions.create(model=OPENAI_MODEL, messages=messages, response_format={"type": "json_object"}, temperature=0.1)
            
            result_json = response.choices[0].message.content
            tqdm.write(f"  -> Response received: {result_json}")
            return json.loads(result_json)
        except Exception as e:
            tqdm.write(f"An error occurred for '{str(description)[:50]}...': {e}")
            return None

# --- 4. Main Interactive Cleaner Class ---
class InteractiveDataCleaner:
    def __init__(self, db_manager, ai_processor):
        self.db = db_manager
        self.ai = ai_processor

    def run(self):
        while True:
            batch_df = self.db.fetch_unprocessed_items()
            if batch_df.empty:
                print("🎉 All items have been processed. Exiting.")
                break

            results = []
            for index, row in batch_df.iterrows():
                ai_result = self.ai.process_data_row(row['Brand'], row['ConsolidatedDescription'])
                
                result_row = {DB_PRIMARY_KEY: row[DB_PRIMARY_KEY], 'OriginalDescription_Consolidated': row['ConsolidatedDescription']}
                if ai_result:
                    result_row.update({
                        'AiBrand': ai_result.get('brand'), 'AiCompany': ai_result.get('company'),
                        'AiDepartment': ai_result.get('department'), 'AiCategory': ai_result.get('category'),
                        'AiSubCategory': ai_result.get('subcategory'), 'AiCoreCategory': ai_result.get('core_category'),
                        'AiVariant': ai_result.get('variant'), 'AiWeightPcs': ai_result.get('weight_pcs'),
                        'AiUom': ai_result.get('uom'), 'AiDescription': ai_result.get('new_description'),
                        'AiShortDescription': ai_result.get('short_description')
                    })
                results.append(result_row)
            
            results_df = pd.DataFrame(results)
            
            while True:
                print("\n--- AI Processing Results ---")
                display_cols = [DB_PRIMARY_KEY] + TARGET_COLUMNS
                print(results_df[display_cols].to_string())
                
                action = input("\nApprove batch (y), Correct an item (c), or Stop (s): ").lower()
                if action == 'y':
                    self.db.write_cleaned_data(results_df)
                    print(f"✅ Batch of {len(results_df)} items saved.")
                    break
                elif action == 's':
                    print("Stopping script.")
                    return
                elif action == 'c':
                    self.handle_correction(results_df)
                else:
                    print("Invalid input.")

    def handle_correction(self, results_df):
        try:
            item_to_correct = input("Enter the ItemCode of the row to correct: ")
            row_index = results_df.index[results_df[DB_PRIMARY_KEY] == item_to_correct].tolist()[0]
            
            original_description = results_df.at[row_index, 'OriginalDescription_Consolidated']
            corrected_data = {}

            print("\nEnter the correct value for each field. Press Enter to accept the current value.")
            for col in TARGET_COLUMNS:
                current_val = results_df.at[row_index, col]
                new_val = input(f"  {col} (current: {current_val}): ")
                corrected_data[col] = new_val if new_val else current_val
                results_df.at[row_index, col] = corrected_data[col]
            
            ai_training_json = {k.replace('Ai', '').lower(): v for k, v in corrected_data.items()}
            ai_training_json['new_description'] = corrected_data['AiDescription']
            ai_training_json['short_description'] = corrected_data['AiShortDescription']
            
            self.ai.add_dynamic_example(original_description, ai_training_json)
        except (IndexError, ValueError) as e:
            print(f"Invalid ItemCode or input. Error: {e}")

# --- Main Execution Block ---
if __name__ == "__main__":
    print("--- AI-Powered SQL Data Cleaner ---")
    
    server = input("Enter SQL Server name: ")
    database = input("Enter Database name: ")
    username = input("Enter SQL username: ")
    password = getpass("Enter SQL password: ")

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        api_key = getpass("OpenAI API Key not found. Please paste it here: ")

    db = DatabaseManager(server, database, username, password)
    db.connect()
    db.setup_destination_table()

    category_guide = get_category_guide_from_pdf(PDF_GUIDE_FILE)
    if not category_guide:
        print(f"Could not load category guide.")
        db.close()
        exit()

    ai = AIProcessor(api_key, category_guide)
    
    cleaner = InteractiveDataCleaner(db, ai)
    try:
        cleaner.run()
    finally:
        db.close()