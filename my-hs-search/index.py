import pandas as pd
import typesense
from sentence_transformers import SentenceTransformer
import re
import sys
import json # <-- We need this
import os   # <-- We need this

# --- 1. CONFIGURATION & SETUP ---
CSV_FILE = 'all_chapters_extracted.csv'
JSONL_FILE = 'hs_codes.jsonl' # <-- Our new output file
COLLECTION_NAME = 'hs_codes'
MODEL_NAME = 'all-MiniLM-L6-v2'
TYPESENSE_API_KEY = 'xyz'
TYPESENSE_HOST = 'localhost'
TYPESENSE_PORT = '8108'

# --- 2. DATA CLEANING (Unchanged) ---
print(f"Loading and cleaning {CSV_FILE}...")
try:
    df = pd.read_csv(CSV_FILE)
except FileNotFoundError:
    print(f"--- ERROR ---")
    print(f"File not found: {CSV_FILE}")
    sys.exit(1) 

def clean_text(text):
    if not isinstance(text, str): return ""
    text = text.replace('\r\n', ' ').replace('\n', ' ') 
    text = re.sub(r'\(\+\)\.?', '', text)             
    text = re.sub(r'[\.,;:]$', '', text.strip())      
    return text.strip()

df['description_cleaned'] = df['description'].apply(clean_text)
df = df.where(pd.notnull(df), None) 
df = df[df['hscode'].notnull()]
print(f"Loaded {len(df)} valid rows.")

# --- 3. EMBEDDING (Unchanged) ---
print(f"Loading S-BERT model '{MODEL_NAME}'...")
model = SentenceTransformer(MODEL_NAME)
print("Generating 384-dimension vectors...")
embeddings = model.encode(df['description_cleaned'].tolist(), show_progress_bar=True)
print(f"Generated {len(embeddings)} vectors.")

# --- 4. STAGE A: BUILD JSONL FILE ---
print(f"\n--- STAGE A: Building {JSONL_FILE} ---")
count = 0
try:
    with open(JSONL_FILE, 'w', encoding='utf-8') as f:
        for i, row in df.iterrows():
            embedding_vector = embeddings[i].tolist()
            
            doc = {
                'hscode': row['hscode'],
                'description': row['description'], 
                'parent': row['parent'],
                'level': int(row['level']), 
                'section': row['section'],
                'embedding': embedding_vector
            }
            
            # Write each document as a new line in the file
            f.write(json.dumps(doc) + '\n')
            count += 1
    print(f"Successfully wrote {count} documents to {JSONL_FILE}")

except Exception as e:
    print(f"--- ERROR Writing JSONL file ---")
    print(e)
    sys.exit(1)

# --- 5. STAGE B: IMPORT FROM JSONL ---
print(f"\n--- STAGE B: Importing to Typesense ---")

# --- 5a. Connect and setup schema ---
print("Connecting to Typesense server...")
try:
    client = typesense.Client({
        'nodes': [{'host': TYPESENSE_HOST, 'port': TYPESENSE_PORT, 'protocol': 'http'}],
        'api_key': TYPESENSE_API_KEY,
        'connection_timeout_seconds': 10
    })

    schema = {
        'name': COLLECTION_NAME,
        'fields': [
            {'name': 'hscode', 'type': 'string', 'facet': True},
            {'name': 'description', 'type': 'string'},
            {'name': 'parent', 'type': 'string', 'facet': True, 'optional': True},
            {'name': 'level', 'type': 'int32', 'facet': True},
            {'name': 'section', 'type': 'string', 'facet': True, 'optional': True},
            {'name': 'embedding', 'type': 'float[]', 'num_dim': 384}
        ]
    }

    print(f"Checking for existing collection '{COLLECTION_NAME}'...")
    try:
        client.collections[COLLECTION_NAME].delete()
        print("Deleted old collection.")
    except typesense.exceptions.ObjectNotFound:
        print("No old collection found.")
    
    print(f"Creating new collection: {COLLECTION_NAME}")
    client.collections.create(schema)

except Exception as e:
    print(f"--- ERROR during Typesense setup ---")
    print(e)
    sys.exit(1)

# --- 5b. Import the file ---
print(f"Importing data from '{JSONL_FILE}'... (This may take a minute)")
try:
    # This is the robust import method from the documentation
    # We pass the FILENAME, not a variable.
    import_params = {
        'batch_size': 100,
        'timeout_seconds': 120 # Give the import 2 minutes
    }
    
    # We must open the file and send it as bytes
    with open(JSONL_FILE, 'rb') as f_in:
        client.collections[COLLECTION_NAME].documents.import_(
            f_in.read(), 
            import_params
        )

    print("--- Indexing complete! ---")
    print(f"Successfully indexed {count} documents from {JSONL_FILE}.")

except Exception as e:
    print(f"--- ERROR DURING UPLOAD ---")
    print(e)
    print("Please check if your Typesense server is running.")