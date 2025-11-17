"""
HS Code Semantic Search (V20 - THE FINAL FIX)
================================================

This is the logical conclusion of our V1-V19 progress.
It keeps our simple, single-file Flask app structure.
It keeps our "V-FINAL" logic (using multi_search with a dict).

IT FIXES THE BUG by "stealing" the one working piece of code:
We are replacing the broken `client.multi_search.perform()`
with the low-level `client.api_call.post()` and the
magic `as_json=True` flag that makes it work.
"""

from flask import Flask, request, jsonify, render_template_string
from sentence_transformers import SentenceTransformer
import typesense
import json 
import sys

print("--- Initializing Search App (V20 - THE FIX) ---")

print("Loading S-BERT model 'all-MiniLM-L6-v2'...")
model = SentenceTransformer('all-MiniLM-L6-v2')
print("Model loaded.")

print("Connecting to Typesense server (v29.0)...")
try:
    client = typesense.Client({
        'nodes': [{'host': 'localhost', 'port': '8108', 'protocol': 'http'}],
        'api_key': 'xyz',
        'connection_timeout_seconds': 5
    })
    # We MUST upgrade the typesense library to get .api_call
    if not hasattr(client, 'api_call'):
        print("--- FATAL ERROR ---")
        print("Your typesense-python library is too old and buggy.")
        print("Please run: pip install --upgrade typesense")
        sys.exit(1)
        
    client.debug.retrieve() 
    print("Typesense connection successful.")
except Exception as e:
    print(f"--- FATAL ERROR: Could not connect to Typesense server: {e} ---")
    sys.exit(1)

COLLECTION_NAME = 'hs_codes'
app = Flask(__name__)

@app.route('/search', methods=['GET'])
def search():
    query = request.args.get('q', '') 
    if not query:
        return jsonify({"error": "Query parameter 'q' is required"}), 400

    print(f"\n--- NEW SEARCH (V20) ---")
    print(f"Received query: '{query}'")

    print("1. Generating query vector...")
    query_vector = model.encode(query).tolist()

    # This is the correct payload (vector_query as a dictionary)
    # This is what V11.0 tried to do.
    search_operation = {
        'collection': COLLECTION_NAME,
        'q': query,
        'query_by': 'description, hscode',
        'vector_query': {
            'vector': query_vector,
            'k': 10,
            'query_field': 'embedding'
        },
        'per_page': 10,
    }

    multi_search_payload = {
        'searches': [search_operation]
    }

    print("3. Sending request via LOW-LEVEL client.api_call.post()...")
    try:
        #
        # *** THIS IS THE "GOLDEN KEY" STOLEN FROM THE NEW FILES ***
        # This bypasses the broken .multi_search.perform()
        # and correctly sends our dict payload as JSON.
        # This fixes the "Catch-22" for good.
        #
        results = client.api_call.post(
            endpoint="/multi_search",
            body=multi_search_payload,
            as_json=True,  # This forces correct JSON serialization
            entity_type=dict # This tells it what to expect back
        )
        
        search_result = results['results'][0]
        
        print("4. Got response. Returning results.")
        return jsonify(search_result)
        
    except Exception as e:
        print(f"Error during search: {e}") 
        return jsonify({"error": str(e)}), 500

@app.route('/', methods=['GET'])
def home():
    html_template = """
    <!DOCTYPE html>
    <html>
    <head><title>HS Code Semantic Search</title>
        <style>
            body { font-family: sans-serif; margin: 40px; background: #f4f4f4; }
            h1 { color: #333; }
            input[type=text] { width: 400px; padding: 10px; font-size: 16px; }
            input[type=submit] { padding: 10px 20px; font-size: 16px; background: #08f; color: white; border: none; cursor: pointer; }
            input[type=submit]:disabled { background: #999; cursor: not-allowed; }
            #results { margin-top: 20px; }
            pre { background: #fff; padding: 15px; border-radius: 5px; border: 1px solid #ddd; }
        </style>
    </head>
    <body>
        <h1>HS Code Semantic Search (V20 - THE FIX)</h1>
        <form id="search-form">
            <input type="text" id="q" name="q" placeholder="Try 'dilmah' or 'tea' or 'smartphone'...">
            <input type="submit" value="Search" id="search-button">
        </form>
        <div id="results"></div>
        <script>
            document.getElementById('search-form').addEventListener('submit', function(e) {
                e.preventDefault();
                let query = document.getElementById('q').value;
                let resultsDiv = document.getElementById('results');
                let submitButton = document.getElementById('search-button');
                resultsDiv.innerHTML = '<p>Loading...</p>';
                submitButton.disabled = true;
                
                fetch('/search?q=' + encodeURIComponent(query))
                    .then(response => response.json())
                    .then(data => {
                        resultsDiv.innerHTML = '<h2>Results</h2><pre>' + JSON.stringify(data, null, 2) + '</pre>';
                    })
                    .catch(error => {
                        resultsDiv.innerHTML = '<p>Error: ' + error + '</p>';
                    })
                    .finally(() => {
                        submitButton.disabled = false;
                    });
            });
        </script>
    </body>
    </html>
    """
    return render_template_string(html_template)

if __name__ == '__main__':
    print("Starting Flask web server on http://localhost:5000")
    print("Go to http://localhost:5000 in your browser to search.")
    app.run(debug=True, port=5000, use_reloader=True)