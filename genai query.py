import pandas as pd
import json
import os
from pymongo import MongoClient
from langchain_community.llms import LlamaCpp

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client["genai_db"]  # Database Name
collection = db["products"]  # Collection Name

# Load CSV into MongoDB
def load_csv_to_mongo(csv_file):
    if collection.count_documents({}) > 0:
        print("⚠️ Data already exists in MongoDB. Skipping import.")
        return
    
    df = pd.read_csv(csv_file)
    data = df.to_dict(orient="records")
    collection.insert_many(data)
    print(f"✅ {len(data)} records successfully loaded into MongoDB.")

# Load Llama Model
model = LlamaCpp(model_path="C:/Users/Kajal/OneDrive/Desktop/Projects/GIBOTS (Aiqod)/llama-2-7b-chat-hf-q4_k_m.gguf")

# Generate MongoDB Query Using LLM
def generate_mongo_query(user_input):
    prompt = f"""
    You are an AI assistant trained to generate MongoDB queries.
    ONLY return the MongoDB query in JSON format. Do NOT add explanations.

    ## Example Queries:

    **Example Input:** Find products where price is greater than 50  
    **Expected Output (JSON):**
    {{
        "Price": {{"$gt": 50}}
    }}

    **Example Input:** Find all products with rating 4.5 or higher  
    **Expected Output (JSON):**
    {{
        "Rating": {{"$gte": 4.5}}
    }}

    Now generate a MongoDB query for:
    "{user_input}"

    **Return JSON only! No extra text.**
    """

    print("⏳ Calling LLM to generate query...")  
    query = model.invoke(prompt).strip()
    print(f"🔍 LLM Debug Output: {query}")  

    try:
        query_json = json.loads(query)
        print(f"✅ Valid Query Generated: {query_json}")  
        return json.dumps(query_json)
    except json.JSONDecodeError:
        print("❌ Error: LLM output is not valid JSON!")
        return "{}"  

# Execute MongoDB Query
def execute_query(query):
    print(f"🔍 Final Query for MongoDB: {query}")  
    try:
        mongo_query = json.loads(query)
        print(f"✅ Query Sent to MongoDB: {mongo_query}")  
        results = list(collection.find(mongo_query, {"_id": 0}))
        print(f"🔹 MongoDB Results Found: {len(results)} records")
        return results
    except json.JSONDecodeError:
        print("❌ Error: Failed to parse JSON query!")
        return []
    except Exception as e:
        print("Error executing query:", str(e))
        return []

# Save Results to CSV
def save_results_to_csv(results, filename="output.csv"):
    if not results:
        print(f"⚠️ No results to save for {filename}. Skipping.")
        return

    df = pd.DataFrame(results)
    df.to_csv(filename, index=False)
    print(f"📂 Results saved to {filename}")

def main():
    # Load CSV to MongoDB
    csv_file = "sample_data.csv"
    load_csv_to_mongo(csv_file)

    # Define test queries
    user_queries = [
        "Find all products with a rating below 4.5 that have more than 200 reviews and are offered by the brand 'Nike' or 'Sony'.",
        "Which products in the Electronics category have a rating of 4.5 or higher and are in stock?",
        "List products launched after January 1, 2022, in the Home & Kitchen or Sports categories with a discount of 10% or more, sorted by price in descending order."
    ]

    # Save queries to text file
    with open("queries_generated.txt", "w") as f:
        for i, user_query in enumerate(user_queries, 1):
            print(f"\n🔹 Processing Query {i}: {user_query}")
            
            # Generate MongoDB Query using Llama 2
            generated_query = generate_mongo_query(user_query)
            if generated_query == "{}":
                print(f"⚠️ Query {i} failed! No valid MongoDB query generated.")
                continue
            
            f.write(f"Query {i}: {generated_query}\n")
            print(f"✅ Generated Query {i}: {generated_query}")

            # Execute MongoDB Query
            results = execute_query(generated_query)

            # Save results to CSV
            csv_filename = f"test_case{i}.csv"
            save_results_to_csv(results, csv_filename)
    
    # Check if CSV files were created
    csv_files = [f for f in os.listdir() if f.endswith(".csv")]
    print(f"📂 CSV Files Generated: {csv_files}")

    print("🎉 All queries processed successfully!")

if __name__ == "__main__":
    main()
