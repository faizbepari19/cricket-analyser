import os
import duckdb
import pandas as pd
import chromadb
from llama_index.core import VectorStoreIndex, Document, StorageContext
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.embeddings.openai import OpenAIEmbedding
from openai import OpenAI

# === 1. CONNECT TO DUCKDB ===
DUCKDB_PATH = "cricket.duckdb"
conn = duckdb.connect(DUCKDB_PATH)

# === 2. EXTRACT SCHEMA & UNIQUE VALUES ===
schema_docs = []

tables = conn.execute("SHOW TABLES").fetchall()
tables = [t[0] for t in tables]

for table_name in tables:
    # Get columns
    cols_df = conn.execute(f"DESCRIBE {table_name}").df()
    cols = cols_df["column_name"].tolist()
    schema_docs.append(f"Table {table_name} has columns: {', '.join(cols)}")

    # Get distinct values for string-like columns
    for col in cols:
        try:
            values_df = conn.execute(f"SELECT DISTINCT {col} FROM {table_name} LIMIT 50").df()
            values = values_df[col].dropna().unique()
            if len(values) > 0 and values.dtype == object:  # only strings
                schema_docs.append(f"{table_name}.{col} possible values: {', '.join(map(str, values))}")
        except:
            pass

schema_text = "\n".join(schema_docs)

# === 3. BUILD CHROMA VECTOR STORE ===
persist_dir = "./chroma_store"
chroma_client = chromadb.PersistentClient(path=persist_dir)
chroma_collection = chroma_client.get_or_create_collection("cricket_schema")

embedding_model = OpenAIEmbedding(model="text-embedding-3-small")
vector_store = ChromaVectorStore(chroma_collection=chroma_collection)
storage_context = StorageContext.from_defaults(vector_store=vector_store)

# === 4. INDEX SCHEMA & VALUES ===
documents = [Document(text=schema_text)]
index = VectorStoreIndex.from_documents(documents, storage_context=storage_context, embed_model=embedding_model)

# === 5. RETRIEVAL-AUGMENTED SQL GENERATION ===
client = OpenAI(api_key='')

def generate_sql(user_query):
    # Retrieve relevant schema/value matches
    retriever = index.as_retriever(similarity_top_k=5)
    retrieved_docs = retriever.retrieve(user_query)
    retrieved_context = "\n".join([d.text for d in retrieved_docs])

    # Build final SQL prompt
    prompt = f"""
You are an expert cricket database SQL generator.
Only use the given database schema and values when generating SQL.

Schema and values:
{retrieved_context}

User question:
{user_query}

Output ONLY SQL, nothing else.
"""

    resp = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    return resp.choices[0].message.content.strip()

# === 6. TEST ===
query = "how many matches aus won at MCG since 2000 in test"
sql = generate_sql(query)
print("Generated SQL:\n", sql)
