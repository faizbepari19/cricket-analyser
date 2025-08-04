import json
import os
import duckdb
import requests
from langchain_groq import ChatGroq
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from langgraph.errors import GraphRecursionError
from ddgs import DDGS

# --------------------------------------------------
# 1. Load API Key
# --------------------------------------------------
def load_groq_api_key():
    secrets_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "secrets.txt"
    )
    try:
        with open(secrets_file, "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith("gsk_"):
                    os.environ["GROQ_API_KEY"] = line
                    return line
    except FileNotFoundError:
        pass
    return None

groq_api_key = load_groq_api_key()
if not groq_api_key:
    raise ValueError("GROQ_API_KEY not found in secrets.txt")

# --------------------------------------------------
# 2. Load schema and domain rules
# --------------------------------------------------
script_dir = os.path.dirname(os.path.abspath(__file__))
context_file = os.path.join(script_dir, "prompt-context.json")

try:
    with open(context_file, "r") as f:
        context_data = json.load(f)
except FileNotFoundError:
    raise FileNotFoundError(f"Could not find {context_file}")

schema_context = f"""
You are a cricket data analysis assistant.
You have access to a DuckDB cricket database.

Schema:
{json.dumps(context_data['tables'], indent=2)}

Domain Rules:
{json.dumps(context_data['domain_rules'], indent=2)}

Always:
- Use DuckDB-specific SQL syntax
- Respect schema and domain rules
- Use full official names (not abbreviations) for teams and venues.
  Examples:
    "AUS" → "Australia"
    "ENG" → "England"
    "MCG" → "Melbourne Cricket Ground"
- Return answers in concise plain text, no extra commentary
"""

# --------------------------------------------------
# 3. LLM Setup
# --------------------------------------------------
llm = ChatGroq(
    model="llama-3.3-70b-versatile",
    temperature=0,
    max_tokens=4096
)

# --------------------------------------------------
# 4. Database Path
# --------------------------------------------------
db_path = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "cricket.duckdb"
)

# --------------------------------------------------
# 5. Tools
# --------------------------------------------------

@tool
def cricket_sql_tool(query: str) -> str:
    """
    Query the DuckDB cricket database and return results.
    """
    try:
        conn = duckdb.connect(db_path)
        result = conn.execute(query).fetchall()
        columns = [desc[0] for desc in conn.description]
        conn.close()

        if not result:
            return "No results found in database."

        if len(result) == 1 and len(result[0]) == 1:
            return str(result[0][0])

        formatted_result = [
            dict(zip(columns, row))
            for row in result[:10]  # limit to 10
        ]
        return json.dumps(formatted_result, indent=2, default=str)

    except Exception as e:
        return f"SQL Error: {str(e)}"


@tool
def normalize_cricket_terms_tool(text: str) -> str:
    """
    Normalize cricket terms and abbreviations into official names.
    Example: 'MCG' -> 'Melbourne Cricket Ground', 'AUS' -> 'Australia'
    """
    replacements = {
        "MCG": "Melbourne Cricket Ground",
        "AUS": "Australia",
        "ENG": "England",
        "IND": "India",
        "NZ": "New Zealand",
        "SA": "South Africa",
        "WI": "West Indies",
        "PAK": "Pakistan",
        "SL": "Sri Lanka",
        "BAN": "Bangladesh"
    }

    for abbr, full in replacements.items():
        text = text.replace(abbr, full)
    return text


@tool
def duckduckgo_search_tool(query: str) -> str:
    """
    Search DuckDuckGo for cricket-related information as a fallback.
    Use this when the database query fails or returns no results.
    """
    try:
        # # DuckDuckGo Instant Answer API
        # url = "https://api.duckduckgo.com/"
        # params = {
        #     'q': f"cricket {query}",
        #     'format': 'json',
        #     'no_html': '1',
        #     'skip_disambig': '1'
        # }
        
        # response = requests.get(url, params=params, timeout=10)
        # data = response.json()
        # print(f"🔍 DuckDuckGo search for: {data}")
        
        # # Try to get abstract or definition
        # if data.get('Abstract'):
        #     return data['Abstract']
        # elif data.get('Definition'):
        #     return data['Definition']
        # elif data.get('Answer'):
        #     return data['Answer']
        # elif data.get('RelatedTopics') and len(data['RelatedTopics']) > 0:
        #     first_topic = data['RelatedTopics'][0]
        #     if isinstance(first_topic, dict) and 'Text' in first_topic:
        #         return first_topic['Text']
        
        # return f"No specific information found for: {query}. Try searching cricket websites directly."
        with DDGS() as ddgs:
            results = ddgs.text(query, max_results=3)
            print(f"🔍 DuckDuckGo search for: {len(results)}")
            
            # Extract the actual content/answers from the results
            answers = []
            for r in results:
                title = r.get("title", "")
                body = r.get("body", "")
                # Combine title and body for more complete information
                if body:
                    answers.append(f"{title}: {body}")
                else:
                    answers.append(title)
            
            return "\n\n".join(answers) if answers else f"No specific information found for: {query}. Try searching cricket websites directly."
        
    except Exception as e:
        return f"Search Error: {str(e)}"

# --------------------------------------------------
# 6. Create Agent
# --------------------------------------------------
tools = [normalize_cricket_terms_tool, cricket_sql_tool, duckduckgo_search_tool]

system_message = f"""
You are a cricket data analysis assistant with access to a DuckDB cricket database.

{schema_context}

When answering questions:
1. Normalize terms using normalize_cricket_terms_tool before querying.
2. Write a SQL query using cricket_sql_tool.
3. If query returns no results, try a different query with alternative filters.
4. If database queries fail or you encounter recursion limits, use duckduckgo_search_tool as a fallback.
5. Provide the final concise answer in plain text.
"""

agent_executor = create_react_agent(llm, tools)

# --------------------------------------------------
# 7. Agent Query Function
# --------------------------------------------------
def ask_cricket_agent(question: str):
    print(f"\n[User Question] {question}")
    
    try:
        result = agent_executor.invoke({
            "messages": [
                ("system", system_message),
                ("human", question)
            ]
        })
        # Tag result as coming from local database
        answer = result["messages"][-1].content
        return f"🏏 [LOCAL DATABASE] {answer}"
    
    except GraphRecursionError:
        print("[Warning] Graph recursion limit reached. Falling back to DuckDuckGo search...")
        # Use DuckDuckGo search as fallback
        search_result = duckduckgo_search_tool.invoke({"query": question})
        return f"🌐 [WEB SEARCH] Database query exceeded recursion limit. Here's what I found from web search:\n\n{search_result}"
    
    except Exception as e:
        print(f"[Error] Unexpected error: {str(e)}")
        # Also try DuckDuckGo search for other errors
        try:
            search_result = duckduckgo_search_tool.invoke({"query": question})
            return f"🌐 [WEB SEARCH] Database query failed ({str(e)}). Here's what I found from web search:\n\n{search_result}"
        except:
            return f"❌ [ERROR] Both database and web search failed. Error: {str(e)}"

# --------------------------------------------------
# 8. Example Usage
# --------------------------------------------------
if __name__ == "__main__":
    # q1 = "Most test matches played by player since 2000?"
    # q2 = "How many times did AUS win at MCG in Test matches since 2000?"
    # q1 = "How many sixes Maxwell hit in 2018?"
    # q1 = "Most player of the match awards in 2018?"
    # q1 = "Who scored the most runs in the 2019 World Cup final match?"
    # q1 = "What was Kohli's average in year 2015 in Test matches?"
    # q1 = "Who won the world cup in 1992?"
    q1 = "List all centuries scored by Steve Smith in Test mactches against India"

    print("\n--- Q1 ---")
    print(ask_cricket_agent(q1))

    # print("\n--- Q2 ---")
    # print(ask_cricket_agent(q2))
