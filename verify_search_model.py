from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

model_mini = "gpt-4o-mini-search-preview"
model_5 = "gpt-5-search-api"

print(f"--- Test 1: {model_mini} (Implicit) ---")
try:
    resp = client.chat.completions.create(model=model_mini, messages=[{"role": "user", "content": "Latest visa news 2025?"}])
    print("Success?", resp.choices[0].message.content[:100])
except Exception as e:
    print(f"Error: {e}")

print(f"\n--- Test 2: {model_5} (With Tools) ---")
try:
    resp = client.chat.completions.create(
        model=model_5, 
        messages=[{"role": "user", "content": "Latest visa news 2025?"}],
        tools=[{"type": "web_search", "web_search": {"search_depth": "basic"}}]
    )
    print("Success?", resp.choices[0].message.content[:100])
except Exception as e:
    print(f"Error: {e}")

print(f"\n--- Test 3: client.responses.create (input param) ---")
try:
    resp = client.responses.create(
        model="gpt-4o", 
        input="Latest visa news 2025?",
        tools=[{"type": "web_search", "web_search": {"search_depth": "basic"}}]
    )
    print("Success!")
    print(resp.output_text[:100])
except Exception as e:
    print(f"Error: {e}")
