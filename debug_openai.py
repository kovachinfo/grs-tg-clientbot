from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

print(f"Testing client.responses.create...")

tools = [{"type": "web_search", "web_search": {"search_depth": "basic"}}]
messages = [{"role": "user", "content": "What is the capital of France?"}]

try:
    response = client.responses.create(
        model="gpt-4o",
        messages=messages,
        tools=tools
    )
    print("Success!")
    print(f"Output type: {type(response)}")
    # Try to access output_text
    print(f"Output Text: {response.output_text}")
except Exception as e:
    print(f"FAILED: {e}")
