from openai import OpenAI
import os
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

print("Listing models...")
try:
    models = client.models.list()
    for m in models:
        if "gpt-4" in m.id:
            print(m.id)
except Exception as e:
    print(f"Error listing models: {e}")
