from openai import OpenAI
import os
import inspect
from dotenv import load_dotenv

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

print("Signature of client.responses.create:")
try:
    sig = inspect.signature(client.responses.create)
    for name, param in sig.parameters.items():
        print(f"{name}: {param.annotation}")
except Exception as e:
    print(f"Error inspecting signature: {e}")

print("\nMethod Docstring:")
print(client.responses.create.__doc__)
