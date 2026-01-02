from openai import OpenAI
import os

api_key = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=api_key)

print("Checking client attributes:")
print(f"Has responses? {'responses' in dir(client)}")
print(f"Has beta? {'beta' in dir(client)}")

if 'beta' in dir(client):
    print(f"Has beta.responses? {'responses' in dir(client.beta)}")
