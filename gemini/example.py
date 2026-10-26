import os
import time
from google import genai

API_KEY = os.environ.get("HACKNEY_GEMINI_API_KEY")
assert API_KEY, "HACKNEY_GEMINI_API_KEY environment variable is not set"

client = genai.Client(
    api_key=API_KEY,
    http_options=genai.types.HttpOptions(
        retry_options=genai.types.HttpRetryOptions(attempts=1)
    ),
)
tools = []

generation_config = {
    "max_output_tokens": 65536,
    "thinking_level": "medium",
}

print("Creating interaction...")
start = time.perf_counter()
interaction = client.interactions.create(
    model="models/gemini-3.8-flash",
    input="""What is the capital of France?""",
    tools=tools,
    generation_config=generation_config,
)
elapsed = time.perf_counter() - start
print(f"Interaction created in {elapsed:.2f}s.")

print(interaction.steps[-1])
