from openai import OpenAI
import logging
import os
from dotenv import load_dotenv

load_dotenv()


def open_ai(prompt: str) -> str:
    print("Connecting to OpenAI")
    client = OpenAI(api_key=f"{os.environ.get('OPENAI_API_KEY1')}")

    completion = client.chat.completions.create(
        model="gpt-3.5-turbo", messages=[{"role": "user", "content": f"""{prompt}"""}]
    )
    print("Connection to OpenAI is successful")
    message = completion.choices[0].message.content

    start_index = str(message).find("```")

    end_index = str(message).find("```", start_index + 3)

    code_text = message[start_index + 3 : end_index]

    return code_text.replace("sql", "")
