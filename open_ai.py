from openai import OpenAI


def open_ai(prompt: str) -> str:
    client = OpenAI(api_key="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")

    completion = client.chat.completions.create(
        model="gpt-3.5-turbo", messages=[{"role": "user", "content": f"""{prompt}"""}]
    )
    message = completion.choices[0].message.content

    start_index = str(message).find("```")

    end_index = str(message).find("```", start_index + 3)

    code_text = message[start_index + 3 : end_index]

    return code_text.replace("sql", "")
