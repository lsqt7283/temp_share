import os
import openai
from azure.identity import DefaultAzureCredential
import pickle
from time import sleep


def _deploy_gpt_azure(deployment_name='gpt-4o'):
    api_version='2024-02-01'
    azure_endpoint="https://agw-lsc-apps-prd-eastus-01.azure-api.net/azure-openai-service-api"
    try:
        AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID']
    except KeyError:
        os.environ['AZURE_CLIENT_ID'] = ""
        AZURE_CLIENT_ID = os.environ['AZURE_CLIENT_ID']
    try:
        AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET']
    except KeyError:
        os.environ['AZURE_CLIENT_SECRET'] = ""
        AZURE_CLIENT_SECRET = os.environ['AZURE_CLIENT_SECRET']
    try:
        AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID']
    except KeyError:
        os.environ['AZURE_TENANT_ID'] = ""
        AZURE_TENANT_ID = os.environ['AZURE_TENANT_ID']
    default_credential = DefaultAzureCredential()
    token = default_credential.get_token("https://cognitiveservices.azure.com/.default")
    client = openai.AzureOpenAI(
        azure_endpoint=azure_endpoint,
        api_version=api_version,
        api_key=token.token
    )
    return client


def run_gpt_azure(prompt, short_answer=True, bypassCache=False, model='gpt-4o', system_config="You are a successful portfolio manager."):
    """https://platform.openai.com/docs/api-reference/chat"""
    gpt_client = _deploy_gpt_azure()
    try:
        with open('saved_cache.pkl', 'rb') as f:
            cache = pickle.load(f)
    except FileNotFoundError:
        cache = {}
        with open('saved_cache.pkl', 'wb') as f:
            pickle.dump(cache, f)
    cache_key = (prompt, short_answer, model)
    if not bypassCache:
        if cache_key in cache:
            return cache[cache_key]
    if prompt is not None and len(prompt)>0:
        messages = [{"role": "system", "content": system_config}]
        messages += [{"role": "user", "content": prompt}]
    else:
        return None
    if short_answer:
        max_tokens = 88
    else:
        max_tokens = 3888
    for i in range(10):
        try:
            response = gpt_client.chat.completions.create(
                model = model,
                messages = messages,
                temperature = 0.01,
                max_tokens = max_tokens,
                top_p = 1,
                frequency_penalty = 0.0,
                presence_penalty = 0.0,
            ).choices[0].message.content.strip()
            break
        except openai.APIConnectionError as e:
            print("You are blocked by the GREAT WALL at LS. Contact TCS / Network Security..")
            print(e)
        except openai.BadRequestError as e:
            print("You are blocked by the MSFT Safety Rule. Change prompt and retry..")
            print("YOUR PROMPT:\n"+prompt)
            print(e)
            return ""
        except openai.RateLimitError as e:
            if i > 7: raise
            sleep(i*30)
    cache[cache_key] = response
    with open('saved_cache.pkl', 'wb') as f:
        pickle.dump(cache, f)
    return response


__all__ = ["run_gpt_azure"]

