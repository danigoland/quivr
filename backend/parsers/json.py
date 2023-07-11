from fastapi import UploadFile
from langchain.document_loaders import JSONLoader

from .common import process_file


def process_json(file: UploadFile, enable_summarization, user, user_openai_api_key):
    return process_file(file, JSONLoader, "json", enable_summarization, user, user_openai_api_key)
