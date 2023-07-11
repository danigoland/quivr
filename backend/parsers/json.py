from fastapi import UploadFile
from langchain.document_loaders import JSONLoader

from .common import process_file
from models.settings import CommonsDep


def process_json(commons: CommonsDep, file: UploadFile, enable_summarization, user, user_openai_api_key):
    return process_file(commons, file, JSONLoader, enable_summarization, user, user_openai_api_key)
