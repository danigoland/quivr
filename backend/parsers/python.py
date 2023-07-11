from fastapi import UploadFile
from langchain.document_loaders import PythonLoader

from .common import process_file
from models.settings import CommonsDep


def process_py(commons: CommonsDep, file: UploadFile, enable_summarization, user, user_openai_api_key):
    return process_file(commons, file, PythonLoader, enable_summarization, user, user_openai_api_key)
