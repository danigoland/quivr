from fastapi import UploadFile
from langchain.document_loaders import PythonLoader

from .common import process_file


def process_py(file: UploadFile, enable_summarization, user, user_openai_api_key):
    return process_file(file, PythonLoader, "py", enable_summarization, user, user_openai_api_key)
