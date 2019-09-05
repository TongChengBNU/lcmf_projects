import os

def WinPathToPyPath(path: str) -> str:
    return path.replace("\\", "/")


def currentWorkingDirectory():
    return WinPathToPyPath(os.getcwd())