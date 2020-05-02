class Error(Exception):
   """Base class for other exceptions"""
   pass

class DeleteError(Error):
   """Raised when the input value is too small"""
   pass

class UploadError(Error):
   """Raised when the input value is too large"""
   pass

class InvalidDirError(Error):
   """Rasied when Directory is Invalid"""
   pass