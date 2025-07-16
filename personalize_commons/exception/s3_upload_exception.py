class S3UploadException(Exception):
    def __init__(self, message):
        self.message = message