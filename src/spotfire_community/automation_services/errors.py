class JobNotFoundError(Exception):
    def __init__(self, job_id: str):
        super().__init__(f"Job with ID {job_id} not found")


class InvalidJobIdError(Exception):
    def __init__(self, job_id: str):
        super().__init__(f"Invalid job ID: {job_id}. Should be a UUID.")
