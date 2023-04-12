import uuid


def is_uuid_valid(uuid_to_test):
    try:
        uuid_obj = uuid.UUID(uuid_to_test)
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test
