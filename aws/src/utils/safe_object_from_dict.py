from typing import TypeVar

T = TypeVar("T")


def safe_object_from_dict(object_type: type[T], data: dict) -> T:
    """
    Safely creates an object of the given type from the given dictionary.
    :param object_type: The type of object to create.
    :param data: The dictionary containing the data to use for creating the object.
    :return: An object of the given type, created from the given dictionary.
    """

    if data is None:
        return None

    for key in list(data.keys()):
        if key not in object_type.__annotations__.keys():
            # print(f"WARNING: {key} not in {object_type.__name__}")
            del data[key]

    for key in object_type.__annotations__.keys():
        if key not in data.keys():
            # print(f"WARNING: {key} from {object_type.__name__} not in data")
            data[key] = None

    return object_type(**data)
