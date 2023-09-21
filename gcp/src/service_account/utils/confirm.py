def confirm(prompt: str) -> bool:
    """
    Prompts user to confirm an action
    :param prompt: Prompt to show user
    :return: True if user confirms, False if user denies
    """
    while True:
        response = input(f"{prompt} (y/n): ")
        if response.lower() in ["y", "yes"]:
            return True
        elif response.lower() in ["n", "no"]:
            return False
        else:
            print("Invalid response. Please enter y or n.")