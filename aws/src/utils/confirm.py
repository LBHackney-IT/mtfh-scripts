def confirm(question: str, kill=True) -> bool:
    """
    Asks the user for confirmation, kills the program, or returns a boolean
    :param question: Question to ask
    :param kill: Whether to kill the program if the user says no
    """
    confirmation_input: str = ""
    while confirmation_input not in ["y", "yes", "n", "no"]:
        confirmation_input = input(f"{question} (y/n): ").lower()

    if confirmation_input in ["n", "no"]:  # No kill / exit
        if kill:
            print("Exiting")
        else:
            return False
    return True
