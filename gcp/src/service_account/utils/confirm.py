def confirm(question: str, kill=True):
    confirmation = input(f"{question} (y/n): ").lower() not in ["y", "yes"]
    if confirmation and kill:
        print("Exiting")
        exit()
    if confirmation:  # No kill / exit
        return False
    return True
