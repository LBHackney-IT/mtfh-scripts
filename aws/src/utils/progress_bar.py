class ProgressBar:
    def __init__(self, total: int, separation: str = "\n\n"):
        """
        Displays a progress bar to the console
        :param total: Total number of increments
        :param separation: Separation to add to the start and end of the progress bar
        :param bar_length: Length of the progress bar
        """
        self.total = total
        self.separation = separation
        self.BAR_LENGTH = 50

    def display(self, current: int, note: str = None):
        """
        Prints a progress bar to the console
        :param current: Current number
        :param note: Note to add to the end of the progress bar
        """
        progress = current / self.total
        block = int(round(self.BAR_LENGTH * progress))
        bar = f"Progress: [{block * '#'}{(self.BAR_LENGTH - block) * ' '}] {current}/{self.total} " \
              f"{f'- {note}' if note else ''}"
        padded_bar = f"{self.separation}{bar}{self.separation}"
        print(padded_bar)


if __name__ == "__main__":
    # Example usage
    pb = ProgressBar(1000)
    for i in range(201):
        pb.display(i*5)
