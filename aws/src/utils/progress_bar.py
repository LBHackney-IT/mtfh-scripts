class ProgressBar:
    def __init__(self, total: int, separation: str = "\n\n", bar_length: int = 20):
        """
        Displays a progress bar to the console
        :param total: Total number of increments
        :param separation: Separation to add to the start and end of the progress bar
        :param bar_length: Length of the progress bar
        """
        self.total = total
        self.separation = separation
        self.bar_length = bar_length

    def display(self, current: int, note: str = None):
        """
        Prints a progress bar to the console
        :param current: Current number
        :param note: Note to add to the end of the progress bar
        """
        progress = current / self.total
        block = int(round(self.bar_length * progress))
        bar = f"Progress: [{block * '#'}{(self.bar_length - block) * ' '}] {current}/{self.total} " \
              f"{f'- {note}' if note else ''}"
        padded_bar = f"{self.separation}{bar}{self.separation}"
        print(padded_bar)
