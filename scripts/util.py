import sys


def chunk(lst, n):
    """Yield successive `n`-sized chunks from `lst`."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def confirm_to_continue(message):
    """Ask the user for confirmation before continuing.

    Any answer other than "y" will exit the program.
    """
    print(message)
    if input("Continue? [y/N]: ") != "y":
        print("Abort.")
        sys.exit()
