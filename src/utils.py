# Utils (miscellaneous)

# functions
def remove_extra_spaces(text) -> str:
    return " ".join(text.split())

def list_flatten(list) -> list:
    return [x2 for x1 in list for x2 in x1]