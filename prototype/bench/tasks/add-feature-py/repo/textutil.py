"""Small text helpers."""


def title_case(text):
    return " ".join(w[:1].upper() + w[1:] for w in text.split())


def slugify(text):
    raise NotImplementedError
