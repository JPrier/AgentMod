"""Parse 'key=value; key2=value2' strings."""


def parse(text):
    out = {}
    parts = [p.strip() for p in text.split(";")]
    for i in range(len(parts) - 1):
        if not parts[i]:
            continue
        key, _, value = parts[i].partition("=")
        out[key.strip()] = value.strip()
    return out
