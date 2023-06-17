import re

def masking_percent(text, subst="percentagetoken"):
    regex = r"\(?\d+(?:[,\.]\d+)? ?%\)?"

    # You can manually specify the number of replacements by changing the 4th argument
    result = re.sub(regex, subst, text, 0, re.MULTILINE | re.IGNORECASE | re.UNICODE)
    return result

def masking_density(text, subst="densitytoken"):
    regex = r"\d+(?:[,\.]\d+)? ?(?:mcg|mg|kg|g) ?\/ ?(?:ml|lít|l|kg)"
    result = re.sub(regex, subst, text, 0, re.MULTILINE | re.IGNORECASE | re.UNICODE)

    regex = r"\d+(?:[,\.]\d+)? ?(?:ug|mcg|mg|kg|g|ml|lít|l)"
    subst = 'quantitiytoken'
    result = re.sub(regex, subst, result, 0, re.MULTILINE | re.IGNORECASE | re.UNICODE)

    # ins to e number       
    regex = r"\bi?ns ?e?(\d+[abcdef]?)"
    subst = " e\\1"
    result = re.sub(regex, subst, result, 0, re.MULTILINE | re.IGNORECASE | re.UNICODE)

    regex = r"\be (\d+[abcdef]?)[ ivx]*[\b]]"
    subst = " e\\1"
    result = re.sub(regex, subst, result, 0, re.MULTILINE | re.IGNORECASE | re.UNICODE)

    regex = r"\b(\d\d\d+[abcdef]?)"
    subst = " e\\1"
    result = re.sub(regex, subst, result, 0, re.MULTILINE | re.IGNORECASE | re.UNICODE)

    # additive variant e.g e451 (i)
    regex = r"(\be?\d+[abcdef]?) ?\(?([ivx]+)\)?"
    subst = " e\\1_\\2"
    result = re.sub(regex, subst, result, 0, re.MULTILINE | re.IGNORECASE | re.UNICODE)

    regex = r"\b[e ]+(\d+)"
    subst = " e\\1"
    result = re.sub(regex, subst, result, 0, re.MULTILINE | re.IGNORECASE | re.UNICODE)

    # remove multiple space
    regex = r" +"
    subst = " "
    result = re.sub(regex, subst, result, 0, re.MULTILINE | re.IGNORECASE | re.UNICODE)

    return result


def extract_only_word(text):
    return " ".join(re.findall(r"[\w|\d']+", text))

def split_into_term(text):
    pattern = r"\s*(?:,|;|\(|\)|\.|và|\:| \- |\*|\[|\]|\&|\!|\@|\#|\^|\/|\\|\|)\s*"
    terms = re.split(pattern, text, flags=re.MULTILINE | re.IGNORECASE | re.UNICODE)
    return [extract_only_word(i.strip().lower()) for i in terms if i.strip() != '']


def tokenizer(text):
    text = text.lower().strip()
    text = masking_percent(text)
    text = masking_density(text)
    toks = split_into_term(text)
    text = " | ".join(toks)
    return text