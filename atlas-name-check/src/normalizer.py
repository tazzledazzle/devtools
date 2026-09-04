"""
Name normalization pipeline (applied in strict order):

  1. Replace '&' and ',' with spaces.
  2. Lowercase + split into words (collapses all whitespace).
  3. Strip trailing standard suffixes.
  4. Strip a single leading article: "the", "an", "a".
  5. Strip "and" from every non-first position.

If the result is empty, the name is considered unavailable.
"""

SUFFIXES = frozenset({"inc.", "corp.", "llc", "l.l.c.", "llc."})
ARTICLES = frozenset({"the", "an", "a"})


def normalize(name: str) -> str:
    # Step 1: special chars → space
    name = name.replace("&", " ").replace(",", " ")

    # Step 2: lowercase and split (implicitly collapses spaces)
    words = name.lower().split()

    # Step 3: remove trailing suffixes (handles stacked suffixes like "Inc. LLC")
    while words and words[-1] in SUFFIXES:
        words.pop()

    # Step 4: remove a single leading article
    if words and words[0] in ARTICLES:
        words.pop(0)

    # Step 5: remove "and" from any position except the first
    # "at the start of the name" is evaluated after step 4 so that
    # "The And Llama" → (remove "The") → "And Llama" keeps "And".
    if words:
        if words[0] == "and":
            words = words[:1] + [w for w in words[1:] if w != "and"]
        else:
            words = [w for w in words if w != "and"]

    return " ".join(words)
