"""Explicit ownership after the research/data/paper/platform repository split."""

OUT_OF_SCOPE = "out_of_scope"


def external_owner(path: str) -> str | None:
    if path.startswith("webpage/"):
        return "platform-website"
    if path.startswith("deploy/"):
        return "platform-infra"
    return None
