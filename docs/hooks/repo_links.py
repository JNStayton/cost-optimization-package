"""MkDocs hook: fix links in pages included from the repo root (README, TESTING, CHANGELOG).

Those files link to other repo files, which work on GitHub but aren't pages on the site.
Links to the included files go to their site pages; links to other files outside docs/
go to the file on GitHub.
"""
import re

REPO_BLOB = "https://github.com/dbt-labs/dbt-cost-optimization-package/blob/main/"
SITE_PAGES = {
    "README.md": "index.md",
    "TESTING.md": "testing.md",
    "CHANGELOG.md": "changelog.md",
    "CONTRIBUTING.md": "contributing.md",
}

_LINK = re.compile(r"\]\(\.\./([^)#\s]+)(#[^)\s]*)?\)")


def on_page_markdown(markdown, page, config, files):
    def fix(match):
        target, anchor = match.group(1), match.group(2) or ""
        if target.startswith("docs/"):
            return match.group(0)
        if target in SITE_PAGES:
            return f"]({SITE_PAGES[target]}{anchor})"
        return f"]({REPO_BLOB}{target}{anchor})"

    return _LINK.sub(fix, markdown)
