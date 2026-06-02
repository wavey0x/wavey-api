from importlib.metadata import version
import re

import bleach
from markdown_it import MarkdownIt
from mdit_py_plugins.tasklists import tasklists_plugin


SANITIZER_CONFIG_VERSION = "2026-06-02.1"

ALLOWED_TAGS = {
    "a",
    "blockquote",
    "br",
    "code",
    "del",
    "em",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "hr",
    "img",
    "input",
    "li",
    "ol",
    "p",
    "pre",
    "s",
    "strong",
    "table",
    "tbody",
    "td",
    "th",
    "thead",
    "tr",
    "ul",
}

ALLOWED_CLASS_TAGS = {"code", "pre", "ul", "li", "input"}
SCRIPTABLE_BLOCK_RE = re.compile(
    r"<(script|style|svg|math)\b[^>]*>.*?</\1\s*>",
    re.IGNORECASE | re.DOTALL,
)


def _markdown_renderer():
    return MarkdownIt(
        "gfm-like",
        {
            "html": True,
            "linkify": True,
            "typographer": False,
        },
    ).use(tasklists_plugin, enabled=False)


def _allow_attribute(tag, name, value):
    if name == "class" and tag in ALLOWED_CLASS_TAGS:
        return True

    if tag == "a" and name in {"href", "title"}:
        return True

    if tag == "img":
        if name == "src":
            return value.startswith("https://")
        return name in {"alt", "title", "width", "height"}

    if tag == "input":
        if name == "type":
            return value == "checkbox"
        return name in {"checked", "disabled"}

    if tag in {"td", "th"} and name == "align":
        return value in {"left", "right", "center"}

    return False


def render_markdown(markdown):
    renderer = _markdown_renderer()
    raw_html = renderer.render(markdown)
    raw_html = SCRIPTABLE_BLOCK_RE.sub("", raw_html)
    sanitized_html = bleach.clean(
        raw_html,
        tags=ALLOWED_TAGS,
        attributes=_allow_attribute,
        protocols=["http", "https", "mailto"],
        strip=True,
    )
    return sanitized_html


def render_version():
    return (
        f"markdown-it-py/{version('markdown-it-py')};"
        f"mdit-py-plugins/{version('mdit-py-plugins')};"
        f"bleach/{version('bleach')};"
        f"sanitizer/{SANITIZER_CONFIG_VERSION}"
    )
