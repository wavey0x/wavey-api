import json
import logging
import os
import re
import subprocess
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path

import bleach
import cmarkgfm
from cmarkgfm import Options
from lxml import etree, html


logger = logging.getLogger(__name__)

SANITIZER_CONFIG_VERSION = "2026-06-02.2"
SYNTAX_CSS_VERSION = "2026-06-02.1"
HIGHLIGHT_GRAMMAR_SET = "all"
HIGHLIGHT_SCRIPT = Path(__file__).with_name("render_highlight.mjs")
REPO_ROOT = Path(__file__).resolve().parents[2]

HIGHLIGHT_TIMEOUT_SECONDS = float(os.getenv("GIST_HIGHLIGHT_TIMEOUT_SECONDS", "8"))
MAX_HIGHLIGHT_BLOCK_BYTES = int(
    os.getenv("GIST_MAX_HIGHLIGHT_BLOCK_BYTES", str(200 * 1024))
)

SCRIPTABLE_TAGS = {"script", "style", "svg", "math", "iframe"}
SCRIPTABLE_TEXT_RE = re.compile(
    r"<(script|style|svg|math|iframe)\b[^>]*>.*?</\1\s*>",
    re.IGNORECASE | re.DOTALL,
)

ALLOWED_TAGS = {
    "a",
    "blockquote",
    "br",
    "code",
    "del",
    "div",
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
    "span",
    "strong",
    "table",
    "tbody",
    "td",
    "th",
    "thead",
    "tr",
    "ul",
}

SAFE_REL_TOKENS = {"nofollow", "noopener", "noreferrer"}
SAFE_DIR_VALUES = {"auto", "ltr", "rtl"}
SAFE_TASK_CLASSES = {"contains-task-list", "task-list-item"}
SAFE_EXACT_CLASSES = {"highlight"}
SAFE_CLASS_PATTERNS = (
    re.compile(r"^highlight-(source|text)-[A-Za-z0-9_.+-]+$"),
    re.compile(r"^language-[A-Za-z0-9_.+-]+$"),
    re.compile(r"^pl-[A-Za-z0-9_-]+$"),
)


def _package_version(package_name):
    try:
        return version(package_name)
    except PackageNotFoundError:
        return "missing"


def _node_package_version(package_name):
    package_json = REPO_ROOT / "node_modules" / package_name / "package.json"
    if package_json.exists():
        try:
            return json.loads(package_json.read_text(encoding="utf-8")).get(
                "version", "unknown"
            )
        except (OSError, json.JSONDecodeError):
            return "unknown"

    return "missing"


def _render_gfm(markdown):
    return cmarkgfm.github_flavored_markdown_to_html(
        markdown,
        options=Options.CMARK_OPT_UNSAFE,
    )


def _is_code_context(element):
    current = element
    while current is not None:
        if current.tag in {"pre", "code"}:
            return True
        current = current.getparent()
    return False


def _strip_scriptable_text(value):
    if not value:
        return value
    return SCRIPTABLE_TEXT_RE.sub("", value)


def _drop_scriptable_content(root):
    for element in list(root.iter()):
        if element is root:
            continue
        if isinstance(element.tag, str) and element.tag.lower() in SCRIPTABLE_TAGS:
            element.drop_tree()

    for element in root.iter():
        if _is_code_context(element):
            continue
        element.text = _strip_scriptable_text(element.text)
        element.tail = _strip_scriptable_text(element.tail)


def _is_external_href(href):
    return href.startswith("http://") or href.startswith("https://")


def _post_process_links(root):
    for element in root.iter("a"):
        href = element.attrib.get("href", "")
        if not _is_external_href(href):
            continue
        rel = {
            token
            for token in element.attrib.get("rel", "").split()
            if token in SAFE_REL_TOKENS
        }
        rel.add("nofollow")
        element.attrib["rel"] = " ".join(sorted(rel))


def _code_text(pre):
    code = pre.find("code")
    if code is None:
        return pre.text_content()
    return code.text_content()


def _language_class(language):
    safe_language = re.sub(r"[^A-Za-z0-9_.+-]", "-", language.strip().lower())
    return f"language-{safe_language}" if safe_language else None


def _plain_code_block(language, code):
    pre = etree.Element("pre")
    code_element = etree.SubElement(pre, "code")
    language_class = _language_class(language)
    if language_class:
        code_element.attrib["class"] = language_class
    code_element.text = code
    return pre


def _scope_to_highlight_class(scope):
    safe_scope = re.sub(r"[^A-Za-z0-9_.+-]", "-", scope)
    return f"highlight-{safe_scope.replace('.', '-')}"


def _highlight_payload(blocks):
    process = subprocess.run(
        ["node", str(HIGHLIGHT_SCRIPT)],
        cwd=str(REPO_ROOT),
        input=json.dumps(
            {
                "grammar_set": HIGHLIGHT_GRAMMAR_SET,
                "blocks": blocks,
            }
        ),
        text=True,
        capture_output=True,
        timeout=HIGHLIGHT_TIMEOUT_SECONDS,
        check=False,
    )
    if process.returncode != 0:
        logger.warning(
            "Gist code highlighting failed",
            extra={"returncode": process.returncode, "reason": "process_error"},
        )
        return {}

    try:
        data = json.loads(process.stdout)
    except json.JSONDecodeError:
        logger.warning("Gist code highlighting failed", extra={"reason": "bad_json"})
        return {}

    highlighted = {}
    for item in data.get("blocks", []):
        if not item.get("ok"):
            continue
        index = item.get("index")
        if not isinstance(index, int):
            continue
        highlighted[index] = item
    return highlighted


def _highlight_blocks(root):
    candidates = []
    pre_elements = list(root.xpath(".//pre[@lang]"))
    for index, pre in enumerate(pre_elements):
        language = pre.attrib.get("lang", "")
        code = _code_text(pre)
        byte_count = len(code.encode("utf-8"))
        if byte_count > MAX_HIGHLIGHT_BLOCK_BYTES:
            logger.info(
                "Skipping gist code highlighting",
                extra={
                    "language": language,
                    "byte_count": byte_count,
                    "reason": "too_large",
                },
            )
            continue
        candidates.append({"index": index, "language": language, "code": code})

    highlighted = {}
    if candidates and HIGHLIGHT_SCRIPT.exists():
        try:
            highlighted = _highlight_payload(candidates)
        except (
            OSError,
            subprocess.SubprocessError,
            subprocess.TimeoutExpired,
            ValueError,
        ):
            highlighted = {}

    for index, pre in enumerate(pre_elements):
        language = pre.attrib.get("lang", "")
        item = highlighted.get(index)

        if not item:
            logger.info(
                "Using plain gist code block",
                extra={
                    "language": language,
                    "byte_count": len(_code_text(pre).encode("utf-8")),
                    "reason": "unrecognized_or_failed",
                },
            )
            replacement = _plain_code_block(language, _code_text(pre))
            pre.getparent().replace(pre, replacement)
            continue

        wrapper = etree.Element("div")
        highlight_class = item.get("class_name") or _scope_to_highlight_class(
            item["scope"]
        )
        wrapper.attrib["class"] = (
            f"highlight {highlight_class}"
        )
        wrapper.attrib["dir"] = "auto"
        highlighted_pre = etree.SubElement(wrapper, "pre")
        fragments = html.fragments_fromstring(item["html"])
        for fragment in fragments:
            if isinstance(fragment, str):
                if len(highlighted_pre):
                    last = highlighted_pre[-1]
                    last.tail = (last.tail or "") + fragment
                else:
                    highlighted_pre.text = (highlighted_pre.text or "") + fragment
            else:
                highlighted_pre.append(fragment)

        pre.getparent().replace(pre, wrapper)


def _parse_fragment(raw_html):
    return html.fragment_fromstring(raw_html, create_parent="div")


def _serialize_fragment(root):
    return "".join(
        html.tostring(child, encoding="unicode", method="html") for child in root
    )


def _safe_class_token(token):
    if token in SAFE_EXACT_CLASSES or token in SAFE_TASK_CLASSES:
        return True
    return any(pattern.fullmatch(token) for pattern in SAFE_CLASS_PATTERNS)


def _safe_class_value(value):
    tokens = value.split()
    return bool(tokens) and all(_safe_class_token(token) for token in tokens)


def _allow_attribute(tag, name, value):
    if name == "class":
        return tag in {"code", "div", "li", "pre", "span", "ul"} and _safe_class_value(
            value
        )

    if name == "dir":
        return value in SAFE_DIR_VALUES

    if tag == "a":
        if name in {"href", "title"}:
            return True
        if name == "rel":
            tokens = value.split()
            return bool(tokens) and all(token in SAFE_REL_TOKENS for token in tokens)
        return False

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
    raw_html = _render_gfm(markdown)
    root = _parse_fragment(raw_html)
    _drop_scriptable_content(root)
    _highlight_blocks(root)
    _post_process_links(root)
    processed_html = _serialize_fragment(root)

    return bleach.clean(
        processed_html,
        tags=ALLOWED_TAGS,
        attributes=_allow_attribute,
        protocols=["http", "https", "mailto"],
        strip=True,
        strip_comments=True,
    )


def render_version():
    return (
        f"cmarkgfm/{_package_version('cmarkgfm')};"
        f"starry-night/{_node_package_version('@wooorm/starry-night')};"
        f"grammar/{HIGHLIGHT_GRAMMAR_SET};"
        f"bleach/{_package_version('bleach')};"
        f"lxml/{_package_version('lxml')};"
        f"syntax-css/{SYNTAX_CSS_VERSION};"
        f"sanitizer/{SANITIZER_CONFIG_VERSION}"
    )
