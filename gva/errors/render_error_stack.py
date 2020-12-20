"""
Advanced Error Trace Formatting

Adapted From:
https://github.com/willmcgugan/rich/blob/master/rich/traceback.py
Copyright 2020 Will McGugan
MIT Licence
"""
from typing import Callable, Dict, List, Optional, Type, Any, Iterable, Tuple, TypeVar, Generator
from dataclasses import dataclass, field
from traceback import walk_tb
import os.path
import sys

T = TypeVar("T")


@dataclass
class Frame:
    filename: str
    lineno: int
    name: str
    line: str = ""
    locals: Optional[Dict[str, Any]] = None


@dataclass
class _SyntaxError:
    offset: int
    filename: str
    line: str
    lineno: int
    msg: str


@dataclass
class Stack:
    exc_type: str
    exc_value: str
    syntax_error: Optional[_SyntaxError] = None
    is_cause: bool = False
    frames: List[Frame] = field(default_factory=list)


def _build_error_stack():
    exc_type, exc_value, traceback = sys.exc_info()

    stacks = []
    is_cause = False

    while True:
        stack = Stack(
            exc_type=str(exc_type.__name__),
            exc_value=str(exc_value),
            is_cause=is_cause,
        )

        if isinstance(exc_value, SyntaxError):
            stack.syntax_error = _SyntaxError(
                offset=exc_value.offset or 0,
                filename=exc_value.filename or "?",
                lineno=exc_value.lineno or 0,
                line=exc_value.text or "",
                msg=exc_value.msg,
            )

        stacks.append(stack)
        append = stack.frames.append

        for frame_summary, line_no in walk_tb(traceback):
            filename = frame_summary.f_code.co_filename
            filename = os.path.abspath(filename) if filename else "?"
            frame = Frame(
                filename=filename,
                lineno=line_no,
                name=frame_summary.f_code.co_name,
                locals={ key: value for key, value in frame_summary.f_locals.items() }
            )
            append(frame)

        cause = getattr(exc_value, "__cause__", None)
        if cause and cause.__traceback__:
            exc_type = cause.__class__
            exc_value = cause
            traceback = cause.__traceback__
            if traceback:
                is_cause = True
                continue

        cause = exc_value.__context__
        if (
            cause
            and cause.__traceback__
            and not getattr(exc_value, "__suppress_context__", False)
        ):
            exc_type = cause.__class__
            exc_value = cause
            traceback = cause.__traceback__
            if traceback:
                is_cause = False
                continue
        # No cover, code is reached but coverage doesn't recognize it.
        break
    return stacks


def _render_locals(locals):
    if locals:
        yield ('─' * 35) + '  locals  ' + ('─' * 35)

    max_label_len = 0
    for key, value in locals.items():
        if len(key) > max_label_len:
            max_label_len = len(key)

    for key, value in locals.items():
        yield F" {key:<{max_label_len}} : {value}"


def _read_from_code(
        filename: str,
        line: int,
        extend_by: int) -> Generator:
    try:
        with open(filename, "rt", encoding="utf-8") as code_file:
            code = code_file.read()
        lines = code.splitlines()
        start_line = max(line - extend_by, 0)
        end_line = min(line + extend_by, len(lines)+1)
        yield ('─' * 36) + '  code  ' + ('─' * 36)
        for line_number in range(start_line, end_line):
            prefix = '❱' if line_number == line else ' '
            yield F"{prefix}{line_number:4d} {lines[line_number-1]}"
            line_number += 1
    except:
        return ""


def _render_error_stack():
    stack = _build_error_stack()
    for item in stack:
        for frame in item.frames:
            yield F"{frame.filename}:{frame.lineno} in {frame.name}"
            if frame.filename.startswith("<"):
                continue
            yield from _render_locals(frame.locals)
            yield from _read_from_code(
                    filename=frame.filename,
                    line=frame.lineno,
                    extend_by=3)
        
            yield '─' * 80
            yield ''


def RenderErrorStack():
    s = list(_render_error_stack())
    return '\n'.join(s)


if __name__ == "__main__":  # pragma: no cover

    import sys

    def bar(a):
        one = 1
        print(one / a)

    def foo(a):

        zed = {
            "characters": {
                "Paul Atriedies",
                "Vladimir Harkonnen",
                "Thufir Haway",
                "Duncan Idaho",
            },
            "atomic_types": (None, False, True),
        }
        bar(a)

    def error():

#        foo(0)

        try:
            foo(0)
        except:
            print(RenderErrorStack())


    error()