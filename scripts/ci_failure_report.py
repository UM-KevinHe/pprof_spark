#!/usr/bin/env python3
"""Publish CI failure details where readers who are not signed in to GitHub can see them.

GitHub shows workflow logs only to signed-in users, but annotations and job summaries appear on
the public run page. This script reads sbt's JUnit reports (target/test-reports/*.xml) and the
`[error]` lines of sbt logs, emits error annotations, and appends a job summary. It never fails
the job itself: the step that failed already did.

Usage: python3 scripts/ci_failure_report.py <sbt-log>...
"""

import glob
import os
import re
import sys
import xml.etree.ElementTree as ET

MAX_TEST_ANNOTATIONS = 8  # GitHub keeps at most 10 error annotations per step
MAX_LOG_LINES = 40
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")
NOISE = re.compile(r"Using incubator modules|^\s*$")


def escape_data(text):
    return text.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")


def escape_property(text):
    return escape_data(text).replace(":", "%3A").replace(",", "%2C")


def failed_tests():
    found = []
    for path in sorted(glob.glob("**/target/test-reports/*.xml", recursive=True)):
        try:
            root = ET.parse(path).getroot()
        except ET.ParseError as exc:
            found.append((path, "unreadable test report: %s" % exc, ""))
            continue
        for case in root.iter("testcase"):
            for node in case.findall("failure") + case.findall("error"):
                name = "%s: %s" % (case.get("classname", "?"), case.get("name", "?"))
                found.append((name, (node.get("message") or "").strip(), (node.text or "").strip()))
    return found


def error_lines(paths):
    lines, seen = [], set()
    for path in paths:
        if not os.path.exists(path):
            continue
        with open(path, encoding="utf-8", errors="replace") as handle:
            for raw in handle:
                line = ANSI.sub("", raw.rstrip("\n"))
                if not line.startswith("[error]"):
                    continue
                text = line[len("[error]"):].strip()
                if NOISE.search(text) or text in seen:
                    continue
                seen.add(text)
                lines.append(text)
    return lines


def main(argv):
    tests = failed_tests()
    errors = error_lines(argv[1:])
    for name, message, detail in tests[:MAX_TEST_ANNOTATIONS]:
        body = "\n".join([message] + detail.splitlines()[:6]).strip()
        print("::error title=%s::%s" % (escape_property(name), escape_data(body[:3000])))
    if len(tests) > MAX_TEST_ANNOTATIONS:
        extra = len(tests) - MAX_TEST_ANNOTATIONS
        print("::error title=More test failures::%d more failed test cases; see the job summary" % extra)
    if errors:
        shown = "\n".join(errors[:MAX_LOG_LINES])
        title = "sbt [error] lines (first %d)" % min(len(errors), MAX_LOG_LINES)
        print("::error title=%s::%s" % (escape_property(title), escape_data(shown[:4000])))
    if not tests and not errors:
        print("::error title=No failure details found::No failed test cases or sbt error lines")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as out:
            out.write("## Failure details\n\n")
            if tests:
                out.write("%d failed test case(s):\n\n" % len(tests))
                for name, message, _ in tests:
                    out.write("- `%s`: %s\n" % (name, message[:300].replace("\n", " ")))
                out.write("\n")
            if errors:
                out.write("First sbt `[error]` lines:\n\n```\n%s\n```\n" % "\n".join(errors[:120]))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
