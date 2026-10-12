"""The official MCP Skills extension, copied until an ``mcp`` release ships it (ADR 0039).

``server.py`` and ``shared.py`` are ``src/mcp/server/skills.py`` and
``src/mcp/shared/skills.py`` of the official MCP Python SDK,
https://github.com/modelcontextprotocol/python-sdk, at commit
``9a754b0aabb873d9707d52dc165b569fb442ec8c`` ("Add `Skills` extension", #3485),
the server side of the ``io.modelcontextprotocol/skills`` extension. They run on
the released ``mcp`` 2.3.0.

The only change is one import in ``server.py``, which reads ``shared.py`` from
this package instead of ``mcp.shared.skills``. Nothing else is edited, fixed or
extended; take an upstream fix by copying both files again from a later commit.
``UPSTREAM`` records each file's SHA-256 as upstream published it, and a test
checks that the copies still match. Delete this package once an ``mcp`` release
contains the extension.

The copied files are under the license of the MCP Python SDK:

    MIT License

    Copyright (c) 2024 Anthropic, PBC

    Permission is hereby granted, free of charge, to any person obtaining a copy
    of this software and associated documentation files (the "Software"), to deal
    in the Software without restriction, including without limitation the rights
    to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
    copies of the Software, and to permit persons to whom the Software is
    furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all
    copies or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
    IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
    FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
    AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
    LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
    OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
    SOFTWARE.
"""

COMMIT = "9a754b0aabb873d9707d52dc165b569fb442ec8c"

# Copied file: (upstream path, SHA-256 of the upstream file).
UPSTREAM = {
    "server.py": (
        "src/mcp/server/skills.py",
        "4a7e9b1db05d693b07c45a4db4ef85ef54713708a96154841255aa3c8d8a5334",
    ),
    "shared.py": (
        "src/mcp/shared/skills.py",
        "2669d7f4fa0ba328bdde70589d410f58274f5e8aff9d01e31e9b500d7a45ccfb",
    ),
}

# The one line changed in server.py: (upstream, copy).
CHANGED_IMPORT = (
    "from mcp.shared.skills import (",
    "from mainsequence.server._mcp_skills_upstream.shared import (",
)
