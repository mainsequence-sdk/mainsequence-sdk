"""The official MCP Python SDK's own Skills tests, run against the SDK's copy (ADR 0039).

``test_server_skills.py`` and ``test_shared_skills.py`` are ``tests/server/test_skills.py``
and ``tests/shared/test_skills.py`` of https://github.com/modelcontextprotocol/python-sdk at
commit ``9a754b0aabb873d9707d52dc165b569fb442ec8c``, MIT-licensed (see
``mainsequence/server/_mcp_skills_upstream``). They import the copy instead of
``mcp.server.skills`` and ``mcp.shared.skills``, and one ``inline_snapshot`` assertion is
written as its literal value. Delete them with the copy.
"""
