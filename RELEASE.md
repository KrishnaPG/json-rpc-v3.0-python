# Release Checklist

Run these checks from the `jsonrpc3/` directory before publishing.

1. `uv run --with build python -m build`
2. `uv run --with twine twine check dist/*`
3. `python -m venv /tmp/jsonrpc3-publish-check`
4. `/tmp/jsonrpc3-publish-check/bin/python -m pip install dist/*.whl`
5. `/tmp/jsonrpc3-publish-check/bin/python - <<'PY'`
   ```python
   from jsonrpc3 import JsonRpcClient, JsonRpcServer, JsonRpcStreamCall

   assert JsonRpcClient
   assert JsonRpcServer
   assert JsonRpcStreamCall
   print("jsonrpc3 import smoke passed")
   ```
   `PY`

Use TestPyPI before publishing a first public release. Confirm the package name,
license, owner, and project URLs before uploading to production PyPI.
