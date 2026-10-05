#python3 uv pip compile pyproject.toml -o pylock.toml
python3 uv export --no-emit-project -o pylock.toml
python3 uv pip sync pylock.toml