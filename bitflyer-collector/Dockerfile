FROM ghcr.io/astral-sh/uv:latest AS uv_bin
# uv のバイナリだけを拝借する

FROM python:3.14-slim

# uv バイナリをコピー
COPY --from=uv_bin /uv /uvx /bin/

# コンテナ内での uv の挙動を設定
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# 先に依存関係だけをインストール（コード変更で再インストールさせない）
COPY pyproject.toml ./
RUN uv sync --no-dev

# アプリケーションをコピー
COPY . .

# 実行
CMD ["uv", "run", "python", "main.py"]
