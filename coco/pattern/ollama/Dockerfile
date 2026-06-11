FROM ollama/ollama:latest

# Model to pull on first start (override at runtime with -e MODEL=...)
ENV MODEL=qwen2.5-coder:3b
ENV OLLAMA_HOST=0.0.0.0:11434

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 11434

ENTRYPOINT ["/entrypoint.sh"]
