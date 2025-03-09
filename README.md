1. Make sure Docker is running
2. Set Enviorment Variables

export LLAMA_STACK_PORT=5001
export TOGETHER_API_KEY=...
export INFERENCE_MODEL=meta-llama/Llama-3.1-8B-Instruct

4. Run Docker Image

LLAMA_STACK_PORT=5001
docker run \
  -it \
  -p $LLAMA_STACK_PORT:$LLAMA_STACK_PORT \
  llamastack/distribution-together \
  --port $LLAMA_STACK_PORT \
  --env TOGETHER_API_KEY=$TOGETHER_API_KEY

5. make sure XML files folder destination is set in patent.py
6. Set user query to preffered query
7. python patent.py
