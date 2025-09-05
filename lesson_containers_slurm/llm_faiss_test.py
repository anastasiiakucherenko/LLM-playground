from transformers import AutoTokenizer, AutoModel
import torch
import faiss
import numpy as np

tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

# Encode a test sentence
text = "Grace Hopper was a pioneer in computer science."
inputs = tokenizer(text, return_tensors="pt")
with torch.no_grad():
    embeddings = model(**inputs).last_hidden_state.mean(dim=1).numpy()

# Create FAISS index
index = faiss.IndexFlatL2(embeddings.shape[1])
index.add(embeddings)

# Search (query with same text)
D, I = index.search(embeddings, k=1)
print(f"Distance: {D}, Index: {I}")

