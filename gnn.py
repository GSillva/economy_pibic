import numpy as np
import pandas as pd

from sklearn.model_selection import GroupShuffleSplit

from torch_geometric.nn import GCNConv
import torch.nn.functional as F

from torch_geometric.data import Data

df = pd.read_parquet("dataset_frases_embeddings.parquet")

X = np.vstack(df["embedding"].values)
y = df["label"].values
groups = df["id_doc"].values

gss = GroupShuffleSplit(test_size=0.2, n_splits=1, random_state=42)
train_idx, test_idx = next(gss.split(X, y, groups))

X_train, X_test = X[train_idx], X[test_idx]
y_train, y_test = y[train_idx], y[test_idx]

df_train = df.iloc[train_idx].copy()
df_test  = df.iloc[test_idx].copy()


import torch
import torch.nn as nn

class AttentionPooling(nn.Module):
    def __init__(self, dim):
        super().__init__()
        self.att = nn.Linear(dim, 1)

    def forward(self, x):
        # x: [num_frases, dim]
        scores = self.att(x)
        weights = torch.softmax(scores, dim=0)
        pooled = (weights * x).sum(dim=0)
        return pooled

embedding_dim = X.shape[1]
pooling = AttentionPooling(embedding_dim)

def build_doc_embeddings(df, pooling):
    doc_embeddings = []
    doc_labels = []
    doc_ids = []

    for doc_id, group in df.groupby("id_doc"):
        sentence_embs = torch.tensor(
            np.vstack(group["embedding"].values),
            dtype=torch.float
        )

        doc_emb = pooling(sentence_embs)
        doc_embeddings.append(doc_emb)

        doc_labels.append(group["label"].iloc[0])
        doc_ids.append(doc_id)

    return (
        torch.stack(doc_embeddings),
        torch.tensor(doc_labels),
        doc_ids
    )

X_doc_train, y_doc_train, train_doc_ids = build_doc_embeddings(df_train, pooling)
X_doc_test,  y_doc_test,  test_doc_ids  = build_doc_embeddings(df_test,  pooling)

from sklearn.metrics.pairwise import cosine_similarity

def build_knn_graph(X, k=5):
    sim = cosine_similarity(X.detach().numpy())
    edges = []

    for i in range(sim.shape[0]):
        neighbors = np.argsort(sim[i])[-(k+1):]  # inclui ele mesmo
        for j in neighbors:
            if i != j:
                edges.append([i, j])

    return torch.tensor(edges, dtype=torch.long).t()

edge_index = build_knn_graph(X_doc_train, k=5)

class GNN(torch.nn.Module):
    def __init__(self, in_dim, hidden_dim):
        super().__init__()
        self.conv1 = GCNConv(in_dim, hidden_dim)
        self.conv2 = GCNConv(hidden_dim, 2)

    def forward(self, data):
        x, edge_index = data.x, data.edge_index
        x = self.conv1(x, edge_index)
        x = F.relu(x)
        x = self.conv2(x, edge_index)
        return x
    

model = GNN(in_dim=embedding_dim, hidden_dim=512)
optimizer = torch.optim.Adam(
    list(model.parameters()) + list(pooling.parameters()),
    lr=1e-3
)

criterion = nn.CrossEntropyLoss()

data = Data(
    x=X_doc_train,
    edge_index=edge_index,
    y=y_doc_train
)

def build_doc_embeddings(df, pooling):
    doc_embeddings = []
    doc_labels = []

    for _, group in df.groupby("id_doc"):
        sentence_embs = torch.tensor(
            np.vstack(group["embedding"].values),
            dtype=torch.float
        )

        doc_emb = pooling(sentence_embs)
        doc_embeddings.append(doc_emb)
        doc_labels.append(group["label"].iloc[0])

    return torch.stack(doc_embeddings), torch.tensor(doc_labels)

for epoch in range(10000):
    pooling.train()
    model.train()
    optimizer.zero_grad()

    X_doc_train, y_doc_train = build_doc_embeddings(df_train, pooling)

    edge_index = build_knn_graph(X_doc_train, k=5)

    train_data = Data(
        x=X_doc_train,
        edge_index=edge_index,
        y=y_doc_train
    )

    out = model(train_data)
    loss = criterion(out, train_data.y)

    loss.backward()
    optimizer.step()

    if epoch % 10 == 0:
        print(f"Epoch {epoch:05d} | Loss {loss.item():.4f}")


X_test_doc, y_test_doc, _ = build_doc_embeddings(df_test, pooling)

model.eval()
with torch.no_grad():
    test_data = Data(
        x=X_test_doc,
        edge_index=build_knn_graph(X_test_doc, k=5)
    )

    logits = model(test_data)
    preds = logits.argmax(dim=1)


acc = (preds == y_test_doc).float().mean()
print("Test accuracy:", acc.item())
