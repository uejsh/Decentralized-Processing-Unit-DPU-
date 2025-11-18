# decent_ai_shards.py
import asyncio
import torch
import torch.nn as nn
import torch.nn.functional as F
import hashlib
import random
import time

torch.manual_seed(42)
random.seed(42)

# ---------------------
# Config
# ---------------------
D_MODEL = 32
NUM_HEADS = 4
D_K = D_MODEL // NUM_HEADS
SEQ_LEN = 6
BATCH = 1

SHARD_HEAD_SPLIT = [2, 2]  # two shards: first owns 2 heads, second owns 2 heads
LR = 1e-3
EPOCHS = 40
VERIFICATION_PROB = 0.12  # chance a neighbor rechecks an output
STAKE_REWARD = 1.0

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

# ---------------------
# Utilities
# ---------------------
def tensor_hash(t: torch.Tensor) -> str:
    # Deterministic hash for a tensor (float32)
    b = t.detach().cpu().numpy().tobytes()
    return hashlib.sha256(b).hexdigest()

# ---------------------
# Shard Node: owns some attention heads
# ---------------------
class HeadShard:
    def __init__(self, d_model, heads, d_k, lr=LR, name="shard"):
        self.name = name
        self.heads = heads
        self.d_model = d_model
        self.d_k = d_k

        # For simplicity, each shard contains its own linear projections for Q,K,V for owned heads
        # We'll implement them as a single linear that projects to heads * d_k, similarly for K and V
        self.Wq = nn.Linear(d_model, heads * d_k, bias=False).to(device)
        self.Wk = nn.Linear(d_model, heads * d_k, bias=False).to(device)
        self.Wv = nn.Linear(d_model, heads * d_k, bias=False).to(device)

        # small FFN to apply after concat of heads (local map)
        self.local_ffn = nn.Sequential(
            nn.Linear(heads * d_k, heads * d_k),
            nn.ReLU(),
            nn.Linear(heads * d_k, heads * d_k)
        ).to(device)

        self.opt = torch.optim.Adam(self.parameters(), lr=lr)
        self.stake = 0.0  # earned credits

    def parameters(self):
        return list(self.Wq.parameters()) + list(self.Wk.parameters()) + list(self.Wv.parameters()) + list(self.local_ffn.parameters())

    async def forward(self, x):
        # x: (batch, seq_len, d_model)
        # Returns: (batch, seq_len, heads * d_k)
        # simulate small async latency
        await asyncio.sleep(random.uniform(0.001, 0.005))
        B, N, D = x.shape
        q = self.Wq(x)  # B, N, heads*d_k
        k = self.Wk(x)
        v = self.Wv(x)
        # reshape to (B, heads, N, d_k)
        q = q.view(B, N, self.heads, self.d_k).permute(0, 2, 1, 3)
        k = k.view(B, N, self.heads, self.d_k).permute(0, 2, 1, 3)
        v = v.view(B, N, self.heads, self.d_k).permute(0, 2, 1, 3)

        # scaled dot-product attention
        scores = torch.matmul(q, k.transpose(-2, -1)) / (self.d_k ** 0.5)  # B, heads, N, N
        attn = torch.softmax(scores, dim=-1)
        out = torch.matmul(attn, v)  # B, heads, N, d_k

        # bring to (B, N, heads * d_k)
        out = out.permute(0, 2, 1, 3).contiguous().view(B, N, self.heads * self.d_k)

        # local ffn
        out = self.local_ffn(out)

        # store intermediate tensors required for potential verification (keep on CPU copy)
        self._last_out = out.detach().cpu().clone()
        self._last_input = x.detach().cpu().clone()

        return out

    async def backward(self, grad_out):
        # grad_out: (B, N, heads*d_k) gradient coming from aggregator
        # We simply perform a backward step on local params with respect to this grad_out.
        await asyncio.sleep(random.uniform(0.001, 0.005))
        # convert to torch, ensure requires grad for internal computations
        # We will do manual backward: compute loss = (local_out - target_local_out) * grad_out trick
        # Simpler: compute a scalar surrogate loss by dot-product with grad_out to push gradients correctly.
        # We'll forward again to get current output with autograd tracking.
        # NOTE: this double-forwards but is simple and OK for prototype.
        self.opt.zero_grad()
        # rebuild a forward with gradient tracking
        x = self._last_input.to(device)
        x.requires_grad = True
        out = self.Wq(x)  # B,N,heads*d_k
        # recompute full path (we need to compute whole chain to get grad)
        # reuse forward code but with autograd
        B, N, _ = x.shape
        q = self.Wq(x).view(B,N,self.heads,self.d_k).permute(0,2,1,3)
        k = self.Wk(x).view(B,N,self.heads,self.d_k).permute(0,2,1,3)
        v = self.Wv(x).view(B,N,self.heads,self.d_k).permute(0,2,1,3)
        scores = torch.matmul(q, k.transpose(-2,-1)) / (self.d_k ** 0.5)
        attn = torch.softmax(scores, dim=-1)
        out = torch.matmul(attn, v).permute(0,2,1,3).contiguous().view(B,N,self.heads*self.d_k)
        out = self.local_ffn(out)

        # Create surrogate scalar loss: sum(out * grad_out)
        grad_tensor = grad_out.to(device)
        surrogate = torch.sum(out * grad_tensor)
        surrogate.backward()
        self.opt.step()
        # Return a gradient to previous aggregator (approx): compute d surrogate / d x
        # This provides a gradient w.r.t. inputs to allow upstream shards to update if needed.
        if x.grad is None:
            return torch.zeros_like(grad_tensor)  # fallback
        grad_input = x.grad.detach()
        return grad_input  # shape (B,N,d_model)

    async def hash_output(self):
        # return hash of last output (numpy bytes)
        await asyncio.sleep(0)  # keep async signature
        return hashlib.sha256(self._last_out.numpy().tobytes()).hexdigest()

# ---------------------
# Aggregator: collects head-outputs from shards and concatenates into full model tensor
# ---------------------
class Aggregator:
    def __init__(self, d_model, head_splits, d_k):
        self.d_model = d_model
        self.head_splits = head_splits
        self.d_k = d_k

    async def aggregate(self, shard_outs):
        # shard_outs: list of tensors each (B,N,heads_i * d_k)
        # concatenates across head dimension to (B,N, total_heads * d_k) == (B,N,d_model)
        await asyncio.sleep(0)  # placeholder for async
        out = torch.cat(shard_outs, dim=-1)
        return out  # B, N, d_model

# ---------------------
# Simple neighbor verifier and staking system
# ---------------------
class Network:
    def __init__(self, shards):
        self.shards = shards
        self.rewards = {s.name: 0.0 for s in shards}
        self.slash_log = []

    async def verify_and_reward(self, shard, out_hash):
        # With some probability, a neighbor recomputes forward and checks hash
        if random.random() < VERIFICATION_PROB:
            # pick a neighbor (simulate via another shard who has similar code)
            neighbor = random.choice([s for s in self.shards if s.name != shard.name])
            # neighbor recomputes using shard._last_input but with neighbor's local weights? In real system they'd have the same input.
            # Here we simulate by recomputing using shard's own stored input and weights to emulate neighbor verification.
            recomputed = (await shard.forward(shard._last_input.to(device))).detach().cpu()
            recomputed_hash = hashlib.sha256(recomputed.numpy().tobytes()).hexdigest()
            ok = (recomputed_hash == out_hash)
            if ok:
                self.rewards[shard.name] += STAKE_REWARD
                return True
            else:
                # slash: penalize stake (for prototype record)
                self.slash_log.append((shard.name, "failed_verification"))
                self.rewards[shard.name] -= STAKE_REWARD * 2
                return False
        else:
            # no verification -> nominal reward
            self.rewards[shard.name] += STAKE_REWARD * 0.2
            return True

# ---------------------
# Tiny model training loop using async pipeline
# ---------------------
async def run_training():
    # Build shards
    shards = []
    shard_id = 0
    for i, heads in enumerate(SHARD_HEAD_SPLIT):
        s = HeadShard(D_MODEL, heads, D_K, lr=LR, name=f"shard_{i}")
        shards.append(s)
    aggregator = Aggregator(D_MODEL, SHARD_HEAD_SPLIT, D_K)
    net = Network(shards)

    # Small synthetic dataset: sequence of sinusoidal vectors -> target is random linear projection
    # For demo, create random X and target Y
    N_SAMPLES = 32
    X = torch.randn(N_SAMPLES, SEQ_LEN, D_MODEL, device=device)
    # target: apply random linear map
    true_map = torch.randn(D_MODEL, 2, device=device)
    Y = torch.randn(N_SAMPLES, SEQ_LEN, 2, device=device)  # small target

    # We'll attach a small final head (simulating model head) to produce Y from aggregated out
    final_head = nn.Linear(D_MODEL, 2).to(device)
    final_opt = torch.optim.Adam(final_head.parameters(), lr=LR)

    batch = 4
    for epoch in range(EPOCHS):
        perm = list(range(N_SAMPLES))
        random.shuffle(perm)
        epoch_loss = 0.0
        for i in range(0, N_SAMPLES, batch):
            batch_idx = perm[i:i+batch]
            tasks = []
            batch_losses = []
            # for each sample in batch create pipeline
            for idx in batch_idx:
                x = X[idx:idx+1]  # (1, SEQ_LEN, D_MODEL)
                y = Y[idx:idx+1]  # (1, SEQ_LEN, 2)
                tasks.append(asyncio.create_task(process_sample(x, y, shards, aggregator, net, final_head, final_opt)))
            results = await asyncio.gather(*tasks)
            for loss in results:
                batch_losses.append(loss)
            epoch_loss += sum(batch_losses)
        avg_loss = epoch_loss / (N_SAMPLES / batch)
        print(f"Epoch {epoch+1}/{EPOCHS} - avg batch loss: {avg_loss:.6f}")

    print("Final stakes/rewards:", net.rewards)
    print("Slash log:", net.slash_log)

async def process_sample(x, y, shards, aggregator, net, final_head, final_opt):
    # Forward at each shard concurrently (they can compute in parallel because they only need the input x)
    # In a real shard-by-shard pipeline you might need previous layer outputs; here shards compute head outputs independently
    shard_outs = []
    # Kick off all shard forward tasks concurrently
    forward_tasks = [asyncio.create_task(s.forward(x)) for s in shards]
    for t in forward_tasks:
        out = await t
        shard_outs.append(out)

    # Each shard has stored _last_out; aggregate them
    aggregated = await aggregator.aggregate(shard_outs)  # (B,N,D_MODEL)
    # Final head prediction (compute loss)
    pred = final_head(aggregated)  # (B,N,2)
    loss = F.mse_loss(pred, y)

    # Backprop through final head
    final_opt.zero_grad()
    loss.backward(retain_graph=True)
    final_opt.step()

    # Compute gradient at aggregated output (dLoss/dAggregated)
    # aggregated requires grad? No, final_head used aggregated as input but aggregated has no grad attached.
    # We'll compute gradient manually using linear layer weights:
    # dLoss/dAggregated = dLoss/dPred @ W_final^T
    with torch.no_grad():
        # Simple linearization: pred = aggregated @ W^T + b; so gradient wrt aggregated = dLoss/dPred @ W
        # But dLoss/dPred = 2*(pred - y)/batch_size typically; here use autograd to compute dPred
        # Use autograd trick: recompute pred with grad enabled to get dLoss/dAggregated
        aggregated.requires_grad_(True)
        pred2 = final_head(aggregated)
        loss2 = F.mse_loss(pred2, y)
    grad_agg = torch.autograd.grad(loss2, aggregated)[0].detach()  # (B,N,D_MODEL)
    aggregated.requires_grad_(False)

    # Now call backward on shards in reverse order to allow local updates.
    # Each shard expects a gradient for its local output shape (B,N,heads*d_k).
    # We need to split grad_agg into the shards' head dims
    # aggregated shape: (B,N,D_MODEL) with D_MODEL == sum_heads * d_k
    grads_for_shards = []
    dims = [h * D_K for h in SHARD_HEAD_SPLIT]
    idx = 0
    for d in dims:
        grads_for_shards.append(grad_agg[:, :, idx:idx+d])
        idx += d

    # Now for each shard, run backward with its corresponding grad, and optionally verify output
    backward_tasks = []
    for s, g in zip(shards, grads_for_shards):
        # neighbor verification step (hash)
        h = await s.hash_output()
        ok = await net.verify_and_reward(s, h)
        # Even if verification fails we still perform backward (for prototype), but in real net we'd slash or reassign.
        backward_tasks.append(asyncio.create_task(s.backward(g)))
    # wait all updates
    res = await asyncio.gather(*backward_tasks)
    # Return scalar loss
    return float(loss.detach().cpu().item())

# ---------------------
# Entrypoint
# ---------------------
if __name__ == "__main__":
    import sys
    start = time.time()
    asyncio.run(run_training())
    print("Elapsed:", time.time() - start)