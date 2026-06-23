# Attestation Sync

- Reduce D. D=8 is too high. Target: D_Push=4

- Have a separate IHave advertisement for a larger set than currently done. D_Pull = 3*D_Push

- Keep sending messages out whenever the queue is empty. 
    - Maintain D_Push sends in parallel. 
    - When the Mesh is completely served, send items to peers in D_Pull. 
    - This helps using the cloud node's bandwidth and keeps the home node's bandwidth as it is. 
    - Send messages in size of 4kB(if possible)

- On receiving an advertisement: fetch the item depending on how frequent the advertisements are. If
  the advertisements are few, fetch the piece.

- Every 500ms, do a sync with a couple of peers. A Sync tells the peers, send me everything you have
  that I don't have. 
    - Must be random to avoid eclipse attacks.  
    - Can also be mitigated by keeping outbound connections in your pool. Though even this can be
      gamed probably.

## Topology

- We can keep the D_Push mesh entirely dependent on farther nodes.
- Heavy D_Pull mesh with the closer nodes.   

## Will this improve the situation? 

- There's a priority over the sends. For every message in our outbound queue: 
    - (topic_id, msg_id) => priority
- We keep 10 outbound items going in parallel.  
    - This is tricky to decide. For a supernode, the 10 outbound in parallel might be too small. 
    - For a home node the 10 outbound in parallel, might be too much. 
    - 1Gbit/s => 50 Homenodes saturated. 
    - 

