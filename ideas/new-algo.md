# Attestation Broadcast

New protocol for broadcasting attestations. Lot of the details are the same as the existing gossipsub partial messages implementation but the substrate is changed from gossipsub to a new protocol. 

Features of the new Protocol(single topic only)
- Two streams per peer: 
  - Bitmap Stream: sends our set bitmap
  - Data Stream: actually forwards the data
- A peer has three states:
  - Connected 
  - Bitmap Mesh: 
  - Push Mesh: 
  
The whole idea is that we reduce the Push Mesh to 1/2 and let the BitmapMesh transfer information to senders.

On the sending side: 
  - Once we receive 20/30 attestations: we send out our bitmap update to all our BitmapMesh peers. 
  - And just like in partial messages we send out data on the PushMesh every 20ms.
  - Now if we have space in the sendqueue: 
    - We see the least frequent item in our set. This is least frequent compared to the Bitmaps that we've received from our peers. And we push the message out.

On the receiving side:
  - Once we recieve 20/30 attestations: we send out our bitmap update to all our BitmapMesh peers.


## Protocol

### Topology:
Peers when connecting to other peers first try to fill up their PushMesh and then try to fill up their BitmapMesh. 
1. Graft: PushMesh
2. Graft: BitmapMesh

On receiving: Graft: PushMesh
  - if there's a slot available, we graft it. Dlow -> Dhigh
  - otherwise: if there's a slot in BitmapMesh send a Prune: BitmapMesh
  - if there's no slot in bitmap mesh: send prune: Full 

On receiving: Graft: BitmapMesh
  - if there's a slot in bitmap mesh, graft it to bitmap mesh. DBitmap low -> DBitmap High
  - if there's no slot in the bitmap mesh, send a Prune:

Prunes have backoff just like in gossipsub. You'll have to handle backoff for both push and bitmap here. A prune full is backoff for both push and bitmap.


### Sending messages: Push Mesh
There's a tick that goes on every TICK_INTERVAL: 20ms as partial messages. 
Here simply push the highest priority message that you have for the peer to the queue. 
The Send Queue will tick back when there aren't enough messages for our push mesh peers in the queue. On receiving this ping, enqueue the next highest priority message to the PR. This priority is decided based on the partial priority logic.  
The SendQueue will provide a method like AllFreeMeshPeers

### Sending messages: Extra
If we've already *sent* all the messages pending for push mesh and there are slots in the send queues:
Push the least frequent message that we have to one of the bitmap peers. Repeat this process till we have some slots in the send queues. Here the SendQueue will provied a method AllFreeBitmapPeers


### Send Queue: 
This is where the process gets much better than gossipsub. We don't just queue all messages to the peer. What we do is that we have a PeerSender.  
This peer sender ensures:
0. Bitmap only messages are sent out immediately. 
1. All mesh peers are prioritised when sending messages
2. When a data message is being sent to the push mesh peers, we wait before enqueuing another message to the queue. 
3. When a push mesh data send completes, if there's another push mesh data send still enqueued, we send that first. 
4. Once there are no outstanding push mesh data sends, we pick up a data message that we have for bitmap peer. If we have no such data messages for the bitmap peer, we ping the main control loop to provide us with some message.
5. At a time we aim to have only D parallel data sends happening. (Maybe we should make it 2*D? Keep it configurable)

- Here the send *completes* when the write to the libp2p stream ends. So that we can make another write on the wire.
