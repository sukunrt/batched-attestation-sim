# change the proto structure from: 

message CommitteeAttestationPartsMetadata {
  int32 slot              = 1; // redundant with attestation_data per spec
  bytes attestation_data  = 2;
  bytes available         = 3; // bitmap[committee_size]
  bytes requests          = 4; // bitmap[committee_size]
  bytes attestation_data_hash = 5; // sha256(attestation_data), used after the first full-data send
}

to 

message CommitteeAttestationPartsMetadata {
  int32 slot              = 1; // redundant with attestation_data per spec
  bytes attestation_data  = 2;
  bytes available         = 3; // bitmap[committee_size]
  bytes requests          = 4; // bitmap[committee_size]
  bytes attestation_data_hash = 5; // sha256(attestation_data), used after the first full-data send
  repeated uint32 available_ids         = 6; 
  repeated uint32 requests_ids         = 7; 
}

Then when encoding bitmaps in  partial, partial_priority and attprop: 
send the bitmaps as available_ids, but ensure that you only send the new fields, and not the ones that the peer was previously informed about. 
For attprop this might require some working with the data structure in bitmap_writer, but I'm sure this is simple enough.
