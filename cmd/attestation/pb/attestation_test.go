package pb

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// TestAttPropControlRoundTrip checks that an AttPropControl carrying a mix of
// graft/prune items for each mesh survives marshal/unmarshal intact.
func TestAttPropControlRoundTrip(t *testing.T) {
	orig := &AttPropControl{
		Items: []*AttPropControlItem{
			{Op: AttPropMeshOp_GRAFT, Mesh: AttPropMesh_PUSH},
			{Op: AttPropMeshOp_PRUNE, Mesh: AttPropMesh_BITMAP},
			{Op: AttPropMeshOp_PRUNE, Mesh: AttPropMesh_FULL},
		},
	}

	encoded, err := proto.Marshal(orig)
	require.NoError(t, err)

	var got AttPropControl
	require.NoError(t, proto.Unmarshal(encoded, &got))

	require.Len(t, got.Items, len(orig.Items))
	for i, item := range got.Items {
		assert.Equal(t, orig.Items[i].Op, item.Op, "item %d op", i)
		assert.Equal(t, orig.Items[i].Mesh, item.Mesh, "item %d mesh", i)
	}
}

// TestAttPropControlEmpty checks the zero value round-trips to an empty list.
func TestAttPropControlEmpty(t *testing.T) {
	encoded, err := proto.Marshal(&AttPropControl{})
	require.NoError(t, err)

	var got AttPropControl
	require.NoError(t, proto.Unmarshal(encoded, &got))
	assert.Empty(t, got.Items)
}
