package handles

import (
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func TestChunkInitUsesLiteralJSONPath(t *testing.T) {
	user := &model.User{ID: 7, BasePath: "/team/a"}
	for _, requested := range []string{"/100%.txt", "/literal%2Fname.txt", "/normal.txt"} {
		joined, err := decodeAndJoinUserPath(user, requested)
		if err != nil {
			t.Fatal(err)
		}
		if joined != user.BasePath+requested {
			t.Fatalf("path %q became %q", requested, joined)
		}
	}
}
