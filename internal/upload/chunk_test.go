package upload

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/model"
)

func testChunkManager(root string) *Manager {
	m := &Manager{root: root}
	// Tests own a temporary root and do not start the process-wide cleanup loop.
	m.once.Do(func() {})
	return m
}

func testChunkArgs(data []byte) InitArgs {
	hash := sha256.Sum256(data)
	return InitArgs{
		User: &model.User{ID: 7, Username: "tester"}, Path: "/local/test.bin",
		Size: int64(len(data)), ChunkSize: DefaultChunkSize,
		TotalChunks: calcTotalChunks(int64(len(data)), DefaultChunkSize),
		Mimetype:    "application/octet-stream", LastModified: time.Unix(1700000000, 0),
		Hashes: map[string]string{"sha256": hex.EncodeToString(hash[:])},
	}
}

func sendTestChunk(t *testing.T, m *Manager, args InitArgs, session *Session, index int, data []byte) {
	t.Helper()
	if _, _, err := m.UploadChunk(UploadChunkArgs{
		User: args.User, SessionID: session.ID, Index: index,
		Size: int64(len(data)), Reader: bytes.NewReader(data),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPersistentChunkResumeAfterManagerRestart(t *testing.T) {
	root := t.TempDir()
	data := bytes.Repeat([]byte{0x37}, DefaultChunkSize+17)
	args := testChunkArgs(data)
	m := testChunkManager(root)
	session, err := m.InitSession(args)
	if err != nil {
		t.Fatal(err)
	}
	sendTestChunk(t, m, args, session, 0, data[:DefaultChunkSize])
	if _, _, err := m.CompleteReader(args.User, session.ID); err == nil {
		t.Fatal("completion accepted an incomplete upload")
	}
	// A fresh manager has no in-memory sessions or locks from the first attempt.
	restarted := testChunkManager(root)
	resumed, err := restarted.InitSession(args)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.ID != session.ID || len(resumed.Chunks) != 1 {
		t.Fatalf("lost persisted session: id=%s chunks=%d", resumed.ID, len(resumed.Chunks))
	}
	sendTestChunk(t, restarted, args, resumed, 1, data[DefaultChunkSize:])
	_, reader, err := restarted.CompleteReader(args.User, resumed.ID)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, data) {
		t.Fatal("resumed file differs from original")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal("reader close must be idempotent:", err)
	}
	if err := restarted.RemoveSession(args.User, resumed.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.GetSession(args.User, resumed.ID); err == nil {
		t.Fatal("removed session remains accessible")
	}
}

func TestChunkCompletionRejectsCorruptedDataBeforeDriverHandoff(t *testing.T) {
	m := testChunkManager(t.TempDir())
	data := []byte("correct file")
	args := testChunkArgs(data)
	session, err := m.InitSession(args)
	if err != nil {
		t.Fatal(err)
	}
	sendTestChunk(t, m, args, session, 0, bytes.Repeat([]byte{'x'}, len(data)))
	if _, reader, err := m.CompleteReader(args.User, session.ID); err == nil {
		_ = reader.Close()
		t.Fatal("corrupt upload reached the storage reader")
	}
	// Repair the chunk and retry using the original persisted session.
	sendTestChunk(t, m, args, session, 0, data)
	_, reader, err := m.CompleteReader(args.User, session.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := io.CopyN(io.Discard, reader, int64(len(data))); err != nil {
		t.Fatal(err)
	}
}

func TestChunkSessionOwnershipAndMetadata(t *testing.T) {
	m := testChunkManager(t.TempDir())
	args := testChunkArgs([]byte("file"))
	session, err := m.InitSession(args)
	if err != nil {
		t.Fatal(err)
	}
	other := &model.User{ID: 8, Username: args.User.Username}
	if _, err := m.GetSession(other, session.ID); err == nil {
		t.Fatal("another user accessed the session")
	}
	otherArgs := args
	otherArgs.User = other
	otherSession, err := m.InitSession(otherArgs)
	if err != nil {
		t.Fatal(err)
	}
	if otherSession.ID == session.ID {
		t.Fatal("init resumed another user's session")
	}
	args.Size++
	if _, err := m.InitSession(args); err == nil {
		t.Fatal("init reused a session with incompatible size")
	}
	if err := m.RemoveSession(other, session.ID); err == nil {
		t.Fatal("another user deleted the session")
	}
	if _, err := os.Stat(session.tempPath); err != nil {
		t.Fatal("owner's session file was removed:", err)
	}
}
