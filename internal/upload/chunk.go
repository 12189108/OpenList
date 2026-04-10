package upload

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	stdpath "path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils/random"
)

const (
	DefaultSessionTTL                 = 24 * time.Hour
	DefaultChunkUploadTimeout         = 15 * time.Minute
	DefaultCompleteUploadLimit        = 2 * time.Hour
	cleanupInterval                   = 10 * time.Minute
	headerMagic                       = "OLCHNK01"
	headerVersion              uint32 = 1
	metaBlockSize                     = 32 * 1024
	chunkStateBlockSize               = 1024
	fileSuffix                        = ".upload"
	preambleSize                      = 24
	DefaultChunkSize                  = 25 * 1024 * 1024
)

type Session struct {
	ID           string               `json:"id"`
	UserID       uint                 `json:"user_id"`
	Username     string               `json:"username"`
	Path         string               `json:"path"`
	Name         string               `json:"name"`
	Size         int64                `json:"size"`
	ChunkSize    int64                `json:"chunk_size"`
	TotalChunks  int                  `json:"total_chunks"`
	Mimetype     string               `json:"mimetype"`
	LastModified int64                `json:"last_modified"`
	CreatedAt    int64                `json:"created_at"`
	UpdatedAt    int64                `json:"updated_at"`
	ExpiresAt    int64                `json:"expires_at"`
	Hashes       map[string]string    `json:"hashes"`
	Chunks       map[int]*ChunkRecord `json:"chunks"`
	HeaderSize   int64                `json:"-"`
	tempPath     string
}

type ChunkRecord struct {
	Index      int               `json:"index"`
	Size       int64             `json:"size"`
	Hashes     map[string]string `json:"hashes"`
	UploadedAt int64             `json:"uploaded_at"`
}

type InitArgs struct {
	User         *model.User
	Path         string
	Size         int64
	ChunkSize    int64
	TotalChunks  int
	Mimetype     string
	LastModified time.Time
	Hashes       map[string]string
}

type UploadChunkArgs struct {
	User      *model.User
	SessionID string
	Index     int
	Size      int64
	Hashes    map[string]string
	Reader    io.Reader
}

type Manager struct {
	root string
	once sync.Once
	mu   sync.Map
}

type sessionMeta struct {
	ID           string            `json:"id"`
	UserID       uint              `json:"user_id"`
	Username     string            `json:"username"`
	Path         string            `json:"path"`
	Name         string            `json:"name"`
	Size         int64             `json:"size"`
	ChunkSize    int64             `json:"chunk_size"`
	TotalChunks  int               `json:"total_chunks"`
	Mimetype     string            `json:"mimetype"`
	LastModified int64             `json:"last_modified"`
	CreatedAt    int64             `json:"created_at"`
	UpdatedAt    int64             `json:"updated_at"`
	ExpiresAt    int64             `json:"expires_at"`
	Hashes       map[string]string `json:"hashes"`
}

type chunkState struct {
	Done      bool   `json:"done"`
	Size      int64  `json:"size"`
	UpdatedAt int64  `json:"updated_at"`
	SHA256    string `json:"sha256,omitempty"`
}

type offsetWriter struct {
	file *os.File
	off  int64
}

func (w *offsetWriter) Write(p []byte) (int, error) {
	n, err := w.file.WriteAt(p, w.off)
	w.off += int64(n)
	return n, err
}

var ChunkUpload = &Manager{}

var invalidUserDirChars = regexp.MustCompile(`[<>:"/\\|?*\x00-\x1f]`)

func (m *Manager) InitSession(args InitArgs) (*Session, error) {
	if args.User == nil {
		return nil, fmt.Errorf("missing user")
	}
	if args.Path == "" {
		return nil, fmt.Errorf("missing path")
	}
	if args.Size <= 0 {
		return nil, fmt.Errorf("invalid file size")
	}
	args.ChunkSize = DefaultChunkSize
	if args.TotalChunks <= 0 {
		return nil, fmt.Errorf("invalid total chunks")
	}
	if got := calcTotalChunks(args.Size, args.ChunkSize); got != args.TotalChunks {
		return nil, fmt.Errorf("total chunks mismatch: expect %d", got)
	}
	hashes := normalizeHashes(args.Hashes)
	if hashes["sha256"] == "" {
		return nil, fmt.Errorf("sha256 is required")
	}
	if err := m.ensureReady(); err != nil {
		return nil, err
	}
	normalizedPath := filepath.ToSlash(args.Path)
	sha256Hash := hashes["sha256"]
	// Try to find existing session for resumable upload
	existing, err := m.findSessionByPathAndHash(args.User, normalizedPath, sha256Hash)
	if err == nil && existing != nil && !isExpired(existing) {
		return existing, nil
	}
	now := time.Now()
	session := &Session{
		ID:           random.String(32),
		UserID:       args.User.ID,
		Username:     args.User.Username,
		Path:         filepath.ToSlash(args.Path),
		Name:         stdpath.Base(filepath.ToSlash(args.Path)),
		Size:         args.Size,
		ChunkSize:    args.ChunkSize,
		TotalChunks:  args.TotalChunks,
		Mimetype:     args.Mimetype,
		LastModified: args.LastModified.UnixMilli(),
		CreatedAt:    now.UnixMilli(),
		UpdatedAt:    now.UnixMilli(),
		ExpiresAt:    now.Add(DefaultSessionTTL).UnixMilli(),
		Hashes:       map[string]string{"sha256": hashes["sha256"]},
		Chunks:       map[int]*ChunkRecord{},
		HeaderSize:   headerSize(args.TotalChunks),
	}
	lock := m.lock(session.ID)
	lock.Lock()
	defer lock.Unlock()
	filePath := m.filePath(args.User, session.ID)
	if err := os.MkdirAll(filepath.Dir(filePath), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	if err := file.Truncate(session.HeaderSize + session.Size); err != nil {
		return nil, err
	}
	session.tempPath = filePath
	if err := writeSessionFile(file, session); err != nil {
		return nil, err
	}
	return session, nil
}

func (m *Manager) GetSession(user *model.User, sessionID string) (*Session, error) {
	if user == nil {
		return nil, fmt.Errorf("missing user")
	}
	if err := m.ensureReady(); err != nil {
		return nil, err
	}
	lock := m.lock(sessionID)
	lock.Lock()
	defer lock.Unlock()
	return m.loadSessionByUser(user, sessionID)
}

func (m *Manager) UploadChunk(args UploadChunkArgs) (*Session, *ChunkRecord, error) {
	if args.User == nil {
		return nil, nil, fmt.Errorf("missing user")
	}
	if args.Reader == nil {
		return nil, nil, fmt.Errorf("missing chunk reader")
	}
	if err := m.ensureReady(); err != nil {
		return nil, nil, err
	}
	lock := m.lock(args.SessionID)
	lock.Lock()
	defer lock.Unlock()
	session, err := m.loadSessionByUser(args.User, args.SessionID)
	if err != nil {
		return nil, nil, err
	}
	if isExpired(session) {
		_ = m.removeSessionLocked(session)
		return nil, nil, fmt.Errorf("upload session expired")
	}
	if args.Index < 0 || args.Index >= session.TotalChunks {
		return nil, nil, fmt.Errorf("invalid chunk index")
	}
	expectedSize := chunkExpectedSize(session, args.Index)
	if args.Size <= 0 {
		args.Size = expectedSize
	}
	if args.Size != expectedSize {
		return nil, nil, fmt.Errorf("chunk size mismatch: expect %d", expectedSize)
	}
	file, err := os.OpenFile(session.tempPath, os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()
	writer := &offsetWriter{file: file, off: session.HeaderSize + int64(args.Index)*session.ChunkSize}
	written, err := utils.CopyWithBuffer(writer, io.LimitReader(args.Reader, expectedSize+1))
	if err != nil {
		return nil, nil, err
	}
	if written != expectedSize {
		return nil, nil, fmt.Errorf("chunk size mismatch: expect %d, got %d", expectedSize, written)
	}
	state := chunkState{Done: true, Size: written, UpdatedAt: time.Now().UnixMilli()}
	if sha256Value := normalizeHashes(args.Hashes)["sha256"]; sha256Value != "" {
		state.SHA256 = sha256Value
	}
	if err := writeChunkState(file, session, args.Index, state); err != nil {
		return nil, nil, err
	}
	touchSession(session)
	if err := writeSessionMeta(file, session); err != nil {
		return nil, nil, err
	}
	record := &ChunkRecord{Index: args.Index, Size: written, Hashes: map[string]string{}, UploadedAt: state.UpdatedAt}
	if state.SHA256 != "" {
		record.Hashes["sha256"] = state.SHA256
	}
	session.Chunks[args.Index] = record
	return session, record, nil
}

func (m *Manager) CompleteReader(user *model.User, sessionID string) (*Session, io.ReadCloser, error) {
	if err := m.ensureReady(); err != nil {
		return nil, nil, err
	}
	lock := m.lock(sessionID)
	lock.Lock()
	defer lock.Unlock()
	session, err := m.loadSessionByUser(user, sessionID)
	if err != nil {
		return nil, nil, err
	}
	if isExpired(session) {
		_ = m.removeSessionLocked(session)
		return nil, nil, fmt.Errorf("upload session expired")
	}
	for i := 0; i < session.TotalChunks; i++ {
		record, ok := session.Chunks[i]
		if !ok || record.Size != chunkExpectedSize(session, i) {
			return nil, nil, fmt.Errorf("chunk %d not uploaded", i)
		}
	}
	file, err := os.OpenFile(session.tempPath, os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, err
	}
	touchSession(session)
	if err := writeSessionMeta(file, session); err != nil {
		file.Close()
		return nil, nil, err
	}
	return session, newTempFileReader(file, session), nil
}

func (m *Manager) RemoveSession(user *model.User, sessionID string) error {
	if err := m.ensureReady(); err != nil {
		return err
	}
	lock := m.lock(sessionID)
	lock.Lock()
	defer lock.Unlock()
	session, err := m.loadSessionByUser(user, sessionID)
	if err != nil {
		return err
	}
	return m.removeSessionLocked(session)
}

func (m *Manager) RemoveSessionByID(userID uint, sessionID string) error {
	if err := m.ensureReady(); err != nil {
		return err
	}
	lock := m.lock(sessionID)
	lock.Lock()
	defer lock.Unlock()
	session, err := m.findSessionByID(userID, sessionID)
	if err != nil {
		return err
	}
	return m.removeSessionLocked(session)
}

func (m *Manager) ensureReady() error {
	var err error
	m.once.Do(func() {
		m.root = chunkRootDir()
		err = os.MkdirAll(m.root, 0o700)
		if err == nil {
			go m.cleanupLoop()
		}
	})
	return err
}

func (m *Manager) cleanupLoop() {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for range ticker.C {
		_ = m.cleanupExpired()
	}
}

func (m *Manager) cleanupExpired() error {
	users, err := os.ReadDir(m.root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	for _, userDir := range users {
		if !userDir.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(m.root, userDir.Name()))
		if err != nil {
			continue
		}
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), fileSuffix) {
				continue
			}
			path := filepath.Join(m.root, userDir.Name(), file.Name())
			session, err := loadSessionFile(path)
			if err != nil || isExpired(session) {
				_ = os.Remove(path)
				m.mu.Delete(strings.TrimSuffix(file.Name(), fileSuffix))
			}
		}
	}
	return nil
}

func (m *Manager) loadSessionByUser(user *model.User, sessionID string) (*Session, error) {
	if user == nil {
		return nil, fmt.Errorf("upload session not found")
	}
	session, err := loadSessionFile(m.filePath(user, sessionID))
	if err != nil {
		return nil, fmt.Errorf("upload session not found")
	}
	if session.UserID != user.ID {
		return nil, fmt.Errorf("upload session not found")
	}
	return session, nil
}

func (m *Manager) loadSessionByUserID(userID uint, username string, sessionID string) (*Session, error) {
	tempUser := &model.User{ID: userID, Username: username}
	session, err := loadSessionFile(m.filePath(tempUser, sessionID))
	if err != nil {
		return nil, fmt.Errorf("upload session not found")
	}
	if session.UserID != userID {
		return nil, fmt.Errorf("upload session not found")
	}
	return session, nil
}

func (m *Manager) findSessionByPathAndHash(user *model.User, path string, sha256Hash string) (*Session, error) {
	userDir := m.userDir(user)
	files, err := os.ReadDir(userDir)
	if err != nil {
		return nil, fmt.Errorf("upload session not found")
	}
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), fileSuffix) {
			continue
		}
		session, err := loadSessionFile(filepath.Join(userDir, f.Name()))
		if err != nil {
			continue
		}
		if session.Path == path && normalizeHashes(session.Hashes)["sha256"] == sha256Hash && !isExpired(session) {
			return session, nil
		}
	}
	return nil, fmt.Errorf("upload session not found")
}

func (m *Manager) findSessionByID(userID uint, sessionID string) (*Session, error) {
	userDirs, err := os.ReadDir(m.root)
	if err != nil {
		return nil, fmt.Errorf("upload session not found")
	}
	for _, userDir := range userDirs {
		if !userDir.IsDir() {
			continue
		}
		session, err := loadSessionFile(filepath.Join(m.root, userDir.Name(), sessionID+fileSuffix))
		if err != nil {
			continue
		}
		if session.UserID == userID {
			return session, nil
		}
	}
	return nil, fmt.Errorf("upload session not found")
}

func (m *Manager) removeSessionLocked(session *Session) error {
	m.mu.Delete(session.ID)
	if session == nil {
		return nil
	}
	return os.Remove(session.tempPath)
}

func (m *Manager) lock(sessionID string) *sync.Mutex {
	value, _ := m.mu.LoadOrStore(sessionID, &sync.Mutex{})
	return value.(*sync.Mutex)
}

func (m *Manager) userDir(user *model.User) string {
	name := "user-" + strconv.FormatUint(uint64(user.ID), 10)
	if user != nil {
		if sanitized := sanitizeUserDirName(user.Username); sanitized != "" {
			name = sanitized
		}
	}
	return filepath.Join(m.root, name)
}

func (m *Manager) filePath(user *model.User, sessionID string) string {
	return filepath.Join(m.userDir(user), sessionID+fileSuffix)
}

func chunkRootDir() string {
	if conf.ConfigPath != "" {
		return filepath.Join(filepath.Dir(conf.ConfigPath), "tmp", "chunk_uploads")
	}
	return filepath.Join(conf.Conf.TempDir, "chunk_uploads")
}

func headerSize(totalChunks int) int64 {
	return int64(preambleSize + metaBlockSize + totalChunks*chunkStateBlockSize)
}

func calcTotalChunks(size, chunkSize int64) int {
	return int((size + chunkSize - 1) / chunkSize)
}

func chunkExpectedSize(session *Session, index int) int64 {
	if index == session.TotalChunks-1 {
		return session.Size - int64(index)*session.ChunkSize
	}
	return session.ChunkSize
}

func touchSession(session *Session) {
	now := time.Now()
	session.UpdatedAt = now.UnixMilli()
	session.ExpiresAt = now.Add(DefaultSessionTTL).UnixMilli()
}

func isExpired(session *Session) bool {
	return session == nil || time.Now().UnixMilli() > session.ExpiresAt
}

func writeSessionFile(file *os.File, session *Session) error {
	if err := writePreamble(file, session.HeaderSize); err != nil {
		return err
	}
	if err := writeSessionMeta(file, session); err != nil {
		return err
	}
	zero := make([]byte, chunkStateBlockSize)
	for i := 0; i < session.TotalChunks; i++ {
		if _, err := file.WriteAt(zero, chunkStateOffset(i)); err != nil {
			return err
		}
	}
	return nil
}

func writePreamble(file *os.File, hdrSize int64) error {
	buf := make([]byte, preambleSize)
	copy(buf[:8], []byte(headerMagic))
	binary.LittleEndian.PutUint32(buf[8:12], headerVersion)
	binary.LittleEndian.PutUint64(buf[12:20], uint64(hdrSize))
	binary.LittleEndian.PutUint32(buf[20:24], metaBlockSize)
	_, err := file.WriteAt(buf, 0)
	return err
}

func writeSessionMeta(file *os.File, session *Session) error {
	meta := sessionMeta{ID: session.ID, UserID: session.UserID, Username: session.Username, Path: session.Path, Name: session.Name, Size: session.Size, ChunkSize: session.ChunkSize, TotalChunks: session.TotalChunks, Mimetype: session.Mimetype, LastModified: session.LastModified, CreatedAt: session.CreatedAt, UpdatedAt: session.UpdatedAt, ExpiresAt: session.ExpiresAt, Hashes: map[string]string{"sha256": session.Hashes["sha256"]}}
	data, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	if len(data) > metaBlockSize {
		return fmt.Errorf("session metadata too large")
	}
	buf := make([]byte, metaBlockSize)
	copy(buf, data)
	_, err = file.WriteAt(buf, preambleSize)
	return err
}

func writeChunkState(file *os.File, session *Session, index int, state chunkState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if len(data) > chunkStateBlockSize {
		return fmt.Errorf("chunk state too large")
	}
	buf := make([]byte, chunkStateBlockSize)
	copy(buf, data)
	_, err = file.WriteAt(buf, chunkStateOffset(index))
	return err
}

func loadSessionFile(path string) (*Session, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	hdrSize, err := readPreamble(file)
	if err != nil {
		return nil, err
	}
	meta, err := readSessionMeta(file)
	if err != nil {
		return nil, err
	}
	session := &Session{ID: meta.ID, UserID: meta.UserID, Username: meta.Username, Path: meta.Path, Name: meta.Name, Size: meta.Size, ChunkSize: meta.ChunkSize, TotalChunks: meta.TotalChunks, Mimetype: meta.Mimetype, LastModified: meta.LastModified, CreatedAt: meta.CreatedAt, UpdatedAt: meta.UpdatedAt, ExpiresAt: meta.ExpiresAt, Hashes: map[string]string{"sha256": normalizeHashes(meta.Hashes)["sha256"]}, Chunks: map[int]*ChunkRecord{}, HeaderSize: hdrSize, tempPath: path}
	for i := 0; i < session.TotalChunks; i++ {
		state, err := readChunkState(file, i)
		if err != nil {
			return nil, err
		}
		if !state.Done {
			continue
		}
		record := &ChunkRecord{Index: i, Size: state.Size, Hashes: map[string]string{}, UploadedAt: state.UpdatedAt}
		if state.SHA256 != "" {
			record.Hashes["sha256"] = state.SHA256
		}
		session.Chunks[i] = record
	}
	return session, nil
}

func readPreamble(file *os.File) (int64, error) {
	buf := make([]byte, preambleSize)
	if _, err := file.ReadAt(buf, 0); err != nil {
		return 0, err
	}
	if string(buf[:8]) != headerMagic {
		return 0, fmt.Errorf("invalid upload temp file")
	}
	if binary.LittleEndian.Uint32(buf[8:12]) != headerVersion {
		return 0, fmt.Errorf("unsupported upload temp file version")
	}
	return int64(binary.LittleEndian.Uint64(buf[12:20])), nil
}

func readSessionMeta(file *os.File) (*sessionMeta, error) {
	buf := make([]byte, metaBlockSize)
	if _, err := file.ReadAt(buf, preambleSize); err != nil {
		return nil, err
	}
	buf = trimZero(buf)
	var meta sessionMeta
	if err := json.Unmarshal(buf, &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

func readChunkState(file *os.File, index int) (*chunkState, error) {
	buf := make([]byte, chunkStateBlockSize)
	if _, err := file.ReadAt(buf, chunkStateOffset(index)); err != nil {
		return nil, err
	}
	buf = trimZero(buf)
	if len(buf) == 0 {
		return &chunkState{}, nil
	}
	var state chunkState
	if err := json.Unmarshal(buf, &state); err != nil {
		return nil, err
	}
	return &state, nil
}

func chunkStateOffset(index int) int64 {
	return int64(preambleSize+metaBlockSize) + int64(index*chunkStateBlockSize)
}

func trimZero(buf []byte) []byte {
	for i, b := range buf {
		if b == 0 {
			return buf[:i]
		}
	}
	return buf
}

type tempFileReader struct {
	file     *os.File
	reader   *io.SectionReader
	expected string
	hash     hashWriter
	readSize int64
	total    int64
}

type hashWriter interface {
	Write([]byte) (int, error)
	Sum([]byte) []byte
}

func newTempFileReader(file *os.File, session *Session) io.ReadCloser {
	return &tempFileReader{file: file, reader: io.NewSectionReader(file, session.HeaderSize, session.Size), expected: session.Hashes["sha256"], hash: sha256.New(), total: session.Size}
}

func (r *tempFileReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.readSize += int64(n)
		_, _ = r.hash.Write(p[:n])
	}
	if err == io.EOF {
		if r.readSize != r.total {
			return 0, fmt.Errorf("file size mismatch")
		}
		if hex.EncodeToString(r.hash.Sum(nil)) != r.expected {
			return 0, fmt.Errorf("sha256 mismatch")
		}
	}
	return n, err
}

func (r *tempFileReader) Close() error { return r.file.Close() }

func normalizeHashes(h map[string]string) map[string]string {
	dst := map[string]string{}
	if h == nil {
		return dst
	}
	if value := strings.ToLower(strings.TrimSpace(h["sha256"])); value != "" {
		dst["sha256"] = value
	}
	return dst
}

func BuildStream(session *Session, reader io.ReadCloser) *stream.FileStream {
	hashes := map[*utils.HashType]string{}
	if value := normalizeHashes(session.Hashes)["sha256"]; value != "" {
		hashes[utils.SHA256] = value
	}
	return &stream.FileStream{Obj: &model.Object{Name: session.Name, Size: session.Size, Modified: time.UnixMilli(session.LastModified), HashInfo: utils.NewHashInfoByMap(hashes)}, Reader: reader, Mimetype: session.Mimetype, Closers: utils.Closers{reader}}
}

func UploadedIndexes(session *Session) []int {
	if session == nil {
		return []int{}
	}
	indexes := make([]int, 0, len(session.Chunks))
	for index := range session.Chunks {
		indexes = append(indexes, index)
	}
	slices.Sort(indexes)
	return indexes
}

func RemainingIndexes(session *Session) []int {
	if session == nil {
		return []int{}
	}
	remaining := make([]int, 0, session.TotalChunks-len(session.Chunks))
	for i := 0; i < session.TotalChunks; i++ {
		if _, ok := session.Chunks[i]; !ok {
			remaining = append(remaining, i)
		}
	}
	return remaining
}

func ParseHashes(values map[string][]string, prefix string) map[string]string {
	hashes := map[string]string{}
	if value := strings.TrimSpace(firstValue(values[prefix+"sha256"])); value != "" {
		hashes["sha256"] = value
	}
	return hashes
}

func firstValue(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func ParseChunkIndex(value string) (int, error) {
	index, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return 0, fmt.Errorf("invalid chunk index")
	}
	return index, nil
}

func sanitizeUserDirName(name string) string {
	name = strings.TrimSpace(name)
	name = invalidUserDirChars.ReplaceAllString(name, "_")
	name = strings.TrimRight(name, ". ")
	if name == "" {
		return ""
	}
	return name
}
