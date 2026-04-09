package handles

import (
	"context"
	"io"
	"net/url"
	stdpath "path"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/errs"
	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/upload"
	"github.com/OpenListTeam/OpenList/v4/pkg/utils"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
)

type FsChunkInitReq struct {
	Path         string `json:"path" form:"path" binding:"required"`
	Size         int64  `json:"size" form:"size" binding:"required"`
	ChunkSize    int64  `json:"chunk_size" form:"chunk_size" binding:"required"`
	TotalChunks  int    `json:"total_chunks" form:"total_chunks" binding:"required"`
	LastModified int64  `json:"last_modified" form:"last_modified"`
	Mimetype     string `json:"mimetype" form:"mimetype"`
	SHA256       string `json:"sha256" form:"sha256"`
}

type FsChunkSessionReq struct {
	UploadID string `json:"upload_id" form:"upload_id" binding:"required"`
	AsTask   bool   `json:"as_task" form:"as_task"`
}

func FsChunkInit(c *gin.Context) {
	var req FsChunkInitReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	path, err := decodeAndJoinUserPath(user, req.Path)
	if err != nil {
		common.ErrorResp(c, err, 403)
		return
	}
	if err := ensureUploadWritable(c.Request.Context(), user, path); err != nil {
		common.ErrorResp(c, err, 403)
		return
	}
	overwrite := c.GetHeader("Overwrite") != "false"
	if !overwrite {
		if res, _ := fs.Get(c.Request.Context(), path, &fs.GetArgs{NoLog: true}); res != nil {
			common.ErrorStrResp(c, "file exists", 403)
			return
		}
	}
	name := stdpath.Base(path)
	if shouldIgnoreSystemFile(name) {
		common.ErrorStrResp(c, errs.IgnoredSystemFile.Error(), 403)
		return
	}
	if req.Mimetype == "" {
		req.Mimetype = utils.GetMimeType(name)
	}
	session, err := upload.ChunkUpload.InitSession(upload.InitArgs{
		User:         user,
		Path:         path,
		Size:         req.Size,
		ChunkSize:    req.ChunkSize,
		TotalChunks:  req.TotalChunks,
		Mimetype:     req.Mimetype,
		LastModified: lastModifiedFromMillis(req.LastModified),
		Hashes: map[string]string{
			"sha256": req.SHA256,
		},
	})
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, chunkSessionResp(session))
}

func FsChunkStatus(c *gin.Context) {
	var req FsChunkSessionReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	session, err := upload.ChunkUpload.GetSession(user, req.UploadID)
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}
	if err := ensureUploadWritable(c.Request.Context(), user, session.Path); err != nil {
		common.ErrorResp(c, err, 403)
		return
	}
	common.SuccessResp(c, chunkSessionResp(session))
}

func FsChunkUpload(c *gin.Context) {
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	uploadID := strings.TrimSpace(c.PostForm("upload_id"))
	if uploadID == "" {
		uploadID = strings.TrimSpace(c.Query("upload_id"))
	}
	index, err := upload.ParseChunkIndex(firstNonEmpty(c.PostForm("chunk_index"), c.Query("chunk_index")))
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	session, err := upload.ChunkUpload.GetSession(user, uploadID)
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}
	if err := ensureUploadWritable(c.Request.Context(), user, session.Path); err != nil {
		common.ErrorResp(c, err, 403)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), upload.DefaultChunkUploadTimeout)
	defer cancel()
	chunkSize := c.Request.ContentLength
	if chunkSize < 0 {
		chunkSize = session.ChunkSize
	}
	hashes := map[string]string{
		"sha256": strings.TrimSpace(c.GetHeader("X-Chunk-Sha256")),
	}
	hashes = dropEmptyHashes(hashes)
	if len(hashes) == 0 {
		hashes = upload.ParseHashes(c.Request.URL.Query(), "chunk_")
	}
	session, record, err := upload.ChunkUpload.UploadChunk(upload.UploadChunkArgs{
		User:      user,
		SessionID: uploadID,
		Index:     index,
		Size:      chunkSize,
		Hashes:    hashes,
		Reader:    &ctxReader{ctx: ctx, reader: c.Request.Body},
	})
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	common.SuccessResp(c, gin.H{
		"upload_id":        session.ID,
		"chunk_index":      record.Index,
		"chunk_size":       record.Size,
		"uploaded_chunks":  upload.UploadedIndexes(session),
		"remaining_chunks": upload.RemainingIndexes(session),
		"expires_at":       session.ExpiresAt,
		"chunk_hashes":     record.Hashes,
		"completed":        len(session.Chunks) == session.TotalChunks,
		"completed_chunks": len(session.Chunks),
		"total_chunks":     session.TotalChunks,
		"target_file_path": session.Path,
		"target_file_name": session.Name,
	})
}

func FsChunkComplete(c *gin.Context) {
	var req FsChunkSessionReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	session, err := upload.ChunkUpload.GetSession(user, req.UploadID)
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}
	if err := ensureUploadWritable(c.Request.Context(), user, session.Path); err != nil {
		common.ErrorResp(c, err, 403)
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), upload.DefaultCompleteUploadLimit)
	defer cancel()
	session, reader, err := upload.ChunkUpload.CompleteReader(user, req.UploadID)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	defer reader.Close()
	file := upload.BuildStream(session, &ctxReader{ctx: ctx, reader: reader})
	dir := stdpath.Dir(session.Path)
	if req.AsTask {
		taskInfo, err := fs.PutAsTask(ctx, dir, file)
		if err != nil {
			_ = upload.ChunkUpload.RemoveSessionByID(user.ID, session.ID)
			common.ErrorResp(c, err, 500)
			return
		}
		if err := upload.ChunkUpload.RemoveSessionByID(user.ID, session.ID); err != nil {
			common.ErrorResp(c, err, 500)
			return
		}
		common.SuccessResp(c, gin.H{
			"upload_id": session.ID,
			"path":      session.Path,
			"size":      session.Size,
			"hashes":    session.Hashes,
			"task":      getTaskInfo(taskInfo),
		})
		return
	}
	if err := fs.PutDirectly(ctx, dir, file); err != nil {
		_ = upload.ChunkUpload.RemoveSessionByID(user.ID, session.ID)
		common.ErrorResp(c, err, 500)
		return
	}
	if err := upload.ChunkUpload.RemoveSessionByID(user.ID, session.ID); err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c, gin.H{
		"upload_id": session.ID,
		"path":      session.Path,
		"size":      session.Size,
		"hashes":    session.Hashes,
	})
}

func FsChunkCancel(c *gin.Context) {
	var req FsChunkSessionReq
	if err := c.ShouldBind(&req); err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	user := c.Request.Context().Value(conf.UserKey).(*model.User)
	session, err := upload.ChunkUpload.GetSession(user, req.UploadID)
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}
	if err := ensureUploadWritable(c.Request.Context(), user, session.Path); err != nil {
		common.ErrorResp(c, err, 403)
		return
	}
	if err := upload.ChunkUpload.RemoveSession(user, req.UploadID); err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	common.SuccessResp(c)
}

type ctxReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *ctxReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
}

func (r *ctxReader) Close() error {
	if closer, ok := r.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func ensureUploadWritable(_ context.Context, user *model.User, path string) error {
	parentPath := stdpath.Dir(path)
	parentMeta, err := op.GetNearestMeta(parentPath)
	if err != nil && !errors.Is(errors.Cause(err), errs.MetaNotFound) {
		return err
	}
	if !user.CanWriteContent() && !common.CanWriteContentBypassUserPerms(parentMeta, parentPath) {
		return errs.PermissionDenied
	}
	if !common.CanWrite(user, parentMeta, parentPath) {
		return errs.PermissionDenied
	}
	return nil
}

func decodeAndJoinUserPath(user *model.User, rawPath string) (string, error) {
	path, err := url.PathUnescape(rawPath)
	if err != nil {
		return "", err
	}
	return user.JoinPath(path)
}

func lastModifiedFromMillis(value int64) time.Time {
	if value <= 0 {
		return time.Now()
	}
	return time.UnixMilli(value)
}

func chunkSessionResp(session *upload.Session) gin.H {
	return gin.H{
		"upload_id":        session.ID,
		"path":             session.Path,
		"name":             session.Name,
		"size":             session.Size,
		"chunk_size":       session.ChunkSize,
		"total_chunks":     session.TotalChunks,
		"uploaded_chunks":  upload.UploadedIndexes(session),
		"remaining_chunks": upload.RemainingIndexes(session),
		"hashes":           session.Hashes,
		"expires_at":       session.ExpiresAt,
		"completed":        len(session.Chunks) == session.TotalChunks,
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func dropEmptyHashes(hashes map[string]string) map[string]string {
	cleaned := make(map[string]string, len(hashes))
	for name, value := range hashes {
		if strings.TrimSpace(value) != "" {
			cleaned[name] = strings.TrimSpace(value)
		}
	}
	return cleaned
}
