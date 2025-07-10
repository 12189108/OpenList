package handles

import (
	"net/url"
	"path"
	"strconv"

	"github.com/OpenListTeam/OpenList/v4/internal/fs"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	"github.com/OpenListTeam/OpenList/v4/internal/task"
	"github.com/OpenListTeam/OpenList/v4/server/common"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

// ChunkUploadInit 初始化分片上传
func ChunkUploadInit(c *gin.Context) {
	// 获取请求参数
	filePath := c.GetHeader("File-Path")
	filePath, err := url.PathUnescape(filePath)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	// 获取文件大小
	totalSizeStr := c.GetHeader("File-Size")
	if totalSizeStr == "" {
		common.ErrorStrResp(c, "File-Size required", 400)
		return
	}
	totalSize, err := strconv.ParseInt(totalSizeStr, 10, 64)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	// 获取分片大小(可选)
	chunkSize := stream.DefaultChunkSize
	if chunkSizeStr := c.GetHeader("Chunk-Size"); chunkSizeStr != "" {
		if parsedChunkSize, err := strconv.ParseInt(chunkSizeStr, 10, 64); err == nil && parsedChunkSize > 0 {
			chunkSize = parsedChunkSize
		}
	}

	// 获取文件哈希值(可选)
	fileHash := c.GetHeader("File-Hash")

	// 获取当前用户
	user := c.MustGet("user").(*model.User)
	filePath, err = user.JoinPath(filePath)
	if err != nil {
		common.ErrorResp(c, err, 403)
		return
	}

	// 检查是否允许上传
	storage, err := fs.GetStorage(filePath, &fs.GetStoragesArgs{})
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}
	if storage.Config().NoUpload {
		common.ErrorStrResp(c, "Current storage doesn't support upload", 405)
		return
	}

	// 初始化分片上传
	_, fileName := path.Split(filePath)
	uploader := stream.ChunkedUploaderManager.CreateUploader(fileName, filePath, totalSize, chunkSize, fileHash)

	// 返回上传ID和分片信息
	common.SuccessResp(c, uploader.GetInfo())
}

// ChunkUploadPart 上传一个分片
func ChunkUploadPart(c *gin.Context) {
	// 获取上传ID
	uploadID := c.GetHeader("Upload-ID")
	if uploadID == "" {
		common.ErrorStrResp(c, "Upload-ID required", 400)
		return
	}

	// 获取分片索引
	chunkIndexStr := c.GetHeader("Chunk-Index")
	if chunkIndexStr == "" {
		common.ErrorStrResp(c, "Chunk-Index required", 400)
		return
	}
	chunkIndex, err := strconv.Atoi(chunkIndexStr)
	if err != nil {
		common.ErrorResp(c, err, 400)
		return
	}

	// 获取上传器
	uploader, err := stream.ChunkedUploaderManager.GetUploader(uploadID)
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}

	// 上传分片
	err = uploader.UploadChunk(chunkIndex, c.Request.Body)
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}
	defer c.Request.Body.Close()

	common.SuccessResp(c)
}

// ChunkUploadComplete 完成分片上传
func ChunkUploadComplete(c *gin.Context) {
	// 获取上传ID
	uploadID := c.GetHeader("Upload-ID")
	if uploadID == "" {
		common.ErrorStrResp(c, "Upload-ID required", 400)
		return
	}

	// 获取上传器
	uploader, err := stream.ChunkedUploaderManager.GetUploader(uploadID)
	if err != nil {
		common.ErrorResp(c, err, 404)
		return
	}

	// 完成上传，合并分片
	fileStream, err := uploader.CompleteUpload()
	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}

	// 删除上传器
	stream.ChunkedUploaderManager.RemoveUploader(uploadID)

	// 获取上传路径
	info := uploader.GetInfo()
	dir, _ := path.Split(info.FilePath)

	// 将文件保存到存储
	asTask := c.GetHeader("As-Task") == "true"
	logrus.Infof("ChunkUploadComplete: filepath=%s, size=%d", info.FilePath, fileStream.GetSize())

	var t task.TaskExtensionInfo
	if asTask {
		t, err = fs.PutAsTask(c, dir, fileStream)
	} else {
		err = fs.PutDirectly(c, dir, fileStream, true)
	}

	if err != nil {
		common.ErrorResp(c, err, 500)
		return
	}

	if t == nil {
		common.SuccessResp(c)
		return
	}

	common.SuccessResp(c, gin.H{
		"task": getTaskInfo(t),
	})
}

// ChunkUploadAbort 中止分片上传
func ChunkUploadAbort(c *gin.Context) {
	// 获取上传ID
	uploadID := c.GetHeader("Upload-ID")
	if uploadID == "" {
		common.ErrorStrResp(c, "Upload-ID required", 400)
		return
	}

	// 删除上传器
	stream.ChunkedUploaderManager.RemoveUploader(uploadID)
	common.SuccessResp(c)
}
